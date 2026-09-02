#!/usr/bin/env python3

"""Reclone every repository on a Sourcegraph instance whose last clone or fetch failed.

Lists repositories matching `repositories(failedFetch: true)`, then, with
`--apply`, calls the `recloneRepository` mutation on each one. Recloning
deletes the repository from gitserver disk, marks it as not cloned, and
starts a fresh clone.

Reclone mutations are packed into one GraphQL request per batch, using field
aliases, and batches are sent in parallel over keep-alive connections.

Requires an access token with site-admin (REPO_MANAGEMENT#WRITE) permission.
Uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import csv
import http.client
import json
import logging
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 60
MAX_ERROR_CHARS = 160
RECLONE_IN_PROGRESS_MESSAGE = "another reclone is in progress"
CSV_COLUMNS = ("repo_name", "action", "result")

FAILED_REPOSITORIES_QUERY = """
query FailedRepositories($first: Int!, $after: String) {
  repositories(failedFetch: true, first: $first, after: $after) {
    nodes {
      id
      name
      mirrorInfo {
        cloneInProgress
        updatedAt
        lastError
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

Repository = dict[str, Any]
# (repository, error message) — error is None when the reclone was triggered
Outcome = tuple[Repository, str | None]
# (repo_name, action, result), matching CSV_COLUMNS
CsvRow = tuple[str, str, str]


class GraphQLError(Exception):
    """The Sourcegraph API returned an HTTP or GraphQL error."""


def reclone_mutation(count: int) -> str:
    """Build a mutation that reclones `count` repositories via aliased fields."""
    declarations = ", ".join(f"$r{index}: ID!" for index in range(count))
    fields = "\n".join(
        f"  r{index}: recloneRepository(repo: $r{index}) {{ alwaysNil }}"
        for index in range(count)
    )
    return f"mutation Reclone({declarations}) {{\n{fields}\n}}"


class SourcegraphClient:
    """GraphQL client with one keep-alive connection per calling thread."""

    def __init__(self, endpoint: str, token: str) -> None:
        parsed = urlparse(endpoint)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError(f"endpoint must be an http(s) URL: {endpoint!r}")
        self.hostname = parsed.hostname
        self.port = parsed.port
        self.secure = parsed.scheme == "https"
        self.path = parsed.path.rstrip("/") + "/.api/graphql"
        self.headers = {
            "Authorization": f"token {token}",
            "Content-Type": "application/json",
            "User-Agent": "reclone-failed-repos/0.0.1",
        }
        self.thread_local = threading.local()

    def connection(self) -> http.client.HTTPConnection:
        """Return this thread's connection, opening it on first use."""
        connection = getattr(self.thread_local, "connection", None)
        if connection is None:
            connection_class = (
                http.client.HTTPSConnection
                if self.secure
                else http.client.HTTPConnection
            )
            connection = connection_class(
                self.hostname, self.port, timeout=REQUEST_TIMEOUT_SECONDS
            )
            self.thread_local.connection = connection
        return connection

    def drop_connection(self) -> None:
        connection = getattr(self.thread_local, "connection", None)
        if connection is not None:
            connection.close()
            self.thread_local.connection = None

    def graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        """Send one GraphQL request and return the full response payload.

        Reopens the connection and retries once if the server dropped an idle
        keep-alive connection. Callers inspect `errors` themselves, because a
        batched mutation can succeed for some aliases and fail for others.
        """
        body = json.dumps({"query": query, "variables": variables}).encode()
        try:
            status, reason, response_body = self.post(body)
        except (http.client.HTTPException, OSError):
            self.drop_connection()
            status, reason, response_body = self.post(body)
        if status != 200:
            raise GraphQLError(
                f"HTTP {status} {reason}: {response_body.decode(errors='replace')}"
            )
        return json.loads(response_body)

    def post(self, body: bytes) -> tuple[int, str, bytes]:
        """POST on this thread's connection; return (status, reason, body)."""
        connection = self.connection()
        connection.request("POST", self.path, body=body, headers=self.headers)
        response = connection.getresponse()
        return response.status, response.reason, response.read()

    def failed_repositories(
        self, list_repos_page_size: int, max_repos: int | None
    ) -> list[Repository]:
        """Return repositories whose last clone or fetch failed, paginating."""
        repositories: list[Repository] = []
        after: str | None = None
        while True:
            payload = self.graphql(
                FAILED_REPOSITORIES_QUERY,
                {"first": list_repos_page_size, "after": after},
            )
            if payload.get("errors"):
                raise GraphQLError(json.dumps(payload["errors"]))
            connection = payload["data"]["repositories"]
            repositories.extend(connection["nodes"])
            logger.info("Fetched %d failed repositories so far", len(repositories))
            if max_repos is not None and len(repositories) >= max_repos:
                return repositories[:max_repos]
            if not connection["pageInfo"]["hasNextPage"]:
                return repositories
            after = connection["pageInfo"]["endCursor"]

    def reclone_batch(self, repositories: list[Repository]) -> list[Outcome]:
        """Reclone a batch in one request; return an outcome per repository."""
        variables = {
            f"r{index}": repository["id"]
            for index, repository in enumerate(repositories)
        }
        try:
            payload = self.graphql(reclone_mutation(len(repositories)), variables)
        except (GraphQLError, http.client.HTTPException, OSError) as error:
            return [(repository, str(error)) for repository in repositories]

        errors_by_alias: dict[str | None, list[str]] = {}
        for error in payload.get("errors") or []:
            path = error.get("path") or [None]
            message = error.get("message") or json.dumps(error)
            errors_by_alias.setdefault(path[0], []).append(message)
        if None in errors_by_alias:
            # Request-level error (e.g. permission denied): nothing was recloned
            message = "; ".join(errors_by_alias[None])
            return [(repository, message) for repository in repositories]
        return [
            (repository, "; ".join(errors_by_alias.get(f"r{index}", [])) or None)
            for index, repository in enumerate(repositories)
        ]


def load_dotenv() -> None:
    """Set SRC_ENDPOINT / SRC_ACCESS_TOKEN from ./.env when not already in the env."""
    env_file = Path(".env")
    if not env_file.is_file():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.strip().partition("=")
        if separator and key.strip() in ("SRC_ENDPOINT", "SRC_ACCESS_TOKEN"):
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def parse_args() -> argparse.Namespace:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Reclone all repositories with failed clones or fetches "
        "on a Sourcegraph instance. Default is dry run; use --apply to reclone.",
    )
    basic = parser.add_argument_group("basic options")
    basic.add_argument(
        "--apply",
        action="store_true",
        help="Trigger reclones. Without this flag, only list failed repositories",
    )
    basic.add_argument(
        "--src-endpoint",
        default=os.environ.get("SRC_ENDPOINT"),
        help="Sourcegraph URL, e.g. https://sourcegraph.example.com "
        "(default: SRC_ENDPOINT env var or .env file)",
    )
    basic.add_argument(
        "--src-access-token",
        default=os.environ.get("SRC_ACCESS_TOKEN"),
        help="Site-admin access token (default: SRC_ACCESS_TOKEN env var or .env file)",
    )
    advanced = parser.add_argument_group("advanced options")
    advanced.add_argument(
        "--max-repos",
        type=int,
        metavar="COUNT",
        help="Only list (and with --apply, reclone) the first COUNT failed "
        "repos (default: all failed repos)",
    )
    advanced.add_argument(
        "--list-repos-page-size",
        type=int,
        default=100,
        metavar="COUNT",
        help="When listing failed repositories, fetch COUNT per GraphQL "
        "request (default: 100)",
    )
    advanced.add_argument(
        "--reclone-batch-size",
        type=int,
        default=10,
        metavar="COUNT",
        help="With --apply, batch COUNT repos into each reclone request (default: 10)",
    )
    advanced.add_argument(
        "--reclone-parallelism",
        type=int,
        default=8,
        metavar="COUNT",
        help="With --apply, send up to COUNT reclone requests in parallel (default: 8)",
    )
    args = parser.parse_args()
    if not args.src_endpoint or not args.src_access_token:
        parser.error(
            "set SRC_ENDPOINT and SRC_ACCESS_TOKEN in the environment or a .env file, "
            "or pass --src-endpoint and --src-access-token"
        )
    if (
        min(
            args.reclone_batch_size, args.reclone_parallelism, args.list_repos_page_size
        )
        < 1
    ):
        parser.error(
            "--list-repos-page-size, --reclone-batch-size, and --reclone-parallelism must be >= 1"
        )
    return args


def describe(repository: Repository) -> str:
    mirror_info = repository.get("mirrorInfo") or {}
    status = "cloning" if mirror_info.get("cloneInProgress") else "failed"
    error = last_error(repository)
    if len(error) > MAX_ERROR_CHARS:
        error = error[:MAX_ERROR_CHARS] + "..."
    error_summary = f", error: {error}" if error else ""
    return (
        f"{repository['name']} (status: {status}, "
        f"last updated: {mirror_info.get('updatedAt') or 'never'}{error_summary})"
    )


def last_error(repository: Repository) -> str:
    """Return the repository's last fetch error on one line, or ''."""
    mirror_info = repository.get("mirrorInfo") or {}
    return " ".join((mirror_info.get("lastError") or "").split())


def reclone_all(
    client: SourcegraphClient,
    repositories: list[Repository],
    reclone_batch_size: int,
    reclone_parallelism: int,
) -> list[CsvRow]:
    """Reclone in parallel batches; return one CSV row per repository."""
    batches = [
        repositories[start : start + reclone_batch_size]
        for start in range(0, len(repositories), reclone_batch_size)
    ]
    rows: list[CsvRow] = []
    with ThreadPoolExecutor(max_workers=reclone_parallelism) as executor:
        for outcomes in executor.map(client.reclone_batch, batches):
            for repository, error in outcomes:
                name = repository["name"]
                if error is None:
                    rows.append((name, "reclone triggered", ""))
                    logger.info("Triggered reclone: %s", name)
                elif RECLONE_IN_PROGRESS_MESSAGE in error:
                    rows.append((name, "skipped", error))
                    logger.info("Reclone already in progress: %s", name)
                else:
                    rows.append((name, "reclone failed", error))
                    logger.error("Failed to reclone %s: %s", name, error)
    return rows


def write_csv(rows: list[CsvRow]) -> Path:
    """Write rows to a timestamped CSV in the current directory; return its path."""
    timestamp = datetime.now().astimezone().strftime("%Y-%m-%d-%H-%M-%S")
    path = Path(f"{timestamp}-reclone-failed-repos.csv")
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(CSV_COLUMNS)
        writer.writerows(rows)
    return path


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()
    try:
        client = SourcegraphClient(args.src_endpoint, args.src_access_token)
        repositories = client.failed_repositories(
            args.list_repos_page_size, args.max_repos
        )
    except (ValueError, GraphQLError, http.client.HTTPException, OSError) as error:
        logger.error("Could not list failed repositories: %s", error)
        return 1

    if not repositories:
        logger.info("No failed repositories found")
        return 0

    logger.info("%d failed repositories:", len(repositories))
    for index, repository in enumerate(repositories, 1):
        logger.info("  %3d. %s", index, describe(repository))

    if args.apply:
        rows = reclone_all(
            client, repositories, args.reclone_batch_size, args.reclone_parallelism
        )
    else:
        rows = [
            (repository["name"], "listed (dry run)", last_error(repository))
            for repository in repositories
        ]
        logger.info("Dry run; rerun with --apply to reclone these repositories")

    csv_path = write_csv(rows)
    logger.info("Wrote %s", csv_path)

    failed = sum(action == "reclone failed" for _, action, _ in rows)
    if args.apply:
        logger.info(
            "Triggered %d reclones, skipped %d already in progress, %d failed",
            sum(action == "reclone triggered" for _, action, _ in rows),
            sum(action == "skipped" for _, action, _ in rows),
            failed,
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
