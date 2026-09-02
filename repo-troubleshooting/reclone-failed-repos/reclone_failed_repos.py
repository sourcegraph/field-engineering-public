#!/usr/bin/env python3

"""Report, fetch, or reclone every repository on a Sourcegraph instance whose
last clone or fetch failed.

Lists repositories matching `repositories(failedFetch: true)` and writes their
mirror diagnostics (shard, size, schedule, queue position, last error, ...) to
a CSV. With `--fetch`, also calls `updateMirrorRepository` on each one, which
queues a fetch of the existing clone. With `--reclone`, calls
`recloneRepository` instead, which deletes the repository from gitserver disk,
marks it as not cloned, and starts a fresh clone.

Mutations are packed into one GraphQL request per batch, using field aliases,
and batches are sent in parallel over keep-alive connections.

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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 60
MAX_ERROR_CHARS = 160
RECLONE_IN_PROGRESS_MESSAGE = "another reclone is in progress"

# action name -> (GraphQL mutation field, name of its repository ID argument)
ACTIONS = {
    "fetch": ("updateMirrorRepository", "repository"),
    "reclone": ("recloneRepository", "repo"),
}

CSV_COLUMNS: tuple[str, ...] = (
    "repo_name",
    "sourcegraph_url",
    "remote_url",
    "gitserver_shard",
    "size_mb",
    "cloned",
    "clone_in_progress",
    "is_corrupted",
    "last_successful_fetch",
    "time_since_last_successful_fetch",
    "next_sync",
    "time_until_next_sync",
    "update_schedule_due",
    "time_until_update_schedule_due",
    "update_schedule_interval_seconds",
    "update_queue_position",
    "currently_updating",
    "last_error",
    "last_sync_output",
    "action",
    "result",
)

FAILED_REPOSITORIES_QUERY = """
query FailedRepositories($first: Int!, $after: String) {
  repositories(failedFetch: true, first: $first, after: $after) {
    nodes {
      id
      name
      url
      mirrorInfo {
        remoteURL
        shard
        byteSize
        cloned
        cloneInProgress
        isCorrupted
        updatedAt
        nextSyncAt
        updateSchedule { intervalSeconds due }
        updateQueue { index updating }
        lastError
        lastSyncOutput
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
# (repository, error message) — error is None when the mutation was triggered
Outcome = tuple[Repository, str | None]
CsvRow = dict[str, Any]


class GraphQLError(Exception):
    """The Sourcegraph API returned an HTTP or GraphQL error."""


def batched_mutation(action: str, count: int) -> str:
    """Build a mutation that applies `action` to `count` repositories via aliased fields."""
    field, argument = ACTIONS[action]
    declarations = ", ".join(f"$r{index}: ID!" for index in range(count))
    fields = "\n".join(
        f"  r{index}: {field}({argument}: $r{index}) {{ alwaysNil }}"
        for index in range(count)
    )
    return f"mutation {action.capitalize()}({declarations}) {{\n{fields}\n}}"


class SourcegraphClient:
    """GraphQL client with one keep-alive connection per calling thread."""

    def __init__(self, endpoint: str, token: str) -> None:
        parsed = urlparse(endpoint)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError(f"endpoint must be an http(s) URL: {endpoint!r}")
        self.endpoint = endpoint.rstrip("/")
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

    def mutate_batch(
        self, action: str, repositories: list[Repository]
    ) -> list[Outcome]:
        """Apply `action` to a batch in one request; return an outcome per repository."""
        variables = {
            f"r{index}": repository["id"]
            for index, repository in enumerate(repositories)
        }
        try:
            payload = self.graphql(
                batched_mutation(action, len(repositories)), variables
            )
        except (GraphQLError, http.client.HTTPException, OSError) as error:
            return [(repository, str(error)) for repository in repositories]

        errors_by_alias: dict[str | None, list[str]] = {}
        for error in payload.get("errors") or []:
            path = error.get("path") or [None]
            message = error.get("message") or json.dumps(error)
            errors_by_alias.setdefault(path[0], []).append(message)
        if None in errors_by_alias:
            # Request-level error (e.g. permission denied): nothing was changed
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
        description="Report all repositories with failed clones or fetches on a "
        "Sourcegraph instance, and optionally fetch or reclone them. "
        "Default is a read-only report; use --fetch or --reclone to act.",
    )
    basic = parser.add_argument_group("basic options")
    action = basic.add_mutually_exclusive_group()
    action.add_argument(
        "--fetch",
        action="store_const",
        const="fetch",
        dest="action",
        help="Queue a fetch of each failed repository's existing clone",
    )
    action.add_argument(
        "--reclone",
        action="store_const",
        const="reclone",
        dest="action",
        help="Delete each failed repository from gitserver disk and reclone it",
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
        help="Only report (and with --fetch / --reclone, act on) the first COUNT "
        "failed repos (default: all failed repos)",
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
        "--batch-size",
        type=int,
        default=10,
        metavar="COUNT",
        help="With --fetch / --reclone, batch COUNT repos into each mutation "
        "request (default: 10)",
    )
    advanced.add_argument(
        "--parallelism",
        type=int,
        default=8,
        metavar="COUNT",
        help="With --fetch / --reclone, send up to COUNT mutation requests in "
        "parallel (default: 8)",
    )
    args = parser.parse_args()
    if not args.src_endpoint or not args.src_access_token:
        parser.error(
            "set SRC_ENDPOINT and SRC_ACCESS_TOKEN in the environment or a .env file, "
            "or pass --src-endpoint and --src-access-token"
        )
    if min(args.batch_size, args.parallelism, args.list_repos_page_size) < 1:
        parser.error(
            "--list-repos-page-size, --batch-size, and --parallelism must be >= 1"
        )
    return args


def one_line(text: str | None) -> str:
    """Collapse whitespace so multi-line API text fits one CSV cell / log line."""
    return " ".join((text or "").split())


def relative_time(iso_timestamp: str | None, now: datetime) -> str:
    """Render an RFC 3339 timestamp as e.g. '3h 12m ago' or 'in 45s'."""
    if not iso_timestamp:
        return ""
    timestamp = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
    seconds = int((timestamp - now).total_seconds())
    days, remainder = divmod(abs(seconds), 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, remainder = divmod(remainder, 60)
    if days:
        duration = f"{days}d {hours}h"
    elif hours:
        duration = f"{hours}h {minutes}m"
    elif minutes:
        duration = f"{minutes}m"
    else:
        duration = f"{remainder}s"
    return f"in {duration}" if seconds > 0 else f"{duration} ago"


def describe(repository: Repository) -> str:
    mirror_info = repository.get("mirrorInfo") or {}
    status = "cloning" if mirror_info.get("cloneInProgress") else "failed"
    error = one_line(mirror_info.get("lastError"))
    if len(error) > MAX_ERROR_CHARS:
        error = error[:MAX_ERROR_CHARS] + "..."
    error_summary = f", error: {error}" if error else ""
    return (
        f"{repository['name']} (status: {status}, "
        f"last updated: {mirror_info.get('updatedAt') or 'never'}{error_summary})"
    )


def csv_row(
    repository: Repository, action: str, result: str, endpoint: str, now: datetime
) -> CsvRow:
    """Flatten one repository's mirror diagnostics plus the action taken on it."""
    mirror_info = repository.get("mirrorInfo") or {}
    schedule = mirror_info.get("updateSchedule") or {}
    queue = mirror_info.get("updateQueue") or {}
    return {
        "repo_name": repository["name"],
        "sourcegraph_url": endpoint + repository["url"],
        "remote_url": mirror_info.get("remoteURL") or "",
        "gitserver_shard": mirror_info.get("shard") or "",
        "size_mb": f"{int(mirror_info.get('byteSize') or 0) / (1024 * 1024):.1f}",
        "cloned": mirror_info.get("cloned"),
        "clone_in_progress": mirror_info.get("cloneInProgress"),
        "is_corrupted": mirror_info.get("isCorrupted"),
        "last_successful_fetch": mirror_info.get("updatedAt") or "",
        "time_since_last_successful_fetch": relative_time(
            mirror_info.get("updatedAt"), now
        ),
        "next_sync": mirror_info.get("nextSyncAt") or "",
        "time_until_next_sync": relative_time(mirror_info.get("nextSyncAt"), now),
        "update_schedule_due": schedule.get("due") or "",
        "time_until_update_schedule_due": relative_time(schedule.get("due"), now),
        "update_schedule_interval_seconds": schedule.get("intervalSeconds", ""),
        "update_queue_position": queue.get("index", ""),
        "currently_updating": queue.get("updating", ""),
        "last_error": one_line(mirror_info.get("lastError")),
        "last_sync_output": one_line(mirror_info.get("lastSyncOutput")),
        "action": action,
        "result": result,
    }


def mutate_all(
    client: SourcegraphClient,
    action: str,
    repositories: list[Repository],
    batch_size: int,
    parallelism: int,
) -> list[Outcome]:
    """Apply `action` in parallel batches; return one outcome per repository."""
    batches = [
        repositories[start : start + batch_size]
        for start in range(0, len(repositories), batch_size)
    ]
    outcomes: list[Outcome] = []
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        for batch_outcomes in executor.map(
            lambda batch: client.mutate_batch(action, batch), batches
        ):
            outcomes.extend(batch_outcomes)
    return outcomes


def outcome_action(action: str, error: str | None) -> str:
    """Map a mutation outcome to the CSV `action` value."""
    if error is None:
        return f"{action} triggered"
    if RECLONE_IN_PROGRESS_MESSAGE in error:
        return f"{action} skipped"
    return f"{action} failed"


def write_csv(rows: list[CsvRow]) -> Path:
    """Write rows to a timestamped CSV in the current directory; return its path."""
    timestamp = datetime.now().astimezone().strftime("%Y-%m-%d-%H-%M-%S")
    path = Path(f"{timestamp}-failed-repos.csv")
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
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

    now = datetime.now(timezone.utc)
    if args.action:
        outcomes = mutate_all(
            client, args.action, repositories, args.batch_size, args.parallelism
        )
        rows = []
        for repository, error in outcomes:
            action = outcome_action(args.action, error)
            rows.append(csv_row(repository, action, error or "", client.endpoint, now))
            if action.endswith("failed"):
                logger.error("%s: %s: %s", action, repository["name"], error)
            else:
                logger.info("%s: %s", action, repository["name"])
    else:
        rows = [
            csv_row(repository, "listed (dry run)", "", client.endpoint, now)
            for repository in repositories
        ]
        logger.info(
            "Dry run; rerun with --fetch or --reclone to act on these repositories"
        )

    csv_path = write_csv(rows)
    logger.info("Wrote %s", csv_path)

    failed = sum(row["action"].endswith("failed") for row in rows)
    if args.action:
        logger.info(
            "%s: triggered %d, skipped %d already in progress, %d failed",
            args.action,
            sum(row["action"].endswith("triggered") for row in rows),
            sum(row["action"].endswith("skipped") for row in rows),
            failed,
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
