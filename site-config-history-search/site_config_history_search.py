#!/usr/bin/env python3

"""Find which site configuration changes on a Sourcegraph instance touched a given key.

Pages through `site.configuration.history` and prints every change whose diff
adds or removes a line containing the search text, with its date, author, and
full diff. Matching is a plain substring match on `+`/`-` diff lines, so
`auth` also matches `oauth` and comments.

Requires a site-admin access token. Uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 60

SITE_CONFIG_HISTORY_QUERY = """
query SiteConfigHistory($first: Int!, $after: String) {
  site {
    configuration {
      history(first: $first, after: $after) {
        totalCount
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          id
          author {
            username
            displayName
            databaseID
            emails {
              email
              verified
              isPrimary
            }
          }
          createdAt
          diff
        }
      }
    }
  }
}
"""


class Email(TypedDict):
    email: str
    verified: bool
    isPrimary: bool


class Author(TypedDict):
    username: str
    displayName: str | None
    databaseID: int
    emails: list[Email]


class HistoryNode(TypedDict):
    id: str
    author: Author | None
    createdAt: str
    diff: str | None


class PageInfo(TypedDict):
    hasNextPage: bool
    endCursor: str | None


class History(TypedDict):
    totalCount: int
    pageInfo: PageInfo
    nodes: list[HistoryNode]


class GraphQLError(Exception):
    """The Sourcegraph API returned an HTTP or GraphQL error."""


class SourcegraphClient:
    def __init__(self, endpoint: str, token: str) -> None:
        parsed = urlparse(endpoint)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError(f"endpoint must be an http(s) URL: {endpoint!r}")
        self.url = endpoint.rstrip("/") + "/.api/graphql"
        self.headers = {
            "Authorization": f"token {token}",
            "Content-Type": "application/json",
            "User-Agent": "site-config-history-search/0.0.1",
        }

    def graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        """Send one GraphQL request and return its `data`; raise on any error."""
        body = json.dumps({"query": query, "variables": variables}).encode()
        request = urllib.request.Request(self.url, data=body, headers=self.headers)
        try:
            with urllib.request.urlopen(
                request, timeout=REQUEST_TIMEOUT_SECONDS
            ) as response:
                payload = json.loads(response.read())
        except urllib.error.HTTPError as error:
            raise GraphQLError(
                f"HTTP {error.code} {error.reason}: "
                f"{error.read().decode(errors='replace')}"
            ) from error
        if payload.get("errors"):
            raise GraphQLError(json.dumps(payload["errors"], indent=2))
        return payload["data"]

    def site_config_history(self, page_size: int) -> list[HistoryNode]:
        """Return every site configuration change, newest first, paginating."""
        nodes: list[HistoryNode] = []
        after: str | None = None
        while True:
            data = self.graphql(
                SITE_CONFIG_HISTORY_QUERY, {"first": page_size, "after": after}
            )
            history: History = data["site"]["configuration"]["history"]
            nodes.extend(history["nodes"])
            logger.info(
                "Fetched %d/%d site config changes", len(nodes), history["totalCount"]
            )
            if not history["pageInfo"]["hasNextPage"]:
                return nodes
            after = history["pageInfo"]["endCursor"]


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
        description="Print every site configuration change on a Sourcegraph "
        "instance whose diff adds or removes a line containing SEARCH_TEXT.",
    )
    parser.add_argument(
        "search_text",
        metavar="SEARCH_TEXT",
        help='Text to look for in changed lines, e.g. "auth.providers"',
    )
    parser.add_argument(
        "--src-endpoint",
        default=os.environ.get("SRC_ENDPOINT"),
        help="Sourcegraph URL, e.g. https://sourcegraph.example.com "
        "(default: SRC_ENDPOINT env var or .env file)",
    )
    parser.add_argument(
        "--src-access-token",
        default=os.environ.get("SRC_ACCESS_TOKEN"),
        help="Site-admin access token (default: SRC_ACCESS_TOKEN env var or .env file)",
    )
    parser.add_argument(
        "--short",
        action="store_true",
        help="One line per change: date, version, and the new value of the "
        "matching line, or <deleted>",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        metavar="COUNT",
        help="Fetch COUNT history entries per GraphQL request (default: 100)",
    )
    args = parser.parse_args()
    if not args.src_endpoint or not args.src_access_token:
        parser.error(
            "set SRC_ENDPOINT and SRC_ACCESS_TOKEN in the environment or a .env file, "
            "or pass --src-endpoint and --src-access-token"
        )
    if args.page_size < 1:
        parser.error("--page-size must be >= 1")
    return args


def changed_lines_containing(node: HistoryNode, search_text: str) -> list[str]:
    """Added (`+`) and removed (`-`) diff lines containing search_text, with the marker."""
    return [
        line
        for line in (node["diff"] or "").splitlines()
        if line.startswith(("+", "-")) and search_text in line
    ]


def short_lines(node: HistoryNode, changed_lines: list[str]) -> list[str]:
    """One `date  version  value` row per added line, or `<deleted>` if only removed."""
    added = [line[1:].strip() for line in changed_lines if line.startswith("+")]
    return [
        f"{node['createdAt']}  {config_version(node):>4}  {value}"
        for value in (added or ["<deleted>"])
    ]


def config_version(node: HistoryNode) -> str:
    """Decode the Relay ID (base64 of "SiteConfigurationChange:<row id>") to the row id."""
    return base64.b64decode(node["id"]).decode().rpartition(":")[2]


def author_lines(author: Author | None) -> list[str]:
    if author is None:
        return [
            "Author:  <none: internal process, SITE_CONFIG_FILE reload, or deleted user>"
        ]
    primary = next((email for email in author["emails"] if email["isPrimary"]), None)
    email = ""
    if primary:
        email = f"{primary['email']} ({'verified' if primary['verified'] else 'unverified'})"
    return [
        f"Author:  {author['username']}",
        f"  Name:  {author['displayName'] or ''}",
        f"  ID:    {author['databaseID']}",
        f"  Email: {email}",
    ]


def print_change(node: HistoryNode) -> None:
    print(f"Date:    {node['createdAt']}")
    print(f"Version: {config_version(node)}")
    print("\n".join(author_lines(node["author"])))
    # Drop the "--- ID: N" / "+++ ID: M" file headers; Version already says which row this is
    for line in (node["diff"] or "").splitlines():
        if not line.startswith(("--- ID: ", "+++ ID: ")):
            print(line)
    print()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()
    logger.info(
        "Searching %s site config history for %r", args.src_endpoint, args.search_text
    )
    try:
        client = SourcegraphClient(args.src_endpoint, args.src_access_token)
        history = client.site_config_history(args.page_size)
    except (ValueError, GraphQLError, OSError) as error:
        logger.error("Could not fetch site config history: %s", error)
        return 1

    matches = [
        (node, changed_lines)
        for node in history
        if (changed_lines := changed_lines_containing(node, args.search_text))
    ]
    if not matches:
        logger.info("No changes found containing %r", args.search_text)
        return 0

    logger.info("Found %d change(s) containing %r", len(matches), args.search_text)
    for node, changed_lines in matches:
        if args.short:
            print("\n".join(short_lines(node, changed_lines)))
        else:
            print_change(node)
    return 0


if __name__ == "__main__":
    sys.exit(main())
