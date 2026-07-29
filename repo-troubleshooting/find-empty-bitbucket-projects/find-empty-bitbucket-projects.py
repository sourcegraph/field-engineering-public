#!/usr/bin/env python3

"""Count Sourcegraph repos for each Bitbucket Server projectKeys / repositoryQuery item.

Reads every code host connection of kind BITBUCKETSERVER from a Sourcegraph
instance, and for each item in the connection's `projectKeys` and
`repositoryQuery` config lists, runs a Sourcegraph search to count the
repositories on the instance which match that item. Items with 0 matching
repos identify empty (or unsynced) Bitbucket projects.

Requires a site-admin access token (the externalServices GraphQL query is
site-admin only). Works with Sourcegraph 7.0.0 and newer, using only Python
standard library modules, on Python 3.9 and newer.
"""

from __future__ import annotations

import argparse
import csv
import http.client
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn
from urllib.parse import ParseResult, parse_qs, urlparse

logger = logging.getLogger(__name__)


# --- Tune-ables -----------------------------------------------------------------

DEFAULT_OUTPUT_FILE = "bitbucket-project-repo-counts.csv"
DEFAULT_MAX_RETRIES = 5
DEFAULT_REPOSITORY_PATH_PATTERN = "{host}/{projectKey}/{repositorySlug}"
EXTERNAL_SERVICES_PAGE_SIZE = 100
REQUEST_TIMEOUT_SECONDS = 60
SEARCH_TIMEOUT_PARAMETER = "timeout:120s"
RETRYABLE_HTTP_STATUSES = {408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}
RETRYABLE_GRAPHQL_ERROR_TERMS = (
    "bad gateway",
    "code = unavailable",
    "connection reset",
    "context deadline exceeded",
    "deadline exceeded",
    "dial tcp",
    "eof",
    "gateway timeout",
    "no such host",
    "temporarily unavailable",
    "timeout",
    "transport:",
)

# repositoryQuery items are Bitbucket Server REST query strings; these
# parameters can name a project, so we can map the item to a repo search
REPOSITORY_QUERY_PROJECT_PARAMETERS = (
    "projectkey",
    "projectKey",
    "projectname",
    "projectName",
)

CSV_COLUMNS = [
    "externalService.displayName",
    "externalService.url",
    "externalService.username",
    "configField",
    "item",
    "projectKey",
    "searchQuery",
    "repositoryCount",
    "limitHit",
    "alert",
    "note",
]


# --- GraphQL queries ----------------------------------------------------------

CURRENT_USER_QUERY = """
query { currentUser { username siteAdmin } }
"""

EXTERNAL_SERVICES_QUERY = """
query ExternalServices($first: Int!, $after: String) {
  externalServices(first: $first, after: $after) {
    nodes {
      id
      kind
      displayName
      config
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

REPOSITORY_COUNT_SEARCH_QUERY = """
query CountRepos($query: String!) {
  search(query: $query, version: V3) {
    results {
      repositoriesCount
      limitHit
      alert {
        title
      }
    }
  }
}
"""


# --- HTTP / GraphQL client ------------------------------------------------------


class GraphQLError(RuntimeError):
    """Raised when the Sourcegraph GraphQL API returns errors"""


class HTTPRequestError(RuntimeError):
    """Raised when the server returns a definitive 4xx/5xx HTTP response"""

    def __init__(
        self,
        status: int,
        reason: str,
        url: str,
        headers: list[tuple[str, str]],
        body: bytes,
    ) -> None:
        """Capture the response status, headers, and body for later logging"""
        super().__init__(f"HTTP {status} {reason}")
        self.status = status
        self.reason = reason
        self.url = url
        self.headers = headers
        self.body = body


def open_connection(
    parsed: ParseResult,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
) -> http.client.HTTPConnection:
    """Open an HTTP(S) connection and reject other URL schemes"""
    if not parsed.hostname:
        msg = f"URL is missing a hostname: {parsed.geturl()!r}"
        raise ValueError(msg)
    if parsed.scheme == "https":
        return http.client.HTTPSConnection(
            parsed.hostname,
            parsed.port,
            timeout=timeout,
        )
    if parsed.scheme == "http":
        return http.client.HTTPConnection(
            parsed.hostname,
            parsed.port,
            timeout=timeout,
        )
    msg = f"Unsupported URL scheme: {parsed.scheme!r} (expected http or https)"
    raise ValueError(msg)


def send_once(
    url: str,
    body: bytes,
    headers: dict[str, str],
    timeout: int = REQUEST_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Send one POST. Returns parsed JSON on 2xx, raises HTTPRequestError on 4xx/5xx"""
    parsed = urlparse(url)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    connection = open_connection(parsed, timeout=timeout)
    try:
        connection.request("POST", path, body=body, headers=headers)
        response = connection.getresponse()
        response_body = response.read()
        if response.status >= http.client.BAD_REQUEST:
            raise HTTPRequestError(
                response.status,
                response.reason,
                url,
                response.getheaders(),
                response_body,
            )
        return json.loads(response_body)
    finally:
        connection.close()


def retry_delay_seconds(retry_number: int) -> int:
    """Return exponential retry delay: 1, 2, 4, 8, 16... seconds"""
    return 2 ** (retry_number - 1)


def sleep_before_retry(reason: str, retry_number: int, max_retries: int) -> None:
    """Log and sleep before the next retry attempt for this request"""
    delay = retry_delay_seconds(retry_number)
    logger.warning(
        "%s; retrying (%d/%d) in %ds...",
        reason,
        retry_number,
        max_retries,
        delay,
    )
    time.sleep(delay)


def retryable_http_error(error: HTTPRequestError) -> bool:
    """Return True for transient HTTP statuses worth retrying"""
    return error.status in RETRYABLE_HTTP_STATUSES


def graphql_error_message(graphql_error: object) -> str:
    """Return a GraphQL error message string"""
    if isinstance(graphql_error, dict):
        message = graphql_error.get("message")
        if isinstance(message, str):
            return message
    return str(graphql_error)


def has_retryable_graphql_error(errors: object) -> bool:
    """Return True when any GraphQL error looks transient"""
    if not isinstance(errors, list):
        return False
    for graphql_error in errors:
        message = graphql_error_message(graphql_error).lower()
        if any(term in message for term in RETRYABLE_GRAPHQL_ERROR_TERMS):
            return True
    return False


def summarize_graphql_errors(errors: object) -> str:
    """Return compact GraphQL error messages for retry logs"""
    if not isinstance(errors, list):
        return str(errors)
    return "; ".join(graphql_error_message(error) for error in errors)


def graphql_request(
    endpoint: str,
    token: str,
    query: str,
    variables: dict[str, Any],
    timeout: int = REQUEST_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    request_description: str = "GraphQL request",
) -> dict[str, Any]:
    """Send a GraphQL query to the Sourcegraph API and return the data block"""
    url = endpoint.rstrip("/") + "/.api/graphql"
    body = json.dumps({"query": query, "variables": variables}).encode()
    headers = {
        "Authorization": f"token {token}",
        "Content-Type": "application/json",
        "User-Agent": "find-empty-bitbucket-projects/0.0.1",
    }
    retry_prefix = f"{request_description}: " if request_description else ""
    for retry_count in range(max_retries + 1):
        retry_number = retry_count + 1
        try:
            response = send_once(url, body, headers, timeout=timeout)
        except HTTPRequestError as error:
            if not retryable_http_error(error) or retry_count >= max_retries:
                raise
            sleep_before_retry(
                f"{retry_prefix}HTTP {error.status} {error.reason}",
                retry_number,
                max_retries,
            )
            continue
        except OSError as error:
            if retry_count >= max_retries:
                raise
            sleep_before_retry(
                f"{retry_prefix}Request failed: {error}",
                retry_number,
                max_retries,
            )
            continue

        errors = response.get("errors")
        if not errors:
            return response["data"]

        if has_retryable_graphql_error(errors) and retry_count < max_retries:
            sleep_before_retry(
                f"{retry_prefix}GraphQL returned retryable error(s): "
                + summarize_graphql_errors(errors),
                retry_number,
                max_retries,
            )
            continue

        # GraphQL can return both `errors` and partial `data`. If we have data,
        # log the errors and keep going; only abort if no data was returned
        if response.get("data"):
            logger.warning(
                "GraphQL returned partial error(s): %s",
                summarize_graphql_errors(errors),
            )
            return response["data"]
        msg = f"GraphQL errors: {json.dumps(errors, indent=2)}"
        raise GraphQLError(msg)
    msg = "graphql_request retry loop exhausted unexpectedly"
    raise RuntimeError(msg)


def fetch_current_user(
    endpoint: str,
    token: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> tuple[str, bool] | None:
    """Return the authenticated username and site-admin flag

    Returns None when the token did not authenticate a user (currentUser
    is null), which means SRC_ACCESS_TOKEN is invalid on this instance
    """
    data = graphql_request(
        endpoint,
        token,
        CURRENT_USER_QUERY,
        {},
        max_retries=max_retries,
        request_description="Current user query",
    )
    user = data.get("currentUser")
    if not isinstance(user, dict) or not user.get("username"):
        return None
    return str(user["username"]), bool(user.get("siteAdmin"))


# --- Code host connection config parsing ------------------------------------------

# Sourcegraph code host connection configs are JSONC: JSON plus comments and
# trailing commas, which json.loads rejects, so strip both before parsing


def strip_jsonc_comments(text: str) -> str:
    """Remove // and /* */ comments from JSONC, preserving string contents"""
    output: list[str] = []
    index = 0
    length = len(text)
    in_string = False
    while index < length:
        char = text[index]
        if in_string:
            output.append(char)
            if char == "\\" and index + 1 < length:
                output.append(text[index + 1])
                index += 2
                continue
            if char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
            output.append(char)
            index += 1
            continue
        if char == "/" and index + 1 < length and text[index + 1] == "/":
            while index < length and text[index] != "\n":
                index += 1
            continue
        if char == "/" and index + 1 < length and text[index + 1] == "*":
            index += 2
            while index + 1 < length and not (
                text[index] == "*" and text[index + 1] == "/"
            ):
                index += 1
            index += 2
            continue
        output.append(char)
        index += 1
    return "".join(output)


def strip_jsonc_trailing_commas(text: str) -> str:
    """Remove trailing commas before } or ], preserving string contents"""
    output: list[str] = []
    index = 0
    length = len(text)
    in_string = False
    while index < length:
        char = text[index]
        if in_string:
            output.append(char)
            if char == "\\" and index + 1 < length:
                output.append(text[index + 1])
                index += 2
                continue
            if char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
            output.append(char)
            index += 1
            continue
        if char == ",":
            lookahead = index + 1
            while lookahead < length and text[lookahead] in " \t\r\n":
                lookahead += 1
            if lookahead < length and text[lookahead] in "}]":
                index += 1
                continue
        output.append(char)
        index += 1
    return "".join(output)


def parse_jsonc(text: str) -> dict[str, Any]:
    """Parse a JSONC document into a dict"""
    parsed = json.loads(strip_jsonc_trailing_commas(strip_jsonc_comments(text)))
    if not isinstance(parsed, dict):
        msg = f"Expected a JSON object, got {type(parsed).__name__}"
        raise TypeError(msg)
    return parsed


@dataclass(frozen=True)
class BitbucketConnection:
    """One BITBUCKETSERVER code host connection's repo-selection config"""

    display_name: str
    url: str
    username: str
    host: str
    repository_path_pattern: str
    project_keys: list[str]
    repository_queries: list[str]


def string_list(value: object) -> list[str]:
    """Return the string items of a JSON list value"""
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def parse_bitbucket_connection(node: dict[str, Any]) -> BitbucketConnection | None:
    """Parse one externalServices node into a BitbucketConnection, or None

    Returns None for connections without projectKeys or repositoryQuery, and
    for configs which fail to parse (logged as a warning)
    """
    display_name = str(node.get("displayName") or node.get("id") or "")
    config_text = str(node.get("config") or "")
    try:
        config = parse_jsonc(config_text)
    except (json.JSONDecodeError, TypeError) as error:
        logger.warning(
            "Skipping %s: could not parse code host connection config: %s",
            display_name,
            error,
        )
        return None
    project_keys = string_list(config.get("projectKeys"))
    repository_queries = string_list(config.get("repositoryQuery"))
    if not project_keys and not repository_queries:
        return None
    url = str(config.get("url") or "")
    username = str(config.get("username") or "")
    host = urlparse(url).hostname or ""
    repository_path_pattern = str(
        config.get("repositoryPathPattern") or DEFAULT_REPOSITORY_PATH_PATTERN,
    )
    return BitbucketConnection(
        display_name=display_name,
        url=url,
        username=username,
        host=host,
        repository_path_pattern=repository_path_pattern,
        project_keys=project_keys,
        repository_queries=repository_queries,
    )


def fetch_bitbucket_connections(
    endpoint: str,
    token: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> list[BitbucketConnection]:
    """Return all BITBUCKETSERVER connections with projectKeys or repositoryQuery"""
    connections: list[BitbucketConnection] = []
    cursor: str | None = None
    total_seen = 0
    while True:
        data = graphql_request(
            endpoint,
            token,
            EXTERNAL_SERVICES_QUERY,
            {"first": EXTERNAL_SERVICES_PAGE_SIZE, "after": cursor},
            max_retries=max_retries,
            request_description="External services page",
        )
        listing: dict[str, Any] = data["externalServices"]
        nodes: list[dict[str, Any]] = listing.get("nodes") or []
        total_seen += len(nodes)
        for node in nodes:
            if node.get("kind") != "BITBUCKETSERVER":
                continue
            connection = parse_bitbucket_connection(node)
            if connection is not None:
                logger.info(
                    "Code host connection %s (%s, username %s): "
                    "%d projectKeys, %d repositoryQuery item(s)",
                    connection.display_name,
                    connection.url,
                    connection.username,
                    len(connection.project_keys),
                    len(connection.repository_queries),
                )
                connections.append(connection)
        page_info: dict[str, Any] = listing.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
    logger.info(
        "Found %d Bitbucket Server connection(s) with projectKeys or "
        "repositoryQuery, out of %d code host connection(s)",
        len(connections),
        total_seen,
    )
    return connections


# --- Repo counting ---------------------------------------------------------------


def repository_query_project_key(item: str) -> str | None:
    """Extract a project name/key from a repositoryQuery item, when present

    repositoryQuery items are Bitbucket Server REST API query strings, e.g.
    "?projectname=KEY&visibility=private". Items without a project parameter
    (including the "none" sentinel) return None
    """
    parameters = parse_qs(item.lstrip("?"), keep_blank_values=False)
    for parameter_name in REPOSITORY_QUERY_PROJECT_PARAMETERS:
        values = parameters.get(parameter_name)
        if values and values[0]:
            return values[0]
    return None


def project_search_query(connection: BitbucketConnection, project_key: str) -> str:
    """Build the search query counting this connection's repos in one project

    Bitbucket Server repo names on Sourcegraph follow repositoryPathPattern
    (default {host}/{projectKey}/{repositorySlug}); anchor on the repo name
    prefix up to the repository slug
    """
    prefix = connection.repository_path_pattern.split("{repositorySlug}")[0]
    prefix = prefix.replace("{host}", connection.host)
    prefix = prefix.replace("{projectKey}", project_key)
    return f"repo:^{re.escape(prefix)} select:repo count:all {SEARCH_TIMEOUT_PARAMETER}"


@dataclass(frozen=True)
class RepositoryCount:
    """One search's repo count plus response metadata"""

    repository_count: int | None
    limit_hit: bool
    alert_title: str | None
    error: str | None


def fetch_repository_count(
    endpoint: str,
    token: str,
    search_query: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> RepositoryCount:
    """Run one repo-counting search and return its count and metadata"""
    try:
        data = graphql_request(
            endpoint,
            token,
            REPOSITORY_COUNT_SEARCH_QUERY,
            {"query": search_query},
            max_retries=max_retries,
            request_description=f"Repo count search: {search_query}",
        )
    except (GraphQLError, HTTPRequestError, OSError) as error:
        return RepositoryCount(
            repository_count=None,
            limit_hit=False,
            alert_title=None,
            error=str(error),
        )
    search_block: dict[str, Any] = data.get("search") or {}
    results: dict[str, Any] = search_block.get("results") or {}
    raw_count = results.get("repositoriesCount")
    alert: dict[str, Any] = results.get("alert") or {}
    alert_title = alert.get("title")
    return RepositoryCount(
        repository_count=raw_count if isinstance(raw_count, int) else None,
        limit_hit=bool(results.get("limitHit")),
        alert_title=alert_title if isinstance(alert_title, str) else None,
        error=None,
    )


@dataclass(frozen=True)
class ConfigItem:
    """One projectKeys or repositoryQuery list item from one connection"""

    connection: BitbucketConnection
    config_field: str  # "projectKeys" or "repositoryQuery"
    item: str
    project_key: str | None
    skip_note: str | None


def collect_config_items(connections: list[BitbucketConnection]) -> list[ConfigItem]:
    """Flatten each connection's projectKeys and repositoryQuery lists"""
    items: list[ConfigItem] = []
    for connection in connections:
        for project_key in connection.project_keys:
            items.append(
                ConfigItem(
                    connection=connection,
                    config_field="projectKeys",
                    item=project_key,
                    project_key=project_key,
                    skip_note=None,
                ),
            )
        for repository_query in connection.repository_queries:
            if repository_query == "none":
                skip_note = '"none" disables repositoryQuery syncing; nothing to count'
                project_key = None
            else:
                project_key = repository_query_project_key(repository_query)
                skip_note = (
                    None
                    if project_key is not None
                    else "no project parameter in query string; "
                    "cannot map to a repo name search"
                )
            items.append(
                ConfigItem(
                    connection=connection,
                    config_field="repositoryQuery",
                    item=repository_query,
                    project_key=project_key,
                    skip_note=skip_note,
                ),
            )
    return items


def csv_row(
    config_item: ConfigItem,
    search_query: str = "",
    repository_count: Any = "",
    limit_hit: Any = "",
    alert_title: str = "",
    note: str = "",
) -> list[Any]:
    """Build one CSV row in CSV_COLUMNS order"""
    connection = config_item.connection
    return [
        connection.display_name,
        connection.url,
        connection.username,
        config_item.config_field,
        config_item.item,
        config_item.project_key or "",
        search_query,
        repository_count,
        limit_hit,
        alert_title,
        note,
    ]


def count_and_report(
    endpoint: str,
    token: str,
    items: list[ConfigItem],
    output_path: Path,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> None:
    """Search per config item, logging each count and writing the CSV"""
    empty_items: list[ConfigItem] = []
    with output_path.open("w", newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(CSV_COLUMNS)
        for index, config_item in enumerate(items, start=1):
            position = f"[{index}/{len(items)}]"
            connection = config_item.connection
            label = (
                f"{connection.display_name} ({connection.url}, "
                f"username {connection.username}) "
                f"{config_item.config_field} {config_item.item!r}"
            )
            if config_item.project_key is None:
                logger.info(
                    "%s %s: skipped (%s)", position, label, config_item.skip_note
                )
                writer.writerow(csv_row(config_item, note=config_item.skip_note or ""))
                continue
            search_query = project_search_query(
                config_item.connection,
                config_item.project_key,
            )
            count = fetch_repository_count(
                endpoint,
                token,
                search_query,
                max_retries=max_retries,
            )
            note = ""
            if count.error is not None:
                note = f"search failed: {count.error}"
                logger.warning("%s %s: %s", position, label, note)
            else:
                logger.info(
                    "%s %s: %s repo(s)%s%s",
                    position,
                    label,
                    count.repository_count,
                    " (limit hit)" if count.limit_hit else "",
                    f" alert={count.alert_title!r}" if count.alert_title else "",
                )
                if count.repository_count == 0:
                    empty_items.append(config_item)
            writer.writerow(
                csv_row(
                    config_item,
                    search_query=search_query,
                    repository_count=count.repository_count
                    if count.repository_count is not None
                    else "",
                    limit_hit=count.limit_hit,
                    alert_title=count.alert_title or "",
                    note=note,
                ),
            )
    logger.info("Wrote %d row(s) to %s", len(items), output_path.name)
    if empty_items:
        logger.info(
            "Found %d config item(s) with 0 matching repos:",
            len(empty_items),
        )
        for config_item in empty_items:
            logger.info(
                "  %s %s %r",
                config_item.connection.display_name,
                config_item.config_field,
                config_item.item,
            )
    else:
        logger.info("No config items with 0 matching repos")


# --- CLI / credentials -------------------------------------------------------------


def load_dotenv() -> None:
    """Load SRC_ENDPOINT and SRC_ACCESS_TOKEN from `.env` if env vars are unset"""
    env_file = Path(".env")
    if not env_file.is_file():
        return
    for lineno, raw in enumerate(
        env_file.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        line = raw.strip()
        # Blank lines and comments are normal `.env` content; skip silently
        if not line or line.startswith("#"):
            continue
        # Log only the line number; malformed lines can contain secrets
        if "=" not in line:
            logger.warning(
                ".env line %d is malformed (missing '='); skipping",
                lineno,
            )
            continue
        key, _, value = line.partition("=")
        if key.strip() in ("SRC_ENDPOINT", "SRC_ACCESS_TOKEN"):
            # setdefault: real env wins over .env
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def die(message: str) -> NoReturn:
    """Log a one-line error and exit with status 1. Never returns"""
    logger.error("Error: %s", message)
    sys.exit(1)


def validate_endpoint(endpoint: str) -> None:
    """Reject obviously-bad SRC_ENDPOINT values with a friendly message"""
    parsed = urlparse(endpoint)
    if parsed.scheme not in ("https", "http"):
        die(
            f"SRC_ENDPOINT must start with https:// or http:// (got {endpoint!r})",
        )
    if not parsed.hostname:
        die(f"SRC_ENDPOINT is missing a hostname (got {endpoint!r})")


def validate_token(token: str) -> None:
    """Reject obviously-bad SRC_ACCESS_TOKEN values with a friendly message"""
    if not token.startswith("sgp_"):
        # Don't log any of the token bytes — even a 5-char prefix can leak
        # info about the source/format. Length alone is enough to confirm
        # something was set without echoing secret material
        die(
            f"SRC_ACCESS_TOKEN must be a Sourcegraph access token starting "
            f"with 'sgp_' (got a {len(token)}-character value)",
        )


def require_credentials(args: argparse.Namespace) -> tuple[str, str]:
    """Return (endpoint, token), with CLI args overriding env and `.env`"""
    endpoint = args.src_endpoint or os.environ.get("SRC_ENDPOINT", "")
    token = args.src_access_token or os.environ.get("SRC_ACCESS_TOKEN", "")
    if not endpoint or not token:
        die(
            "set SRC_ENDPOINT and SRC_ACCESS_TOKEN (via --src-endpoint / "
            "--src-access-token, environment variables, or a .env file)",
        )
    validate_endpoint(endpoint)
    validate_token(token)
    return endpoint, token


def sanitize_endpoint_for_filename(endpoint: str) -> str:
    """Return the endpoint hostname as a safe output filename prefix"""
    host = urlparse(endpoint).hostname or "sourcegraph"
    return re.sub(r"[^A-Za-z0-9.-]+", "-", host)


def non_negative_int(value: str) -> int:
    """argparse type for integers >= 0"""
    try:
        parsed = int(value)
    except ValueError:
        msg = f"must be an integer, got {value!r}"
        raise argparse.ArgumentTypeError(msg) from None
    if parsed < 0:
        msg = f"must be a non-negative integer (>=0), got {parsed}"
        raise argparse.ArgumentTypeError(msg)
    return parsed


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line arguments into a Namespace"""
    parser = argparse.ArgumentParser(
        description=(
            "Count Sourcegraph repos for each Bitbucket Server projectKeys / "
            "repositoryQuery config item, to find empty Bitbucket projects\n"
            "\n"
            "Set SRC_ENDPOINT and SRC_ACCESS_TOKEN via env, .env, or args\n"
            "Requires a site-admin access token"
        ),
        epilog=("Source: https://github.com/sourcegraph/professional-services-public"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="FILE",
        help=(
            f"Output CSV file path (default <endpoint-hostname>-{DEFAULT_OUTPUT_FILE})"
        ),
    )
    parser.add_argument(
        "--max-retries",
        type=non_negative_int,
        default=DEFAULT_MAX_RETRIES,
        metavar="int",
        help=(
            "Retries per GraphQL request after the initial attempt "
            f"(default {DEFAULT_MAX_RETRIES}; backoff 1s, 2s, 4s, ...)"
        ),
    )
    parser.add_argument(
        "--src-endpoint",
        default=None,
        metavar="URL",
        help="Sourcegraph endpoint URL (e.g. https://sourcegraph.example.com)",
    )
    parser.add_argument(
        "--src-access-token",
        default=None,
        metavar="TOKEN",
        help=(
            "Sourcegraph access token (must start with 'sgp_'); prefer the "
            "SRC_ACCESS_TOKEN environment variable"
        ),
    )
    return parser.parse_args(argv)


def configure_logging() -> None:
    """Send INFO-level logs to stderr for live feedback"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        stream=sys.stderr,
    )


def run(args: argparse.Namespace, endpoint: str, token: str) -> None:
    """Confirm the connection, then count repos per Bitbucket config item"""
    current_user = fetch_current_user(
        endpoint,
        token,
        max_retries=args.max_retries,
    )
    if current_user is None:
        die(
            f"SRC_ACCESS_TOKEN did not authenticate a user on {endpoint}; "
            "check the token is valid, not expired, and was created on "
            "this instance",
        )
    username, is_site_admin = current_user
    logger.info(
        "Connected to: %s as: %s (%s)",
        endpoint,
        username,
        "site admin" if is_site_admin else "non-admin",
    )
    if not is_site_admin:
        die(
            f"SRC_ACCESS_TOKEN lacks the needed permissions: reading code "
            f"host connections requires a site-admin token, and {username!r} "
            f"is not a site admin on {endpoint}",
        )
    connections = fetch_bitbucket_connections(
        endpoint,
        token,
        max_retries=args.max_retries,
    )
    items = collect_config_items(connections)
    if not items:
        logger.info("Nothing to count; exiting")
        return
    output_path = Path(
        args.output
        or f"{sanitize_endpoint_for_filename(endpoint)}-{DEFAULT_OUTPUT_FILE}",
    )
    count_and_report(
        endpoint,
        token,
        items,
        output_path,
        max_retries=args.max_retries,
    )


def log_http_error(error: HTTPRequestError) -> None:
    """Log headers and body of a non-2xx HTTP response"""
    for header, value in error.headers:
        logger.error("  %s: %s", header, value)
    body = error.body.decode(errors="replace")
    if body:
        logger.error("Response body:\n%s", body)


def main() -> None:
    """Entry point: configure logging, load env, parse args, run, handle errors"""
    configure_logging()
    args = parse_args(sys.argv[1:])
    load_dotenv()
    endpoint, token = require_credentials(args)
    try:
        run(args, endpoint, token)
    except HTTPRequestError as error:
        logger.error("HTTP %s %s from %s", error.status, error.reason, error.url)
        if error.status in (401, 403):
            die(
                "SRC_ACCESS_TOKEN was rejected by the server; check the "
                "token is valid, not expired, and was created on "
                f"{endpoint}",
            )
        if error.status in (404, 405):
            die(
                f"SRC_ENDPOINT ({endpoint}) does not appear to be a "
                "Sourcegraph GraphQL endpoint; check the URL (no path is "
                "needed, e.g. https://sourcegraph.example.com)",
            )
        log_http_error(error)
        sys.exit(1)
    except OSError as error:
        die(
            f"could not connect to SRC_ENDPOINT ({endpoint}): {error}; "
            "check the URL, your network, and any VPN or proxy",
        )
    except GraphQLError:
        logger.exception("GraphQL request failed")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl-C); exiting")
        sys.exit(130)


if __name__ == "__main__":
    main()
