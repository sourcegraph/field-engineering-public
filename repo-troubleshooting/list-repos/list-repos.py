#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import collections
import concurrent.futures
import contextlib
import csv
import http.client
import json
import logging
import os
import re
import shlex
import sys
import textwrap
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, TextIO, cast
from urllib.parse import ParseResult, urlparse, urlsplit, urlunsplit

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

logger = logging.getLogger(__name__)


# --- Tune-ables -----------------------------------------------------------------

DEFAULT_CLONING_ERRORS_FILE = "repos-with-cloning-errors.csv"
DEFAULT_CONCURRENCY = 16
DEFAULT_CSV_SCHEMA_FILE = "CSV_SCHEMA.md"
DEFAULT_INDEXING_ERRORS_FILE = "repos-with-indexing-errors.csv"
DEFAULT_LOG_FILE_STEM = "list-repos"
DEFAULT_OUTPUT_FILE = "repos.csv"
DEFAULT_SKIPPED_FILES_FILE = "repos-with-skipped-files.csv"
DEFAULT_SKIPPED_FILE_REASONS_FILE = "skipped-file-reasons.csv"
DEFAULT_STATS_FILE_PREFIX = "stats"
DEFAULT_MAX_RETRIES = 5
GRAPHQL_FIELD_COUNT_RETRY_HEADROOM_PERCENT = 95
PAGE_SIZE = 500
REQUEST_TIMEOUT_SECONDS = 60
REQUEST_TIMEOUT_SECONDS_WITH_COMMIT_COUNT = (
    600  # Counting commits server-side can be slow on big monorepos
)
SKIPPED_FILE_REASON_SEARCH_TIMEOUT_PARAMETER = (
    f"timeout:{REQUEST_TIMEOUT_SECONDS_WITH_COMMIT_COUNT}s"
)
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


# --- GraphQL queries ----------------------------------------------------------

SOURCEGRAPH_VERSION_QUERY = """
query SourcegraphVersion {
  site {
    productVersion
  }
}
"""

GRAPHQL_SCHEMA_QUERY = """
query ListReposSchema {
  __schema {
    queryType { name }
    mutationType { name }
    types {
      name
      fields(includeDeprecated: true) {
        name
        args { name }
        type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
              }
            }
          }
        }
      }
    }
  }
}
"""

# Every repository field used by the listing pipeline. Startup introspection
# removes unavailable fields before GraphQL validates the query.
REPOSITORY_SELECTION: dict[str, Any] = {
    "name": None,
    "id": None,
    "url": None,
    "isFork": None,
    "isArchived": None,
    "isPrivate": None,
    "createdAt": None,
    "mirrorInfo": {
        "remoteURL": None,
        "cloned": None,
        "cloneInProgress": None,
        "isCorrupted": None,
        "lastError": None,
        "lastSyncOutput": None,
        "corruptionLogs": {"timestamp": None, "reason": None},
        "byteSize": None,
        "lastChanged": None,
        "updatedAt": None,
        "nextSyncAt": None,
        "updateSchedule": {"intervalSeconds": None},
        "shard": None,
    },
    "textSearchIndex": {
        "status": {
            "updatedAt": None,
            "contentFilesCount": None,
            "contentByteSize": None,
            "indexByteSize": None,
            "indexShardsCount": None,
            "newLinesCount": None,
            "defaultBranchNewLinesCount": None,
            "otherBranchesNewLinesCount": None,
        },
        "lastIndexStatus": None,
        "lastIndexFailureMessage": None,
        "host": {"name": None},
        "refs": {
            "ref": {"displayName": None},
            "skippedIndexed": {"count": None, "query": None},
        },
    },
    "externalServices": {"nodes": {"displayName": None}},
}


@dataclass(frozen=True)
class GraphQLSchema:
    """GraphQL object fields and their unwrapped return type names"""

    query_type: str
    mutation_type: str | None
    field_types: dict[str, dict[str, str]]
    field_arguments: dict[str, dict[str, frozenset[str]]]

    def field_type(self, type_name: str, field_name: str) -> str | None:
        """Return the named return type for one field"""
        return self.field_types.get(type_name, {}).get(field_name)

    def has_path(self, type_name: str, path: tuple[str, ...]) -> bool:
        """Return whether every field in path exists from type_name"""
        current_type = type_name
        for field_name in path:
            next_type = self.field_type(current_type, field_name)
            if next_type is None:
                return False
            current_type = next_type
        return True


def named_graphql_type(raw_type: object) -> str | None:
    """Unwrap NON_NULL/LIST introspection data to its named type"""
    current = raw_type
    while isinstance(current, dict):
        current_dict = cast("dict[str, object]", current)
        name = current_dict.get("name")
        if isinstance(name, str) and name:
            return name
        current = current_dict.get("ofType")
    return None


def parse_graphql_schema(data: dict[str, Any]) -> GraphQLSchema:
    """Build the small schema index needed to shape queries"""
    raw_schema: dict[str, Any] = data.get("__schema") or {}
    raw_query_type: dict[str, Any] = raw_schema.get("queryType") or {}
    query_type = raw_query_type.get("name")
    if not isinstance(query_type, str) or not query_type:
        msg = "GraphQL introspection did not return a query type"
        raise GraphQLError(msg)
    raw_mutation_type: dict[str, Any] = raw_schema.get("mutationType") or {}
    mutation_type_value = raw_mutation_type.get("name")
    mutation_type = (
        mutation_type_value
        if isinstance(mutation_type_value, str) and mutation_type_value
        else None
    )
    field_types: dict[str, dict[str, str]] = {}
    field_arguments: dict[str, dict[str, frozenset[str]]] = {}
    for raw_type in raw_schema.get("types") or []:
        if not isinstance(raw_type, dict):
            continue
        type_name = raw_type.get("name")
        if not isinstance(type_name, str) or not type_name:
            continue
        type_fields: dict[str, str] = {}
        type_arguments: dict[str, frozenset[str]] = {}
        for raw_field in raw_type.get("fields") or []:
            if not isinstance(raw_field, dict):
                continue
            field_name = raw_field.get("name")
            return_type = named_graphql_type(raw_field.get("type"))
            if not isinstance(field_name, str) or return_type is None:
                continue
            type_fields[field_name] = return_type
            type_arguments[field_name] = frozenset(
                argument_name
                for argument in raw_field.get("args") or []
                if isinstance(argument, dict)
                and isinstance(argument_name := argument.get("name"), str)
            )
        field_types[type_name] = type_fields
        field_arguments[type_name] = type_arguments
    return GraphQLSchema(
        query_type=query_type,
        mutation_type=mutation_type,
        field_types=field_types,
        field_arguments=field_arguments,
    )


def build_supported_selection(
    schema: GraphQLSchema,
    type_name: str,
    selection: dict[str, Any],
    *,
    indent: int,
    field_arguments: dict[str, str] | None = None,
) -> str:
    """Render only selection fields present in schema"""
    lines: list[str] = []
    for field_name, child_selection in selection.items():
        child_type = schema.field_type(type_name, field_name)
        if child_type is None:
            continue
        arguments = (field_arguments or {}).get(field_name, "")
        if field_name == "externalServices" and "first" in schema.field_arguments.get(
            type_name,
            {},
        ).get(field_name, frozenset()):
            arguments = "(first: 100)"
        prefix = " " * indent + field_name + arguments
        if child_selection is None:
            lines.append(prefix)
            continue
        rendered_children = build_supported_selection(
            schema,
            child_type,
            child_selection,
            indent=indent + 2,
            field_arguments=field_arguments,
        )
        if rendered_children:
            lines.extend((prefix + " {", rendered_children, " " * indent + "}"))
    return "\n".join(lines)


def repository_type_name(schema: GraphQLSchema) -> str:
    """Return Repository's actual type name through Query.repositories"""
    connection_type = schema.field_type(schema.query_type, "repositories")
    repository_type = (
        schema.field_type(connection_type, "nodes") if connection_type else None
    )
    if repository_type is None:
        msg = "GraphQL schema does not expose Query.repositories.nodes"
        raise GraphQLError(msg)
    return repository_type


def build_repo_node_fragment(
    schema: GraphQLSchema,
    *,
    can_read_protected_fields: bool,
) -> str:
    """Return the shared Repository fragment supported by this instance"""
    repository_type = repository_type_name(schema)
    selection = dict(REPOSITORY_SELECTION)
    if not can_read_protected_fields:
        selection.pop("externalServices")
    rendered = build_supported_selection(
        schema,
        repository_type,
        selection,
        indent=2,
    )
    return f"fragment RepoNodeFields on {repository_type} {{\n{rendered}\n}}\n"


def build_repository_listing_query(
    schema: GraphQLSchema,
    *,
    can_read_protected_fields: bool,
) -> str:
    """Return a paginated query containing only supported repository fields"""
    return (
        build_repo_node_fragment(
            schema,
            can_read_protected_fields=can_read_protected_fields,
        )
        + """
query ListRepos($first: Int!, $after: String) {
  repositories(first: $first, after: $after) {
    nodes {
      ...RepoNodeFields
    }
    totalCount
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""
    )


# Single-repo lookup used by the scoped variants of --count-commits / --reclone
# / --reindex. Returns the same field set as the listing query (via the shared
# fragment) so the rest of the pipeline (build_row, write_csv, the error/skip
# detectors, etc.) can treat the result identically to a listing-page node
def build_single_repo_query(
    schema: GraphQLSchema,
    *,
    can_read_protected_fields: bool,
) -> str:
    """Return a supported single-repository lookup query"""
    return (
        build_repo_node_fragment(
            schema,
            can_read_protected_fields=can_read_protected_fields,
        )
        + """
query SingleRepo($name: String!) {
  repository(name: $name) {
    ...RepoNodeFields
  }
}
"""
    )


# Used once at startup to gate protected fields and site-admin mutations
CURRENT_USER_QUERY = """
query CurrentUserPermissions {
  currentUser {
    username
    siteAdmin
    permissions {
      nodes {
        namespace
        action
      }
    }
  }
}
"""

REPOSITORY_MANAGEMENT_PERMISSION_NAMESPACE = "REPO_MANAGEMENT"
READ_PERMISSION_ACTION = "READ"

# Per-repo query for exact rev count, cleanup metadata, and all-refs proxy.
# Omitting ancestors.first asks gitserver for the full reachable commit count.
COMMIT_COUNT_REPOSITORY_SELECTION: dict[str, Any] = {
    "commit": {"ancestors": {"totalCount": None}},
    "mirrorInfo": {
        "lastCleanedAt": None,
        "cleanupSchedule": {"due": None, "intervalSeconds": None},
        "cleanupQueue": {"index": None, "optimizing": None},
        "repositoryStatistics": {"packfiles": {"lastFullRepack": None}},
    },
}


def build_commit_count_query(schema: GraphQLSchema) -> str:
    """Return a commit-count query without unsupported repository fields"""
    repository_type = repository_type_name(schema)
    repository_selection = build_supported_selection(
        schema,
        repository_type,
        COMMIT_COUNT_REPOSITORY_SELECTION,
        indent=4,
        field_arguments={"commit": "(rev: $rev)"},
    )
    return (
        """
query CommitCount($name: String!, $rev: String!, $allRefsSearch: String!) {
  repository(name: $name) {
"""
        + repository_selection
        + """
  }
  search(query: $allRefsSearch, version: V3) {
    results {
      matchCount
    }
  }
}
"""
    )


# Approximate all-refs count. Not comparable to the exact rev count
# Repo anchoring, regex escaping, and timeout prevent slow unbounded searches
ALL_REFS_COMMIT_SEARCH_TEMPLATE = (
    "r:^{repo}$ rev:*refs/heads/*:*refs/tags/* type:commit count:all timeout:120s"
)


def build_all_refs_search(repo_name: str) -> str:
    """Build the SG search query that counts commits across all branches+tags"""
    return ALL_REFS_COMMIT_SEARCH_TEMPLATE.format(repo=re.escape(repo_name))


# --- Per-repo arbitrary search (--run-search) ---------------------------------

# Wrap the user's pattern with a repo anchor, count:all, and a server timeout
RUN_SEARCH_QUERY_TEMPLATE = "r:^{repo}$ {pattern} count:all timeout:120s"

RUN_SEARCH_GRAPHQL = """
query RunSearch($query: String!) {
  search(query: $query, version: V3) {
    results {
      matchCount
      limitHit
      alert {
        title
      }
    }
  }
}
"""


def build_run_search_query(repo_name: str, pattern: str) -> str:
    """Build a per-repo --run-search query while leaving pattern syntax verbatim"""
    return RUN_SEARCH_QUERY_TEMPLATE.format(
        repo=re.escape(repo_name),
        pattern=pattern,
    )


RECLONE_MUTATION = """
mutation Reclone($repo: ID!) {
  recloneRepository(repo: $repo) {
    alwaysNil
  }
}
"""

REINDEX_MUTATION = """
mutation Reindex($repository: ID!) {
  reindexRepository(repository: $repository) {
    alwaysNil
  }
}
"""

SKIPPED_FILES_REASON_QUERY = """
query SkippedFileReasons($query: String!) {
  search(query: $query, version: V2) {
    results {
      matchCount
      limitHit
      alert {
        title
        description
      }
      results {
        ... on FileMatch {
          repository {
            name
          }
          file {
            path
            byteSize
          }
          chunkMatches {
            content
          }
        }
      }
    }
  }
}
"""

REPO_REV_VALIDATION_QUERY = """
query ValidateRepoRev($name: String!, $rev: String!) {
  repository(name: $name) {
    name
    defaultBranch {
      displayName
    }
    commit(rev: $rev) {
      oid
    }
    textSearchIndex {
      refs {
        ref {
          displayName
        }
        indexed
        indexedCommit {
          oid
        }
        skippedIndexed {
          count
          query
        }
      }
    }
  }
}
"""


# --- Metadata extractors used by the COLUMNS table --------------------------------


def decode_repo_id(base64_id: str) -> int:
    """Decode Sourcegraph's base64 repo ID to its integer form"""
    return int(base64.b64decode(base64_id).decode().split(":", 1)[1])


def get_path(repo: dict[str, Any], path: str) -> object | None:
    """Walk a dotted dict path; return None if any step is missing"""
    current: object = repo
    for key in path.split("."):
        if not isinstance(current, dict):
            return None
        # cast keeps strict type-checkers happy: isinstance() on `object` only
        # narrows to dict[Unknown, Unknown], so we re-view it concretely
        current_dict = cast("dict[str, object]", current)
        next_value = current_dict.get(key)
        if next_value is None:
            return None
        current = next_value
    return current


def get_path_mb(repo: dict[str, Any], path: str) -> int | None:
    """Like get_path, but convert to megabytes"""
    value = get_path(repo, path)
    if isinstance(value, (int, str)):
        return int(value) // (1024 * 1024)
    return None


def derive_mirror_status(repo: dict[str, Any]) -> str:
    """Summarize the repo's mirror state into a single status string"""
    mirror: dict[str, Any] = repo.get("mirrorInfo") or {}
    if mirror.get("isCorrupted"):
        return "corrupted"
    if mirror.get("lastError"):
        return "errored"
    if mirror.get("cloneInProgress"):
        return "cloning"
    if mirror.get("cloned"):
        return "cloned"
    return "not_cloned"


def seconds_relative_to_now(timestamp: object, *, future: bool) -> int | None:
    """Return seconds since/until an RFC3339 timestamp, or None if invalid"""
    if not isinstance(timestamp, str) or not timestamp:
        return None
    try:
        ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return None
    now = datetime.now(timezone.utc)
    delta = (ts - now) if future else (now - ts)
    return int(delta.total_seconds())


def derive_index_status(repo: dict[str, Any]) -> str:
    """Summarize the repo's search-index state as 'indexed' or 'not_indexed'"""
    return (
        "indexed"
        if get_path(repo, "textSearchIndex.status") is not None
        else "not_indexed"
    )


def redact_remote_url(repo: dict[str, Any]) -> str | None:
    """Redact mirrorInfo.remoteURL userinfo before it reaches any CSV output"""
    raw = get_path(repo, "mirrorInfo.remoteURL")
    if not isinstance(raw, str):
        return None
    if not raw:
        return raw
    parts = urlsplit(raw)
    if not parts.scheme or not parts.netloc or "@" not in parts.netloc:
        return raw
    _, _, host_port = parts.netloc.rpartition("@")
    new_netloc = f"REDACTED@{host_port}"
    return urlunsplit(
        (parts.scheme, new_netloc, parts.path, parts.query, parts.fragment),
    )


def join_external_services(repo: dict[str, Any]) -> str:
    """Combine all attached code-host display names into one ';'-separated string"""
    services: dict[str, Any] = repo.get("externalServices") or {}
    nodes: list[dict[str, Any]] = services.get("nodes") or []
    return "; ".join(str(es["displayName"]) for es in nodes)


def join_corruption_logs(repo: dict[str, Any]) -> str:
    """Flatten corruptionLogs into a ';'-separated 'timestamp: reason' string"""
    mirror: dict[str, Any] = repo.get("mirrorInfo") or {}
    logs: list[dict[str, Any]] = mirror.get("corruptionLogs") or []
    return "; ".join(
        f"{log.get('timestamp', '')}: {log.get('reason', '')}" for log in logs
    )


def truncate_sync_output(repo: dict[str, Any]) -> str | None:
    """Return lastSyncOutput truncated to first 5 + last 5 lines"""
    value = get_path(repo, "mirrorInfo.lastSyncOutput")
    if not isinstance(value, str):
        return None
    return truncate_lines(value)


def truncate_lines(value: str, head: int = 5, tail: int = 5) -> str:
    """Truncate a multi-line string to the first `head` + last `tail` lines"""
    lines = value.splitlines()
    if len(lines) <= head + tail:
        return value
    omitted = len(lines) - head - tail
    return "\n".join(
        [*lines[:head], f"... [{omitted} lines truncated] ...", *lines[-tail:]],
    )


def has_cloning_error(repo: dict[str, Any]) -> bool:
    """Return True for errored, corrupted, or not-yet-cloned repos"""
    return derive_mirror_status(repo) in {"errored", "corrupted", "not_cloned"}


def has_indexing_error(repo: dict[str, Any]) -> bool:
    """Return True for cloned repos with a missing or failed search index"""
    if derive_mirror_status(repo) != "cloned":
        return False
    if get_path(repo, "textSearchIndex.status") is None:
        return True
    last_status = get_path(repo, "textSearchIndex.lastIndexStatus")
    return isinstance(last_status, str) and last_status.upper() == "FAILURE"


def _index_refs(repo: dict[str, Any]) -> list[dict[str, Any]]:
    """Return textSearchIndex.refs (or [] when missing)"""
    index: dict[str, Any] = repo.get("textSearchIndex") or {}
    refs: list[dict[str, Any]] = index.get("refs") or []
    return refs


def total_skipped_files(repo: dict[str, Any]) -> int:
    """Sum skippedIndexed.count across every indexed ref of the repo"""
    total = 0
    for ref in _index_refs(repo):
        skipped: dict[str, Any] = ref.get("skippedIndexed") or {}
        count = skipped.get("count")
        if count is not None:
            total += int(count)
    return total


def refs_with_skips(repo: dict[str, Any]) -> str:
    """Return ';'-joined '<refName>=<count>' for refs with skipped files"""
    return "; ".join(
        f"{name}={skipped_count}"
        for name, skipped_count in refs_with_skipped_file_counts(repo)
    )


def refs_with_skipped_file_queries(repo: dict[str, Any]) -> list[tuple[str, int, str]]:
    """Return (ref name, skipped count, API search query) for refs with skips"""
    refs: list[tuple[str, int, str]] = []
    for ref in _index_refs(repo):
        skipped: dict[str, Any] = ref.get("skippedIndexed") or {}
        count = skipped.get("count")
        if count is None:
            continue
        skipped_count = int(count)
        if skipped_count <= 0:
            continue
        ref_node: dict[str, Any] = ref.get("ref") or {}
        name = str(ref_node.get("displayName") or "")
        if name:
            refs.append((name, skipped_count, str(skipped.get("query") or "")))
    return refs


def refs_with_skipped_file_counts(repo: dict[str, Any]) -> list[tuple[str, int]]:
    """Return (ref name, skipped count) pairs for refs with skipped files"""
    return [
        (name, skipped_count)
        for name, skipped_count, _query in refs_with_skipped_file_queries(repo)
    ]


def head_skipped_query(repo: dict[str, Any]) -> str:
    """Return skippedIndexed.query for HEAD, or the first skipped ref"""
    head_query = ""
    fallback = ""
    for ref in _index_refs(repo):
        skipped: dict[str, Any] = ref.get("skippedIndexed") or {}
        count = skipped.get("count") or 0
        if int(count) <= 0:
            continue
        query = str(skipped.get("query") or "")
        ref_node: dict[str, Any] = ref.get("ref") or {}
        name = str(ref_node.get("displayName") or "")
        if name == "HEAD":
            head_query = query
            break
        if not fallback:
            fallback = query
    return head_query or fallback


def has_skipped_files(repo: dict[str, Any]) -> bool:
    """Return True if zoekt skipped at least one file for this repo"""
    return total_skipped_files(repo) > 0


def fetch_commit_count(
    endpoint: str,
    token: str,
    repo_name: str,
    rev: str = "HEAD",
    *,
    schema: GraphQLSchema,
    unavailable_values: dict[str, str],
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> tuple[int | None, int | None, float, list[Any]]:
    """Return exact rev count, approximate all-refs count, elapsed time, extras"""
    empty_extras = extract_csv_values(
        {},
        COMMIT_COUNT_OPTIMIZATION_COLUMNS,
        unavailable_values,
    )
    start = time.monotonic()
    try:
        data = graphql_request(
            endpoint,
            token,
            build_commit_count_query(schema),
            {
                "name": repo_name,
                "rev": rev,
                "allRefsSearch": build_all_refs_search(repo_name),
            },
            timeout=REQUEST_TIMEOUT_SECONDS_WITH_COMMIT_COUNT,
            max_retries=max_retries,
            request_description=f"Commit count for {repo_name}",
        )
    except (GraphQLError, HTTPRequestError) as exc:
        elapsed = time.monotonic() - start
        logger.warning("commit-count query failed for %s: %s", repo_name, exc)
        return None, None, elapsed, empty_extras
    except OSError as exc:
        elapsed = time.monotonic() - start
        logger.warning(
            "commit-count network error for %s: %s",
            repo_name,
            exc,
        )
        return None, None, elapsed, empty_extras
    elapsed = time.monotonic() - start
    repo: dict[str, Any] = data.get("repository") or {}
    commit: dict[str, Any] = repo.get("commit") or {}
    ancestors: dict[str, Any] = commit.get("ancestors") or {}
    default_count_raw = ancestors.get("totalCount")
    default_count: int | None = (
        default_count_raw if isinstance(default_count_raw, int) else None
    )
    search_block: dict[str, Any] = data.get("search") or {}
    search_results: dict[str, Any] = search_block.get("results") or {}
    all_refs_count_raw = search_results.get("matchCount")
    all_refs_count: int | None = (
        all_refs_count_raw if isinstance(all_refs_count_raw, int) else None
    )
    optimization_values = extract_csv_values(
        repo,
        COMMIT_COUNT_OPTIMIZATION_COLUMNS,
        unavailable_values,
    )
    return default_count, all_refs_count, elapsed, optimization_values


def fetch_run_search(
    endpoint: str,
    token: str,
    repo_name: str,
    pattern: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> tuple[int | None, float, bool, str | None]:
    """Return --run-search match count, elapsed time, limit flag, and alert"""
    start = time.monotonic()
    query = build_run_search_query(repo_name, pattern)
    try:
        data = graphql_request(
            endpoint,
            token,
            RUN_SEARCH_GRAPHQL,
            {"query": query},
            max_retries=max_retries,
            request_description=f"Search {pattern} in {repo_name}",
        )
    except (GraphQLError, HTTPRequestError) as exc:
        elapsed = time.monotonic() - start
        logger.warning("run-search query failed for %s: %s", repo_name, exc)
        return None, elapsed, False, None
    except OSError as exc:
        elapsed = time.monotonic() - start
        logger.warning("run-search network error for %s: %s", repo_name, exc)
        return None, elapsed, False, None
    elapsed = time.monotonic() - start
    search_block: dict[str, Any] = data.get("search") or {}
    results: dict[str, Any] = search_block.get("results") or {}
    raw_count = results.get("matchCount")
    match_count: int | None = raw_count if isinstance(raw_count, int) else None
    limit_hit = bool(results.get("limitHit"))
    alert: dict[str, Any] = results.get("alert") or {}
    alert_title_raw = alert.get("title")
    alert_title: str | None = (
        alert_title_raw if isinstance(alert_title_raw, str) else None
    )
    return match_count, elapsed, limit_hit, alert_title


# --- CSV format -----------------------------------------------------------

# Each entry is (csv_column_name, extractor_function). Keeping the column name
# next to the function that produces its value eliminates the risk of the
# header drifting out of sync with the row data
COLUMNS: list[tuple[str, Callable[[dict[str, Any]], Any], str, bool, str]] = [
    (
        "id",
        lambda r: decode_repo_id(r["id"]),
        "Numeric Sourcegraph database ID for the repository, decoded "
        "locally from the base64 GraphQL global ID; useful when correlating "
        "with the `repo` table or admin URLs",
        False,
        "integer",
    ),
    (
        "url",
        lambda r: r.get("url"),
        "URL to the repository on this Sourcegraph instance",
        False,
        "string",
    ),
    (
        "mirrorInfo.remoteURL",
        redact_remote_url,
        "Clone URL of the upstream repository on the code host",
        True,
        "string",
    ),
    (
        "externalServices",
        join_external_services,
        "Display names of the external service(s) which clone this repository",
        True,
        "string (semicolon-joined)",
    ),
    (
        "mirrorInfo.status",
        derive_mirror_status,
        "Single-word summary of the repo's mirror state, derived locally "
        "from `mirrorInfo`",
        False,
        "enum (corrupted, errored, cloning, cloned, not_cloned)",
    ),
    (
        "isFork",
        lambda r: r.get("isFork"),
        "Whether this repository is a fork",
        False,
        "boolean",
    ),
    (
        "isArchived",
        lambda r: r.get("isArchived"),
        "Whether this repository has been archived on the code host",
        False,
        "boolean",
    ),
    (
        "isPrivate",
        lambda r: r.get("isPrivate"),
        "Whether this repository is private",
        False,
        "boolean",
    ),
    (
        "mirrorInfo.byteSize(MB)",
        lambda r: get_path_mb(r, "mirrorInfo.byteSize"),
        "On-disk size of the bare-cloned repository, in megabytes",
        False,
        "float",
    ),
    (
        "createdAt",
        lambda r: r.get("createdAt"),
        "Timestamp the repo was first cloned to your Sourcegraph instance",
        False,
        "timestamp",
    ),
    (
        "mirrorInfo.lastChanged",
        lambda r: get_path(r, "mirrorInfo.lastChanged"),
        "Timestamp of the most recent commit in the repo",
        False,
        "timestamp",
    ),
    (
        "mirrorInfo.updatedAt",
        lambda r: get_path(r, "mirrorInfo.updatedAt"),
        "Timestamp of the most recent successful sync of the repo from the code host",
        False,
        "timestamp",
    ),
    (
        "mirrorInfo.secondsSinceUpdatedAt",
        lambda r: seconds_relative_to_now(
            get_path(r, "mirrorInfo.updatedAt"),
            future=False,
        ),
        "Integer seconds elapsed between `mirrorInfo.updatedAt` and when the script was run",
        False,
        "integer",
    ),
    (
        "mirrorInfo.nextSyncAt",
        lambda r: get_path(r, "mirrorInfo.nextSyncAt"),
        "Timestamp the repo is next scheduled to be synced from upstream",
        False,
        "timestamp",
    ),
    (
        "mirrorInfo.secondsUntilNextSyncAt",
        lambda r: seconds_relative_to_now(
            get_path(r, "mirrorInfo.nextSyncAt"),
            future=True,
        ),
        "Integer seconds remaining until `mirrorInfo.nextSyncAt`",
        False,
        "integer",
    ),
    (
        "mirrorInfo.updateSchedule.intervalSeconds",
        lambda r: get_path(r, "mirrorInfo.updateSchedule.intervalSeconds"),
        "Interval, in seconds, between scheduled mirror updates. Default max is 28800 seconds (8 hours), but is shortened for busy / popular repos",
        False,
        "integer",
    ),
    (
        "mirrorInfo.shard",
        lambda r: get_path(r, "mirrorInfo.shard"),
        "Pod name of the gitserver shard which holds this repo's clone",
        True,
        "string",
    ),
    (
        "textSearchIndex.status",
        derive_index_status,
        "Search-index state, derived locally: "
        "`indexed` if Zoekt has built an index for this repo, "
        "`not_indexed` otherwise",
        False,
        "enum (indexed, not_indexed)",
    ),
    (
        "textSearchIndex.lastIndexStatus",
        lambda r: get_path(r, "textSearchIndex.lastIndexStatus"),
        "Most recent persisted text search indexing attempt result. "
        "Blank when no attempt was reported",
        False,
        "enum (SUCCESS, FAILURE)",
    ),
    (
        "textSearchIndex.lastIndexFailureMessage",
        lambda r: get_path(r, "textSearchIndex.lastIndexFailureMessage"),
        "Failure message from the most recent persisted text search indexing "
        "attempt. Blank when no failure was reported",
        False,
        "string",
    ),
    (
        "textSearchIndex.status.updatedAt",
        lambda r: get_path(r, "textSearchIndex.status.updatedAt"),
        "Timestamp the repo was last indexed for fast search. It should be shortly after mirrorInfo.lastChanged, as indexing jobs are scheduled after new commits are fetched",
        False,
        "timestamp",
    ),
    (
        "textSearchIndex.status.contentFilesCount",
        lambda r: get_path(r, "textSearchIndex.status.contentFilesCount"),
        "Number of files included in the index. Note that some files are excluded from indexing, ex. binary files",
        False,
        "integer",
    ),
    (
        "textSearchIndex.status.contentByteSize(MB)",
        lambda r: get_path_mb(r, "textSearchIndex.status.contentByteSize"),
        "Size, in megabytes, of the source content that was indexed. Note that some files are excluded from indexing, ex. binary files",
        False,
        "float",
    ),
    (
        "textSearchIndex.status.indexByteSize(MB)",
        lambda r: get_path_mb(r, "textSearchIndex.status.indexByteSize"),
        "Size of the Zoekt search index for this repo, in megabytes",
        False,
        "float",
    ),
    (
        "textSearchIndex.status.indexShardsCount",
        lambda r: get_path(r, "textSearchIndex.status.indexShardsCount"),
        "Number of Zoekt shards that make up this repo's index",
        False,
        "integer",
    ),
    (
        "textSearchIndex.status.newLinesCount",
        lambda r: get_path(r, "textSearchIndex.status.newLinesCount"),
        "Total number of lines across every indexed branch",
        False,
        "integer",
    ),
    (
        "textSearchIndex.status.defaultBranchNewLinesCount",
        lambda r: get_path(r, "textSearchIndex.status.defaultBranchNewLinesCount"),
        "Number of lines indexed on the repo's default branch",
        False,
        "integer",
    ),
    (
        "textSearchIndex.status.otherBranchesNewLinesCount",
        lambda r: get_path(r, "textSearchIndex.status.otherBranchesNewLinesCount"),
        "Number of lines indexed across non-default branches",
        False,
        "integer",
    ),
    (
        "textSearchIndex.host.name",
        lambda r: get_path(r, "textSearchIndex.host.name"),
        "Pod name of the indexserver shard which holds this repo's index",
        False,
        "string",
    ),
]

CSV_COLUMNS = [name for name, _, _, _, _ in COLUMNS]
URL_COLUMN_INDEX = CSV_COLUMNS.index("url")

# Cleanup metadata appended only when --count-commits runs its per-repo query
# repositoryStatistics may be empty for non-admin tokens or non-cloned repos
COMMIT_COUNT_OPTIMIZATION_COLUMNS: list[
    tuple[str, Callable[[dict[str, Any]], Any], str, bool, str]
] = [
    (
        "mirrorInfo.lastCleanedAt",
        lambda r: get_path(r, "mirrorInfo.lastCleanedAt"),
        "Timestamp of the last successful gitserver cleanup ('gc') of this repo",
        False,
        "timestamp",
    ),
    (
        "mirrorInfo.cleanupSchedule.due",
        lambda r: get_path(r, "mirrorInfo.cleanupSchedule.due"),
        "Timestamp the repo is next scheduled to be cleaned up by gitserver",
        False,
        "timestamp",
    ),
    (
        "mirrorInfo.cleanupSchedule.intervalSeconds",
        lambda r: get_path(r, "mirrorInfo.cleanupSchedule.intervalSeconds"),
        "Interval, in seconds, between scheduled cleanup runs",
        False,
        "integer",
    ),
    (
        "mirrorInfo.cleanupQueue.index",
        lambda r: get_path(r, "mirrorInfo.cleanupQueue.index"),
        "Position of the repo in the gitserver cleanup queue",
        False,
        "integer",
    ),
    (
        "mirrorInfo.cleanupQueue.optimizing",
        lambda r: get_path(r, "mirrorInfo.cleanupQueue.optimizing"),
        "Whether gitserver is currently running optimization on this repo",
        False,
        "boolean",
    ),
    (
        "mirrorInfo.repositoryStatistics.packfiles.lastFullRepack",
        lambda r: get_path(
            r,
            "mirrorInfo.repositoryStatistics.packfiles.lastFullRepack",
        ),
        "Timestamp of the most recent full repack of this repo's packfiles",
        True,
        "timestamp",
    ),
]

# Optional --count-commits columns appended to each per-repo CSV
COMMIT_COUNT_COLUMNS: list[tuple[str, str, bool, str]] = [
    (
        "defaultBranch.target.commit.ancestors.totalCount",
        "Number of commits reachable from HEAD on the default branch — "
        "equivalent to `git rev-list --count HEAD`, computed by gitserver",
        False,
        "integer",
    ),
    (
        "allRefs.search.matchCount",
        "Approximate number of commits across every branch, "
        "computed via Sourcegraph's commit-search API",
        False,
        "integer",
    ),
    (
        "commitCount.queryTimeSeconds",
        "Wall-clock seconds the per-repo commit-count GraphQL request "
        "took. Useful for spotting which repos are expensive to count",
        False,
        "float",
    ),
    *(
        (name, desc, admin, vtype)
        for name, _, desc, admin, vtype in COMMIT_COUNT_OPTIMIZATION_COLUMNS
    ),
]

# Optional --run-search columns appended after --count-commits columns
RUN_SEARCH_COLUMNS: list[tuple[str, str, bool, str]] = [
    (
        "runSearch.matchCount",
        "Number of search matches the Sourcegraph search API reported "
        "for the user-supplied `--run-search` pattern, for this repo",
        False,
        "integer",
    ),
    (
        "runSearch.queryTimeSeconds",
        "Wall-clock seconds the per-repo `--run-search` GraphQL request took",
        False,
        "float",
    ),
    (
        "runSearch.limitHit",
        "`True` when the search hit a limit, so the results are incomplete",
        False,
        "boolean",
    ),
    (
        "runSearch.alertTitle",
        "Title of the search-API alert when the server's `timeout:` "
        "budget was exceeded or the query was malformed",
        False,
        "string",
    ),
]

# Extra columns appended only to the cloning-errors CSV
CLONING_ERROR_EXTRA_COLUMNS: list[
    tuple[str, Callable[[dict[str, Any]], Any], str, bool, str]
] = [
    (
        "mirrorInfo.isCorrupted",
        lambda r: get_path(r, "mirrorInfo.isCorrupted"),
        "Whether Sourcegraph has detected the on-disk clone is corrupted",
        False,
        "boolean",
    ),
    (
        "mirrorInfo.lastError",
        lambda r: get_path(r, "mirrorInfo.lastError"),
        "Last error message returned by gitserver while fetching or "
        "cloning this repo, if any",
        False,
        "string",
    ),
    (
        "mirrorInfo.lastSyncOutput",
        truncate_sync_output,
        "Output of the most recent sync attempt, truncated to the first 5 and last 5 lines",
        False,
        "string",
    ),
    (
        "mirrorInfo.corruptionLogs",
        join_corruption_logs,
        "`timestamp: reason` entries for the most recent corruption events",
        False,
        "string (semicolon-joined)",
    ),
]
CLONING_ERROR_CSV_COLUMNS = CSV_COLUMNS + [
    name for name, _, _, _, _ in CLONING_ERROR_EXTRA_COLUMNS
]
# The indexing-errors CSV reuses CSV_COLUMNS verbatim — Sourcegraph's GraphQL
# does not expose any per-repo zoekt error fields beyond textSearchIndex.status

# Extra columns appended only to the skipped-files CSV. The query is the
# Sourcegraph search query produced by the API; running it lists each skipped
# file along with its NOT-INDEXED reason (too-large / binary / too-many-trigrams
# / too-small / blob-missing)
SKIPPED_FILES_EXTRA_COLUMNS: list[
    tuple[str, Callable[[dict[str, Any]], Any], str, bool, str]
] = [
    (
        "skippedIndexed.totalCount",
        total_skipped_files,
        "Count of files Zoekt excluded while indexing this repo",
        False,
        "integer",
    ),
    (
        "skippedIndexed.refsWithSkips",
        refs_with_skips,
        "`<refName>=<count>` entries for every indexed ref which "
        "has at least one excluded file",
        False,
        "string (semicolon-joined)",
    ),
    (
        "skippedIndexed.headQuery",
        head_skipped_query,
        "Sourcegraph search query that lists every excluded file on HEAD. "
        "This search is run when the script is run with the --skipped-files-reason arg",
        False,
        "string",
    ),
]
SKIPPED_FILES_CSV_COLUMNS = CSV_COLUMNS + [
    name for name, _, _, _, _ in SKIPPED_FILES_EXTRA_COLUMNS
]

# Source schema paths needed to produce each listing-derived CSV column.
# Locally derived columns list every field required for a trustworthy value.
CSV_COLUMN_SCHEMA_PATHS: dict[str, tuple[tuple[str, ...], ...]] = {
    "id": (("id",),),
    "url": (("url",),),
    "mirrorInfo.remoteURL": (("mirrorInfo", "remoteURL"),),
    "externalServices": (("externalServices", "nodes", "displayName"),),
    "mirrorInfo.status": (
        ("mirrorInfo", "isCorrupted"),
        ("mirrorInfo", "lastError"),
        ("mirrorInfo", "cloneInProgress"),
        ("mirrorInfo", "cloned"),
    ),
    "isFork": (("isFork",),),
    "isArchived": (("isArchived",),),
    "isPrivate": (("isPrivate",),),
    "mirrorInfo.byteSize(MB)": (("mirrorInfo", "byteSize"),),
    "createdAt": (("createdAt",),),
    "mirrorInfo.lastChanged": (("mirrorInfo", "lastChanged"),),
    "mirrorInfo.updatedAt": (("mirrorInfo", "updatedAt"),),
    "mirrorInfo.secondsSinceUpdatedAt": (("mirrorInfo", "updatedAt"),),
    "mirrorInfo.nextSyncAt": (("mirrorInfo", "nextSyncAt"),),
    "mirrorInfo.secondsUntilNextSyncAt": (("mirrorInfo", "nextSyncAt"),),
    "mirrorInfo.updateSchedule.intervalSeconds": (
        ("mirrorInfo", "updateSchedule", "intervalSeconds"),
    ),
    "mirrorInfo.shard": (("mirrorInfo", "shard"),),
    "textSearchIndex.status": (("textSearchIndex", "status", "updatedAt"),),
    "textSearchIndex.lastIndexStatus": (("textSearchIndex", "lastIndexStatus"),),
    "textSearchIndex.lastIndexFailureMessage": (
        ("textSearchIndex", "lastIndexFailureMessage"),
    ),
    "textSearchIndex.status.updatedAt": (("textSearchIndex", "status", "updatedAt"),),
    "textSearchIndex.status.contentFilesCount": (
        ("textSearchIndex", "status", "contentFilesCount"),
    ),
    "textSearchIndex.status.contentByteSize(MB)": (
        ("textSearchIndex", "status", "contentByteSize"),
    ),
    "textSearchIndex.status.indexByteSize(MB)": (
        ("textSearchIndex", "status", "indexByteSize"),
    ),
    "textSearchIndex.status.indexShardsCount": (
        ("textSearchIndex", "status", "indexShardsCount"),
    ),
    "textSearchIndex.status.newLinesCount": (
        ("textSearchIndex", "status", "newLinesCount"),
    ),
    "textSearchIndex.status.defaultBranchNewLinesCount": (
        ("textSearchIndex", "status", "defaultBranchNewLinesCount"),
    ),
    "textSearchIndex.status.otherBranchesNewLinesCount": (
        ("textSearchIndex", "status", "otherBranchesNewLinesCount"),
    ),
    "textSearchIndex.host.name": (("textSearchIndex", "host", "name"),),
    "mirrorInfo.isCorrupted": (("mirrorInfo", "isCorrupted"),),
    "mirrorInfo.lastError": (("mirrorInfo", "lastError"),),
    "mirrorInfo.lastSyncOutput": (("mirrorInfo", "lastSyncOutput"),),
    "mirrorInfo.corruptionLogs": (
        ("mirrorInfo", "corruptionLogs", "timestamp"),
        ("mirrorInfo", "corruptionLogs", "reason"),
    ),
    "mirrorInfo.lastCleanedAt": (("mirrorInfo", "lastCleanedAt"),),
    "mirrorInfo.cleanupSchedule.due": (("mirrorInfo", "cleanupSchedule", "due"),),
    "mirrorInfo.cleanupSchedule.intervalSeconds": (
        ("mirrorInfo", "cleanupSchedule", "intervalSeconds"),
    ),
    "mirrorInfo.cleanupQueue.index": (("mirrorInfo", "cleanupQueue", "index"),),
    "mirrorInfo.cleanupQueue.optimizing": (
        ("mirrorInfo", "cleanupQueue", "optimizing"),
    ),
    "mirrorInfo.repositoryStatistics.packfiles.lastFullRepack": (
        (
            "mirrorInfo",
            "repositoryStatistics",
            "packfiles",
            "lastFullRepack",
        ),
    ),
    "skippedIndexed.totalCount": (
        ("textSearchIndex", "refs", "skippedIndexed", "count"),
    ),
    "skippedIndexed.refsWithSkips": (
        ("textSearchIndex", "refs", "ref", "displayName"),
        ("textSearchIndex", "refs", "skippedIndexed", "count"),
    ),
    "skippedIndexed.headQuery": (
        ("textSearchIndex", "refs", "ref", "displayName"),
        ("textSearchIndex", "refs", "skippedIndexed", "count"),
        ("textSearchIndex", "refs", "skippedIndexed", "query"),
    ),
}


def unavailable_csv_column_values(
    schema: GraphQLSchema,
    sourcegraph_version: str,
) -> dict[str, str]:
    """Return version markers for columns unsupported by the target schema"""
    repository_type = repository_type_name(schema)
    marker = f"field not in v{sourcegraph_version}"
    return {
        column_name: marker
        for column_name, required_paths in CSV_COLUMN_SCHEMA_PATHS.items()
        if not all(schema.has_path(repository_type, path) for path in required_paths)
    }


def admin_required_csv_column_values() -> dict[str, str]:
    """Return markers for columns unavailable to non-admin users"""
    column_groups = (
        COLUMNS,
        COMMIT_COUNT_OPTIMIZATION_COLUMNS,
        CLONING_ERROR_EXTRA_COLUMNS,
        SKIPPED_FILES_EXTRA_COLUMNS,
    )
    return {
        column_name: "requires admin"
        for columns in column_groups
        for column_name, _, _, requires_admin, _ in columns
        if requires_admin
    }


SKIPPED_FILE_REASON_COLUMNS: list[tuple[str, str, bool, str]] = [
    (
        "repository.name",
        "Sourcegraph repository name containing the skipped file",
        False,
        "string",
    ),
    (
        "rev",
        "Indexed revision parsed from Sourcegraph's skippedIndexed.query",
        False,
        "string",
    ),
    (
        "reason",
        "NOT-INDEXED reason parsed from the indexed placeholder content",
        False,
        "string",
    ),
    (
        "file.extension",
        "File extension derived from file.path",
        False,
        "string",
    ),
    (
        "file.byteSize",
        "Sourcegraph-reported file byte size",
        False,
        "integer",
    ),
    (
        "skippedIndexed.count",
        "Count Sourcegraph reported for this repo/ref before running the details search",
        False,
        "integer",
    ),
    (
        "file.path",
        "Path of the skipped file within the repository",
        False,
        "string",
    ),
    (
        "file_url",
        "Sourcegraph blob URL for the skipped file at the indexed ref",
        False,
        "string",
    ),
]


# --- Statistics ---------------------------------------------------------------

# --statistics buckets repo/content/index sizes and size ratios during listing

# (label, lo_inclusive_mb, hi_exclusive_mb_or_None) — used for the repo and
# indexed-content size distributions, which span many orders of magnitude
SIZE_BUCKETS_MB: list[tuple[str, int, int | None]] = [
    ("0-1 MB", 0, 1),
    ("1 MB - 1 GB", 1, 1024),
    ("1-10 GB", 1024, 10 * 1024),
    ("10-100 GB", 10 * 1024, 100 * 1024),
    (">100 GB", 100 * 1024, None),
]

# Search indexes are typically much smaller than the source they index, so a
# narrower set of buckets is more useful here than reusing SIZE_BUCKETS_MB
INDEX_SIZE_BUCKETS_MB: list[tuple[str, int, int | None]] = [
    ("0-1 MB", 0, 1),
    ("1-10 MB", 1, 10),
    ("10-100 MB", 10, 100),
    (">100 MB", 100, None),
]

# Used for both content/mirror and index/content ratio distributions. The
# >100% bucket isn't a logic bug — content can exceed the bare clone size
# when the bare clone is heavily packed, and the index can briefly exceed
# the content size on small repos due to per-shard overhead
PERCENT_BUCKETS: list[tuple[str, float, float | None]] = [
    ("0-10%", 0, 10),
    ("10-25%", 10, 25),
    ("25-50%", 25, 50),
    ("50-75%", 50, 75),
    ("75-100%", 75, 100),
    ("100-150%", 100, 150),
    (">150%", 150, None),
]


def bucket_label(
    value: float,
    buckets: list[tuple[str, float, float | None]] | list[tuple[str, int, int | None]],
) -> str | None:
    """Return the label of the first bucket that contains `value`, or None"""
    for label, lo, hi in buckets:
        if value >= lo and (hi is None or value < hi):
            return label
    return None


class StatsCollector:
    """Accumulate per-repo size and ratio counts for --statistics"""

    def __init__(self) -> None:
        self.mirror_buckets: collections.Counter[str] = collections.Counter()
        self.content_buckets: collections.Counter[str] = collections.Counter()
        self.index_buckets: collections.Counter[str] = collections.Counter()
        self.content_vs_mirror_buckets: collections.Counter[str] = collections.Counter()
        self.index_vs_content_buckets: collections.Counter[str] = collections.Counter()
        self.cloned_count = 0
        self.cloned_total_mb = 0
        self.content_count = 0
        self.content_total_mb = 0
        self.indexed_count = 0
        self.indexed_total_mb = 0

    def add(self, repo: dict[str, Any]) -> None:
        """Update every counter from a single repo's size fields"""
        mirror_mb = get_path_mb(repo, "mirrorInfo.byteSize")
        content_mb = get_path_mb(repo, "textSearchIndex.status.contentByteSize")
        index_mb = get_path_mb(repo, "textSearchIndex.status.indexByteSize")

        # Restrict the mirror size distribution to repos which actually have
        # a clone on disk; reporting `not_cloned` repos under "0-1 MB" would
        # blur "tiny repo" with "missing clone" in the same bucket
        if mirror_mb is not None and derive_mirror_status(repo) == "cloned":
            self.cloned_count += 1
            self.cloned_total_mb += mirror_mb
            label = bucket_label(mirror_mb, SIZE_BUCKETS_MB)
            if label is not None:
                self.mirror_buckets[label] += 1

        # Both content and index sizes only exist on repos that have a search
        # index, so presence of the underlying field is the right gate
        if content_mb is not None:
            self.content_count += 1
            self.content_total_mb += content_mb
            label = bucket_label(content_mb, SIZE_BUCKETS_MB)
            if label is not None:
                self.content_buckets[label] += 1

        if index_mb is not None:
            self.indexed_count += 1
            self.indexed_total_mb += index_mb
            label = bucket_label(index_mb, INDEX_SIZE_BUCKETS_MB)
            if label is not None:
                self.index_buckets[label] += 1

        # Skip the ratio buckets when either operand is missing or the
        # denominator floored to 0 MB (the result would be undefined / inf)
        if content_mb is not None and mirror_mb is not None and mirror_mb > 0:
            pct = (content_mb / mirror_mb) * 100
            label = bucket_label(pct, PERCENT_BUCKETS)
            if label is not None:
                self.content_vs_mirror_buckets[label] += 1

        if index_mb is not None and content_mb is not None and content_mb > 0:
            pct = (index_mb / content_mb) * 100
            label = bucket_label(pct, PERCENT_BUCKETS)
            if label is not None:
                self.index_vs_content_buckets[label] += 1


# Per-stat output metadata: suffix, description, buckets, counter, summary rows
STATS_FILES: list[
    tuple[
        str,
        str,
        list[tuple[str, int, int | None]] | list[tuple[str, float, float | None]],
        str,
        Callable[[StatsCollector], list[tuple[str, Any]]],
    ]
] = [
    (
        "mirror-byte-size",
        "Distribution of cloned repos by `mirrorInfo.byteSize` (MB)",
        SIZE_BUCKETS_MB,
        "mirror_buckets",
        lambda s: [
            ("TOTAL_CLONED_REPOS", s.cloned_count),
            ("TOTAL_CLONED_SIZE_MB", s.cloned_total_mb),
        ],
    ),
    (
        "content-byte-size",
        "Distribution of indexed repos by `textSearchIndex.status.contentByteSize` (MB)",
        SIZE_BUCKETS_MB,
        "content_buckets",
        lambda s: [
            ("TOTAL_INDEXED_REPOS", s.content_count),
            ("TOTAL_CONTENT_SIZE_MB", s.content_total_mb),
        ],
    ),
    (
        "index-byte-size",
        "Distribution of indexed repos by `textSearchIndex.status.indexByteSize` (MB)",
        INDEX_SIZE_BUCKETS_MB,
        "index_buckets",
        lambda s: [
            ("TOTAL_INDEXED_REPOS", s.indexed_count),
            ("TOTAL_INDEX_SIZE_MB", s.indexed_total_mb),
        ],
    ),
    (
        "content-vs-mirror-pct",
        "Distribution of `contentByteSize / mirrorInfo.byteSize` (as a percentage)",
        PERCENT_BUCKETS,
        "content_vs_mirror_buckets",
        lambda s: [("TOTAL_REPOS", sum(s.content_vs_mirror_buckets.values()))],
    ),
    (
        "index-vs-content-pct",
        "Distribution of `indexByteSize / contentByteSize` (as a percentage)",
        PERCENT_BUCKETS,
        "index_vs_content_buckets",
        lambda s: [("TOTAL_REPOS", sum(s.index_vs_content_buckets.values()))],
    ),
]


def write_stats(prefix: str, stats: StatsCollector) -> list[Path]:
    """Write one bucket/count CSV per stat and return the paths written"""
    written: list[Path] = []
    for suffix, _desc, buckets, attr, summary_builder in STATS_FILES:
        path = Path(f"{prefix}-{DEFAULT_STATS_FILE_PREFIX}-{suffix}.csv")
        counter: collections.Counter[str] = getattr(stats, attr)
        with path.open("w", newline="") as out:
            writer = csv.writer(out)
            writer.writerow(["bucket", "count"])
            for label, _lo, _hi in buckets:
                writer.writerow([label, counter.get(label, 0)])
            for metric, value in summary_builder(stats):
                writer.writerow([metric, value])
        written.append(path)
    return written


# --- CSV schema generation ----------------------------------------------------

# CSV_SCHEMA.md is generated from the same tuples that define CSV output


def format_columns_list(columns: list[tuple[str, str, bool, str]]) -> str:
    """Render column metadata as a Markdown table"""
    rows = [
        table_row("Column", "Type", "Requires admin", "Description"),
        table_row("---", "---", "---", "---"),
    ]
    for name, desc, requires_admin, value_type in columns:
        admin_cell = "true" if requires_admin else ""
        # Defensive: escape pipes so a description never breaks the table
        desc_cell = desc.replace("|", "\\|")
        rows.append(table_row(f"`{name}`", value_type, admin_cell, desc_cell))
    return "\n".join(rows)


def table_row(*cells: str) -> str:
    """Format a Markdown table row, using `| |` for empty cells"""
    return "|" + "|".join(f" {c} " if c else " " for c in cells) + "|"


def name_desc(
    columns: list[tuple[str, Callable[[dict[str, Any]], Any], str, bool, str]],
) -> list[tuple[str, str, bool, str]]:
    """Drop extractor functions from column metadata"""
    return [
        (name, desc, requires_admin, value_type)
        for name, _, desc, requires_admin, value_type in columns
    ]


def format_stats_files_list() -> str:
    """Render STATS_FILES as a Markdown table for CSV_SCHEMA.md"""
    rows = [
        table_row("File suffix", "Buckets", "Description"),
        table_row("---", "---", "---"),
    ]
    for suffix, desc, buckets, _attr, _summary in STATS_FILES:
        bucket_cell = ", ".join(label for label, _, _ in buckets)
        # Defensive: escape pipes so a description never breaks the table
        rows.append(
            table_row(
                f"`{DEFAULT_STATS_FILE_PREFIX}-{suffix}.csv`",
                bucket_cell,
                desc.replace("|", "\\|"),
            ),
        )
    return "\n".join(rows)


def write_csv_schema(path: Path) -> None:
    """Write CSV_SCHEMA.md from the in-script column tables"""
    main_list = format_columns_list(name_desc(COLUMNS))
    cloning_list = format_columns_list(name_desc(CLONING_ERROR_EXTRA_COLUMNS))
    skipped_list = format_columns_list(name_desc(SKIPPED_FILES_EXTRA_COLUMNS))
    skipped_reason_list = format_columns_list(SKIPPED_FILE_REASON_COLUMNS)
    commit_count_list = format_columns_list(COMMIT_COUNT_COLUMNS)
    run_search_list = format_columns_list(RUN_SEARCH_COLUMNS)
    stats_files_list = format_stats_files_list()

    content = f"""# `list-repos.py` CSV column reference

- This file is generated by `python3 list-repos.py --write-csv-schema`
- It documents every column in each of its CSV output files
- Columns where `Requires admin` is `true` are from GraphQL fields
which require a site admin or the `REPO_MANAGEMENT#READ` permission
  - Without either authorization, these columns contain `requires admin`
- Every other column is populated for any authenticated user with read access
to the repository
- If the instance's GraphQL schema does not contain a field, its CSV column
contains `field not in v<Sourcegraph version>`

## Output files

The script prefixes output file names with the sanitized Sourcegraph endpoint
(e.g. `sourcegraph.example.com-repos.csv`),
so the script can run against multiple instances without overwriting files

| File | Written when | Columns |
| --- | --- | --- |
| `<prefix>-{DEFAULT_OUTPUT_FILE}` | always | main columns |
| `<prefix>-{DEFAULT_CLONING_ERRORS_FILE}` | at least one repo has a cloning error | main columns + cloning-error extras |
| `<prefix>-{DEFAULT_INDEXING_ERRORS_FILE}` | at least one repo is cloned but is missing a search index | main columns |
| `<prefix>-{DEFAULT_SKIPPED_FILES_FILE}` | `--skipped-files` is set and the last index excluded some files | main columns + skipped-files extras |
| `<prefix>-{DEFAULT_SKIPPED_FILE_REASONS_FILE}` | `--skipped-files-reason` is set without `REPO[@REV]` | skipped-file reason columns |
| `<prefix>-{DEFAULT_STATS_FILE_PREFIX}-*.csv` | `--statistics` is set | `bucket,count` (see Statistics section) |

The optional `--count-commits` and `--run-search` flags append extra
columns to the repo-listing CSVs above, excluding the `--statistics`
files and the skipped-file reason detail CSV, in this order: main
columns → per-CSV extras → commit-count columns → run-search columns

## Main columns

These are written to every repo-listing CSV file

{main_list}

## Cloning-error extras

Appended to `<prefix>-{DEFAULT_CLONING_ERRORS_FILE}`

{cloning_list}

## Skipped-files extras

Appended to `<prefix>-{DEFAULT_SKIPPED_FILES_FILE}`

{skipped_list}

## Skipped-file reason columns

Written to `<prefix>-{DEFAULT_SKIPPED_FILE_REASONS_FILE}` when
`--skipped-files-reason` is used without `REPO[@REV]`

{skipped_reason_list}

## `--count-commits` columns

Appended to CSV files when `--count-commits` is used

{commit_count_list}

## `--run-search` columns

Appended to CSV files when `--run-search PATTERN` is used

{run_search_list}

## `--statistics` files

- Written when `--statistics` is used
- One CSV file per dimension
- Each file has two columns listing every bucket in declaration
order, followed by per-stat summary rows (totals) appended below the
bucket rows
- Counts come from the same listing pass that produces the
main CSV, so enabling `--statistics` adds no extra GraphQL requests

{stats_files_list}

"""
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


# --- HTTP / GraphQL plumbing --------------------------------------------------


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


@dataclass(frozen=True)
class GraphQLFieldCountViolation:
    """Sourcegraph GraphQL field-count limit details from an error response"""

    actual: int
    limit: int


def graphql_extension_int(value: object) -> int | None:
    """Return an int from a GraphQL extension value when it is numeric"""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def parse_field_count_violation(
    error: HTTPRequestError,
) -> GraphQLFieldCountViolation | None:
    """Extract Sourcegraph's GraphQL field-count violation from an HTTP 400"""
    try:
        payload = json.loads(error.body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    errors = payload.get("errors")
    if not isinstance(errors, list):
        return None
    for graph_error in errors:
        if not isinstance(graph_error, dict):
            continue
        extensions = graph_error.get("extensions")
        if not isinstance(extensions, dict):
            continue
        if extensions.get("code") != "ErrQueryComplexityLimitExceeded":
            continue
        if extensions.get("type") != "field count":
            continue
        actual = graphql_extension_int(extensions.get("actual"))
        limit = graphql_extension_int(extensions.get("limit"))
        if actual is None or limit is None:
            continue
        if actual <= 0 or limit <= 0:
            continue
        return GraphQLFieldCountViolation(actual=actual, limit=limit)
    return None


def retry_page_size_after_field_count_violation(
    page_size: int,
    violation: GraphQLFieldCountViolation,
) -> int:
    """Shrink page size from Sourcegraph's reported actual/limit ratio"""
    next_page_size = (
        page_size
        * violation.limit
        * GRAPHQL_FIELD_COUNT_RETRY_HEADROOM_PERCENT
        // violation.actual
        // 100
    )
    if next_page_size >= page_size:
        next_page_size = page_size - 1
    return max(1, next_page_size)


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
    conn = open_connection(parsed, timeout=timeout)
    try:
        conn.request("POST", path, body=body, headers=headers)
        resp = conn.getresponse()
        response_body = resp.read()
        if resp.status >= http.client.BAD_REQUEST:
            raise HTTPRequestError(
                resp.status,
                resp.reason,
                url,
                resp.getheaders(),
                response_body,
            )
        return json.loads(response_body)
    finally:
        conn.close()


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
    messages = [graphql_error_message(error) for error in errors]
    return "; ".join(messages)


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
        "User-Agent": "list-repos/0.0.1",
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
        # log the errors and keep going; only abort if no data was returned.
        if response.get("data"):
            logger.warning(
                "GraphQL returned %d partial error(s): %s",
                len(errors) if isinstance(errors, list) else 1,
                json.dumps(errors, indent=2),
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
) -> tuple[str, bool, bool]:
    """Return username, site-admin status, and protected-field read access"""
    data = graphql_request(
        endpoint,
        token,
        CURRENT_USER_QUERY,
        {},
        max_retries=max_retries,
        request_description="Current user query",
    )
    user: dict[str, Any] = data["currentUser"] or {}
    is_site_admin = bool(user.get("siteAdmin"))
    permissions: dict[str, Any] = user.get("permissions") or {}
    permission_nodes: list[dict[str, Any]] = permissions.get("nodes") or []
    can_read_protected_fields = is_site_admin or any(
        permission.get("namespace") == REPOSITORY_MANAGEMENT_PERMISSION_NAMESPACE
        and permission.get("action") == READ_PERMISSION_ACTION
        for permission in permission_nodes
    )
    return str(user["username"]), is_site_admin, can_read_protected_fields


def fetch_sourcegraph_version(
    endpoint: str,
    token: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> str:
    """Return site.productVersion from the target Sourcegraph instance"""
    data = graphql_request(
        endpoint,
        token,
        SOURCEGRAPH_VERSION_QUERY,
        {},
        max_retries=max_retries,
        request_description="Sourcegraph version query",
    )
    site: dict[str, Any] = data.get("site") or {}
    version = site.get("productVersion")
    if not isinstance(version, str) or not version:
        msg = "Sourcegraph version query did not return site.productVersion"
        raise GraphQLError(msg)
    return version


def fetch_graphql_schema(
    endpoint: str,
    token: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> GraphQLSchema:
    """Fetch and index the target instance's GraphQL schema"""
    data = graphql_request(
        endpoint,
        token,
        GRAPHQL_SCHEMA_QUERY,
        {},
        max_retries=max_retries,
        request_description="GraphQL schema query",
    )
    return parse_graphql_schema(data)


def fetch_single_repo(
    endpoint: str,
    token: str,
    repo_name: str,
    *,
    can_read_protected_fields: bool,
    schema: GraphQLSchema,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> dict[str, Any]:
    """Fetch one repo node in listing-query shape, respecting protected fields"""
    data = graphql_request(
        endpoint,
        token,
        build_single_repo_query(
            schema,
            can_read_protected_fields=can_read_protected_fields,
        ),
        {"name": repo_name},
        max_retries=max_retries,
        request_description=f"Repository metadata for {repo_name}",
    )
    repo = data.get("repository")
    if repo is None:
        die(f"repository {repo_name!r} not found on {endpoint}")
    return cast("dict[str, Any]", repo)


def trigger_reclone(
    endpoint: str,
    token: str,
    repo_id: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> bool:
    """Send recloneRepository mutation. Returns True on success, False on GraphQL error"""
    try:
        graphql_request(
            endpoint,
            token,
            RECLONE_MUTATION,
            {"repo": repo_id},
            max_retries=max_retries,
            request_description=f"Reclone repository {repo_id}",
        )
    except (GraphQLError, HTTPRequestError) as exc:
        logger.warning("recloneRepository failed for %s: %s", repo_id, exc)
        return False
    return True


def trigger_reindex(
    endpoint: str,
    token: str,
    repo_id: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> bool:
    """Send reindexRepository mutation. Returns True on success, False on GraphQL error"""
    try:
        graphql_request(
            endpoint,
            token,
            REINDEX_MUTATION,
            {"repository": repo_id},
            max_retries=max_retries,
            request_description=f"Reindex repository {repo_id}",
        )
    except (GraphQLError, HTTPRequestError) as exc:
        logger.warning("reindexRepository failed for %s: %s", repo_id, exc)
        return False
    return True


def sanitize_for_filename(text: str) -> str:
    """Replace non-[A-Za-z0-9._-] chars with '_' so the string is filesystem-safe"""
    return re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("_")


def sanitize_endpoint_for_filename(endpoint: str) -> str:
    """Sanitize an endpoint URL for use in filenames, dropping the http(s) scheme"""
    return sanitize_for_filename(re.sub(r"^https?://", "", endpoint))


_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://")


def _split_name_rev(repo_rev: str) -> tuple[str, str | None]:
    """Split repo[@rev], URL, or scp-style repo text into name and rev"""
    rev: str | None = None
    if _SCHEME_RE.match(repo_rev):
        u = urlsplit(repo_rev)
        # u.hostname is lower-cased and userinfo-stripped
        name = (u.hostname or "") + u.path
        if "@" in name:
            before, after = name.rsplit("@", 1)
            name, rev = before, after
        return name, rev

    name = repo_rev
    if "@" in name:
        before, after = name.rsplit("@", 1)
        slash = after.find("/")
        colon = after.find(":")
        if colon != -1 and (slash == -1 or colon < slash):
            # scp-style 'user@host:path' — drop the 'user@'
            name = after
        else:
            name, rev = before, after
    return name, rev


def parse_repo_rev(repo_rev: str) -> str:
    """Extract the revision from 'repo[$]@rev'. Returns 'HEAD' if no '@rev' is present"""
    _, rev = _split_name_rev(repo_rev)
    return rev if rev is not None else "HEAD"


def parse_repo_name(repo_rev: str) -> str:
    """Extract a canonical Sourcegraph repo name from repo/URL/SSH-ish input"""
    name, _ = _split_name_rev(repo_rev)
    name = name.removeprefix("^").removesuffix("$")
    return name.rstrip("/")


def verify_repo_rev(
    endpoint: str,
    token: str,
    repo_rev: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> tuple[str, str]:
    """Require repo/rev to resolve to an index; return output rev and skip query"""
    name = parse_repo_name(repo_rev)
    rev = parse_repo_rev(repo_rev)
    data = graphql_request(
        endpoint,
        token,
        REPO_REV_VALIDATION_QUERY,
        {"name": name, "rev": rev},
        max_retries=max_retries,
        request_description=f"Revision check for {name}@{rev}",
    )
    repository: dict[str, Any] | None = data.get("repository")
    if repository is None:
        die(f"repository {name!r} not found on {endpoint}")
    commit: dict[str, Any] | None = repository.get("commit")
    if commit is None:
        die(f"revision {rev!r} not found in repository {name!r}")

    text_index: dict[str, Any] | None = repository.get("textSearchIndex")
    refs: list[dict[str, Any]] = (text_index or {}).get("refs") or []
    target_oid = commit.get("oid")
    indexed_oids: set[str] = set()
    indexed_names: list[str] = []
    default_branch: dict[str, Any] = repository.get("defaultBranch") or {}
    default_branch_name = str(default_branch.get("displayName") or "")
    selected_skipped_query = ""
    fallback_skipped_query = ""
    for ref in refs:
        if not ref.get("indexed"):
            continue
        indexed_commit: dict[str, Any] = ref.get("indexedCommit") or {}
        oid = indexed_commit.get("oid")
        if oid:
            indexed_oids.add(str(oid))
        ref_node: dict[str, Any] = ref.get("ref") or {}
        ref_name = str(ref_node.get("displayName") or "?")
        indexed_names.append(ref_name)
        if oid != target_oid:
            continue
        skipped: dict[str, Any] = ref.get("skippedIndexed") or {}
        skipped_count = int(skipped.get("count") or 0)
        skipped_query = str(skipped.get("query") or "")
        if skipped_count <= 0 or not skipped_query:
            continue
        if not fallback_skipped_query:
            fallback_skipped_query = skipped_query
        if ref_name == rev or (rev == "HEAD" and ref_name == default_branch_name):
            selected_skipped_query = skipped_query
    if target_oid not in indexed_oids:
        if indexed_names:
            indexed_summary = "\n".join(f"  - {n}" for n in indexed_names)
        else:
            indexed_summary = "  (none)"
        die(
            f"revision {rev!r} (commit {target_oid}) is not currently indexed "
            f"in repository {name!r}.\nIndexed refs:\n{indexed_summary}",
        )

    # When the user didn't specify a rev (or explicitly used "HEAD"), substitute
    # the actual default branch name so filenames and URLs read naturally
    if rev == "HEAD":
        return (
            default_branch_name or "HEAD",
            selected_skipped_query or fallback_skipped_query,
        )
    return rev, selected_skipped_query or fallback_skipped_query


def file_url(endpoint: str, repo_name: str, rev: str, file_path: str) -> str:
    """Build a clickable Sourcegraph URL pointing at a specific file at a revision"""
    base = endpoint.rstrip("/")
    rev_segment = f"@{rev}" if rev and rev != "HEAD" else ""
    return f"{base}/{repo_name}{rev_segment}/-/blob/{file_path}"


def skipped_file_reason(match: dict[str, Any]) -> str:
    """Extract the NOT-INDEXED reason from a skipped-file search match"""
    chunks: list[dict[str, Any]] = match.get("chunkMatches") or []
    for chunk in chunks:
        reason_match = re.search(
            r"NOT-INDEXED:\s*(.+)",
            str(chunk.get("content") or ""),
        )
        if reason_match:
            return reason_match.group(1).strip()
    return ""


def skipped_file_query_revision(query: str, fallback: str) -> str:
    """Return the @rev term from a skippedIndexed query, or fallback"""
    match = re.search(r"\br:\S+@([^\s]+)", query)
    if match:
        return match.group(1)
    return fallback


def skipped_file_reason_search_query(
    skipped_indexed_query: str,
    repo_name: str,
    revision: str,
) -> str:
    """Build a reason-search query from Sourcegraph's skippedIndexed.query"""
    if not skipped_indexed_query:
        repo_filter = f"^{re.escape(repo_name)}$"
        skipped_indexed_query = (
            f"r:{repo_filter}@{revision} type:file index:only "
            f"patternType:regexp ^NOT-INDEXED:"
        )
    terms = [
        term
        for term in skipped_indexed_query.split()
        if term != "select:file"
        and not term.startswith("count:")
        and not term.startswith("timeout:")
    ]
    terms.append("count:all")
    terms.append(SKIPPED_FILE_REASON_SEARCH_TIMEOUT_PARAMETER)
    return " ".join(terms)


def fetch_skipped_file_reason_query(
    endpoint: str,
    token: str,
    name: str,
    rev: str,
    skipped_indexed_query: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> SkippedFileReasonQueryResult:
    """Return NOT-INDEXED matches and search metadata for one indexed repo ref"""
    start = time.monotonic()
    search_query = skipped_file_reason_search_query(skipped_indexed_query, name, rev)
    data = graphql_request(
        endpoint,
        token,
        SKIPPED_FILES_REASON_QUERY,
        {"query": search_query},
        timeout=REQUEST_TIMEOUT_SECONDS_WITH_COMMIT_COUNT,
        max_retries=max_retries,
        request_description=f"Skipped files for {name}@{rev}",
    )
    elapsed = time.monotonic() - start
    results_block: dict[str, Any] = data.get("search", {}).get("results", {})
    raw_results: list[dict[str, Any] | None] = results_block.get("results") or []
    # Non-FileMatch results come back as empty objects; drop them
    matches = [result for result in raw_results if result and result.get("file")]
    raw_match_count = results_block.get("matchCount")
    match_count: int | None = (
        raw_match_count if isinstance(raw_match_count, int) else None
    )
    limit_hit = bool(results_block.get("limitHit"))
    alert: dict[str, Any] = results_block.get("alert") or {}
    alert_title_raw = alert.get("title")
    alert_description_raw = alert.get("description")
    alert_title: str | None = (
        alert_title_raw if isinstance(alert_title_raw, str) else None
    )
    alert_description: str | None = (
        alert_description_raw if isinstance(alert_description_raw, str) else None
    )
    alert_parts = [part for part in (alert_title, alert_description) if part]
    alert_suffix = f", alert={'; '.join(alert_parts)!r}" if alert_parts else ""
    match_count_value = "?" if match_count is None else str(match_count)
    logger.info(
        "Skipped-file reason search for %s@%s: matchCount=%s, fileMatches=%d, "
        "limitHit=%s%s [query took %.3fs]",
        name,
        rev,
        match_count_value,
        len(matches),
        limit_hit,
        alert_suffix,
        elapsed,
    )
    return SkippedFileReasonQueryResult(
        matches=matches,
        match_count=match_count,
        limit_hit=limit_hit,
        alert_title=alert_title,
        alert_description=alert_description,
    )


def write_skipped_files_reason(
    endpoint: str,
    token: str,
    repo_rev: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> None:
    """Fetch skipped-file matches for repo_rev and write the per-file and stats CSVs"""
    endpoint_sanitized = sanitize_endpoint_for_filename(endpoint)
    # Remove raw-input outputs before validation so failures cannot leave stale CSVs
    input_name_sanitized = sanitize_for_filename(parse_repo_name(repo_rev))
    input_rev_sanitized = sanitize_for_filename(parse_repo_rev(repo_rev))
    input_prefix = f"{endpoint_sanitized}-{input_name_sanitized}-{input_rev_sanitized}"
    Path(f"{input_prefix}-skipped-files.csv").unlink(missing_ok=True)
    Path(f"{input_prefix}-skipped-stats.csv").unlink(missing_ok=True)

    rev, skipped_indexed_query = verify_repo_rev(
        endpoint,
        token,
        repo_rev,
        max_retries=max_retries,
    )
    name = parse_repo_name(repo_rev)
    name_sanitized = sanitize_for_filename(name)
    rev_sanitized = sanitize_for_filename(rev)
    prefix = f"{endpoint_sanitized}-{name_sanitized}-{rev_sanitized}"
    files_path = Path(f"{prefix}-skipped-files.csv")
    stats_path = Path(f"{prefix}-skipped-stats.csv")
    # Also remove resolved-rev outputs when they differ from the raw input names
    if prefix != input_prefix:
        files_path.unlink(missing_ok=True)
        stats_path.unlink(missing_ok=True)

    # Keep each local CSV header beside the extractor that writes its value
    def chunk_matches_content(m: dict[str, Any]) -> str:
        chunks: list[dict[str, Any]] = m.get("chunkMatches") or []
        return "\n".join(str(c.get("content") or "") for c in chunks)

    def match_file_byte_size(m: dict[str, Any]) -> int | str:
        file_obj: dict[str, Any] = m.get("file") or {}
        bs = file_obj.get("byteSize")
        return int(bs) if bs is not None else ""

    def match_file_extension(m: dict[str, Any]) -> str:
        file_obj: dict[str, Any] = m.get("file") or {}
        return Path(str(file_obj.get("path") or "")).suffix.lstrip(".")

    def match_file_url(m: dict[str, Any]) -> str:
        repo_obj: dict[str, Any] = m.get("repository") or {}
        file_obj: dict[str, Any] = m.get("file") or {}
        return file_url(
            endpoint,
            str(repo_obj.get("name") or ""),
            rev,
            str(file_obj.get("path") or ""),
        )

    file_columns: list[tuple[str, Callable[[dict[str, Any]], Any]]] = [
        ("chunkMatches.content", chunk_matches_content),
        ("file.byteSize", match_file_byte_size),
        ("file.extension", match_file_extension),
        ("file_url", match_file_url),
    ]
    stats_columns: list[tuple[str, Callable[[tuple[str, int]], Any]]] = [
        ("reason", lambda r: r[0]),
        ("count", lambda r: r[1]),
    ]

    query_result = fetch_skipped_file_reason_query(
        endpoint,
        token,
        name,
        rev,
        skipped_indexed_query,
        max_retries=max_retries,
    )
    matches = query_result.matches

    reason_counts: collections.Counter[str] = collections.Counter()
    rows: list[list[Any]] = []
    for match in matches:
        rows.append([extract(match) for _, extract in file_columns])
        reason = skipped_file_reason(match)
        if reason:
            reason_counts[reason] += 1

    # Sort by chunkMatches.content so files with the same NOT-INDEXED reason
    # are grouped together; ties broken by byteSize, extension, then file_url
    # Coerce byteSize to int (treating missing values as -1) so an int/str
    # union can't blow up the comparator
    rows.sort(
        key=lambda r: (r[0], r[1] if isinstance(r[1], int) else -1, r[2], r[3]),
    )

    with files_path.open("w", newline="") as out:
        writer = csv.writer(out)
        writer.writerow([n for n, _ in file_columns])
        writer.writerows(rows)
    files_written = len(rows)

    with stats_path.open("w", newline="") as out:
        writer = csv.writer(out)
        writer.writerow([n for n, _ in stats_columns])
        for record in reason_counts.most_common():
            writer.writerow([extract(record) for _, extract in stats_columns])

    logger.info(
        "Wrote %d skipped-file match(es) to %s",
        files_written,
        files_path.name,
    )
    logger.info(
        "Wrote %d NOT-INDEXED reason categor(ies) to %s",
        len(reason_counts),
        stats_path.name,
    )


# --- Repo CSV pipeline --------------------------------------------------------


class LazyCSVWriter:
    """csv.writer wrapper that creates optional CSVs only when needed"""

    def __init__(self, path: Path, columns: list[str]) -> None:
        self.path = path
        self.columns = columns
        self.count = 0
        self._file: TextIO | None = None
        self._writer: Any = None

    def writerow(self, row: list[Any]) -> None:
        if self._writer is None:
            self._file = self.path.open("w", newline="")
            self._writer = csv.writer(self._file)
            self._writer.writerow(self.columns)
        self._writer.writerow(row)
        self.count += 1

    def __enter__(self) -> LazyCSVWriter:
        return self

    def __exit__(self, *_args: object) -> None:
        if self._file is not None:
            self._file.close()


@dataclass(frozen=True)
class RepositoryPage:
    """One repository listing page plus the page size Sourcegraph accepted"""

    connection: dict[str, Any]
    request_page_size: int


@dataclass(frozen=True)
class SkippedFileReasonQueryResult:
    """Search response metadata and FileMatch results for one skipped-ref query"""

    matches: list[dict[str, Any]]
    match_count: int | None
    limit_hit: bool
    alert_title: str | None
    alert_description: str | None


@dataclass(frozen=True)
class SkippedFileReasonSearchResult:
    """Skipped-file search outcome for one indexed repo ref"""

    repository_name: str
    ref_name: str
    skipped_count: int
    matches: list[dict[str, Any]]
    match_count: int | None
    limit_hit: bool
    alert_title: str | None
    alert_description: str | None
    error: str | None


def repository_page_request_size(
    current_page_size: int,
    max_repos: int | None,
    total_fetched: int,
) -> int | None:
    """Return the next listing request size, or None when enough repos are fetched"""
    if max_repos is None:
        return current_page_size
    remaining = max_repos - total_fetched
    if remaining <= 0:
        return None
    return min(current_page_size, remaining)


def fetch_repository_page(
    endpoint: str,
    token: str,
    cursor: str | None,
    request_page_size: int,
    *,
    can_read_protected_fields: bool,
    schema: GraphQLSchema,
    max_retries: int,
) -> RepositoryPage:
    """Fetch one repository listing page, reducing page size on field-count errors"""
    while True:
        start = time.monotonic()
        try:
            data = graphql_request(
                endpoint,
                token,
                build_repository_listing_query(
                    schema,
                    can_read_protected_fields=can_read_protected_fields,
                ),
                {
                    "first": request_page_size,
                    "after": cursor,
                },
                max_retries=max_retries,
                request_description=(
                    f"Repository listing page (first={request_page_size})"
                ),
            )
            elapsed = time.monotonic() - start
            cursor_label = "start" if cursor is None else "cursor"
            logger.info(
                "Repository listing page query finished: first=%d, after=%s "
                "[query took %.3fs]",
                request_page_size,
                cursor_label,
                elapsed,
            )
            return RepositoryPage(data["repositories"], request_page_size)
        except HTTPRequestError as error:
            violation = parse_field_count_violation(error)
            if violation is None or request_page_size <= 1:
                raise
            next_page_size = retry_page_size_after_field_count_violation(
                request_page_size,
                violation,
            )
            logger.warning(
                "Sourcegraph rejected listing page size %d: GraphQL "
                "field count %d exceeds limit %d; retrying with page size %d",
                request_page_size,
                violation.actual,
                violation.limit,
                next_page_size,
            )
            request_page_size = next_page_size


def fetch_repos(
    endpoint: str,
    token: str,
    max_repos: int | None = None,
    *,
    page_size: int = PAGE_SIZE,
    scope_repo: str | None = None,
    can_read_protected_fields: bool,
    schema: GraphQLSchema,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> Iterator[tuple[int, int, dict[str, Any]]]:
    """Yield (index, target, repo) tuples for a scoped repo or paged repo list"""
    if scope_repo is not None:
        repo = fetch_single_repo(
            endpoint,
            token,
            scope_repo,
            can_read_protected_fields=can_read_protected_fields,
            schema=schema,
            max_retries=max_retries,
        )
        logger.info("Scope: single repository %s", scope_repo)
        yield 1, 1, repo
        logger.info("Fetched 1/1 repositories...")
        return
    total_fetched = 0
    first_page = True
    current_page_size = page_size
    logger.info(
        "GraphQL listing page size: %d (will retry smaller if Sourcegraph "
        "reports a field-count limit)",
        current_page_size,
    )
    request_page_size = repository_page_request_size(
        current_page_size,
        max_repos,
        total_fetched,
    )
    if request_page_size is None:
        return
    page = fetch_repository_page(
        endpoint,
        token,
        None,
        request_page_size,
        can_read_protected_fields=can_read_protected_fields,
        schema=schema,
        max_retries=max_retries,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as page_executor:
        while True:
            current_page_size = min(current_page_size, page.request_page_size)
            connection = page.connection
            total_count = connection["totalCount"]
            target = (
                min(max_repos, total_count) if max_repos is not None else total_count
            )
            if first_page:
                logger.info(
                    "Fetching %d of %d total repositories...",
                    target,
                    total_count,
                )
                first_page = False

            nodes: list[dict[str, Any]] = connection["nodes"]
            total_after_page = total_fetched + len(nodes)
            page_info: dict[str, Any] = connection["pageInfo"]
            next_page = None
            if page_info["hasNextPage"]:
                next_request_page_size = repository_page_request_size(
                    current_page_size,
                    max_repos,
                    total_after_page,
                )
                if next_request_page_size is not None:
                    next_page = page_executor.submit(
                        fetch_repository_page,
                        endpoint,
                        token,
                        page_info["endCursor"],
                        next_request_page_size,
                        can_read_protected_fields=can_read_protected_fields,
                        schema=schema,
                        max_retries=max_retries,
                    )

            for repo in nodes:
                total_fetched += 1
                yield total_fetched, target, repo

            logger.info("Fetched %d/%d repositories...", total_fetched, target)

            if next_page is None:
                break
            page = next_page.result()


def extract_csv_values(
    repo: dict[str, Any],
    columns: list[tuple[str, Callable[[dict[str, Any]], Any], str, bool, str]],
    unavailable_values: dict[str, str],
) -> list[Any]:
    """Extract supported columns and substitute schema-version markers"""
    return [
        (
            unavailable_values[column_name]
            if column_name in unavailable_values
            else extract(repo)
        )
        for column_name, extract, _, _, _ in columns
    ]


def build_row(
    repo: dict[str, Any],
    endpoint: str,
    unavailable_values: dict[str, str],
) -> list[Any]:
    """Build a base CSV row and absolutize the repo URL"""
    base = endpoint.rstrip("/")
    row = extract_csv_values(repo, COLUMNS, unavailable_values)
    if "url" not in unavailable_values and row[URL_COLUMN_INDEX]:
        row[URL_COLUMN_INDEX] = base + row[URL_COLUMN_INDEX]
    return row


def append_commit_count(
    row: list[Any],
    commit_count: int | None,
    all_refs_count: int | None,
    elapsed_seconds: float | None,
    optimization_values: list[Any] | None = None,
    *,
    count_commits: bool,
) -> list[Any]:
    """Append optional commit-count fields in COMMIT_COUNT_COLUMNS order"""
    if not count_commits:
        return row
    elapsed_cell: str | None = (
        f"{elapsed_seconds:.3f}" if elapsed_seconds is not None else None
    )
    extras = (
        optimization_values
        if optimization_values is not None
        else [None] * len(COMMIT_COUNT_OPTIMIZATION_COLUMNS)
    )
    return [*row, commit_count, all_refs_count, elapsed_cell, *extras]


def append_run_search(
    row: list[Any],
    match_count: int | None,
    elapsed_seconds: float | None,
    limit_hit: bool,
    alert_title: str | None,
    *,
    run_search: bool,
) -> list[Any]:
    """Append optional run-search fields in RUN_SEARCH_COLUMNS order"""
    if not run_search:
        return row
    elapsed_cell: str | None = (
        f"{elapsed_seconds:.3f}" if elapsed_seconds is not None else None
    )
    return [*row, match_count, elapsed_cell, limit_hit, alert_title]


def csv_columns_for(
    base_columns: list[str],
    *,
    count_commits: bool,
    run_search: bool = False,
) -> list[str]:
    """Return base columns plus enabled optional column blocks"""
    cols = list(base_columns)
    if count_commits:
        cols.extend(name for name, _, _, _ in COMMIT_COUNT_COLUMNS)
    if run_search:
        cols.extend(name for name, _, _, _ in RUN_SEARCH_COLUMNS)
    return cols


def collect_skipped_file_reason_search_results(
    endpoint: str,
    token: str,
    repo: dict[str, Any],
    max_retries: int,
) -> list[SkippedFileReasonSearchResult]:
    """Run skipped-file searches for every skipped indexed ref in one repo"""
    repo_name = str(repo.get("name") or "")
    results: list[SkippedFileReasonSearchResult] = []
    for (
        display_ref_name,
        skipped_count,
        skipped_indexed_query,
    ) in refs_with_skipped_file_queries(
        repo,
    ):
        revision = skipped_file_query_revision(skipped_indexed_query, display_ref_name)
        try:
            query_result = fetch_skipped_file_reason_query(
                endpoint,
                token,
                repo_name,
                revision,
                skipped_indexed_query,
                max_retries=max_retries,
            )
        except (GraphQLError, HTTPRequestError, OSError) as error:
            results.append(
                SkippedFileReasonSearchResult(
                    repository_name=repo_name,
                    ref_name=revision,
                    skipped_count=skipped_count,
                    matches=[],
                    match_count=None,
                    limit_hit=False,
                    alert_title=None,
                    alert_description=None,
                    error=str(error),
                ),
            )
            continue
        results.append(
            SkippedFileReasonSearchResult(
                repository_name=repo_name,
                ref_name=revision,
                skipped_count=skipped_count,
                matches=query_result.matches,
                match_count=query_result.match_count,
                limit_hit=query_result.limit_hit,
                alert_title=query_result.alert_title,
                alert_description=query_result.alert_description,
                error=None,
            ),
        )
    return results


def write_skipped_file_reason_rows(
    writer: LazyCSVWriter,
    endpoint: str,
    search_results: list[SkippedFileReasonSearchResult],
) -> None:
    """Append skipped-file detail rows from per-ref search results"""
    for search_result in search_results:
        if search_result.error is not None:
            logger.warning(
                "Skipped-file reason search failed for %s@%s: %s",
                search_result.repository_name,
                search_result.ref_name,
                search_result.error,
            )
            continue
        alert_parts = [
            part
            for part in (search_result.alert_title, search_result.alert_description)
            if part
        ]
        if search_result.limit_hit:
            logger.warning(
                "Skipped-file reason search hit a result limit for %s@%s: "
                "matchCount=%s, fileMatches=%d, skippedIndexed.count=%d",
                search_result.repository_name,
                search_result.ref_name,
                search_result.match_count,
                len(search_result.matches),
                search_result.skipped_count,
            )
        if alert_parts:
            logger.warning(
                "Skipped-file reason search returned alert for %s@%s: %s",
                search_result.repository_name,
                search_result.ref_name,
                "; ".join(alert_parts),
            )
        match_count_mismatch = (
            search_result.match_count is not None
            and search_result.match_count != search_result.skipped_count
        )
        if (
            len(search_result.matches) != search_result.skipped_count
            or match_count_mismatch
        ):
            logger.warning(
                "Skipped-file reason search returned %d file match(es) "
                "(matchCount=%s, limitHit=%s) for %s@%s; "
                "textSearchIndex.refs.skippedIndexed.count reported %d",
                len(search_result.matches),
                search_result.match_count,
                search_result.limit_hit,
                search_result.repository_name,
                search_result.ref_name,
                search_result.skipped_count,
            )
        for match in search_result.matches:
            file_obj: dict[str, Any] = match.get("file") or {}
            file_path = str(file_obj.get("path") or "")
            byte_size = file_obj.get("byteSize")
            file_extension = Path(file_path).suffix.lstrip(".")
            writer.writerow(
                [
                    search_result.repository_name,
                    search_result.ref_name,
                    skipped_file_reason(match),
                    file_extension,
                    int(byte_size) if byte_size is not None else "",
                    search_result.skipped_count,
                    file_path,
                    file_url(
                        endpoint,
                        search_result.repository_name,
                        search_result.ref_name,
                        file_path,
                    ),
                ],
            )


@dataclass(frozen=True)
class RepoProcessingResult:
    """Repo row plus optional per-repo query results"""

    index: int
    target: int
    repo: dict[str, Any]
    row: list[Any]
    commit_count: int | None
    all_refs_count: int | None
    commit_elapsed_seconds: float | None
    optimization_values: list[Any] | None
    search_match_count: int | None
    search_elapsed_seconds: float | None
    search_limit_hit: bool
    search_alert_title: str | None
    skipped_file_reason_search_results: list[SkippedFileReasonSearchResult]


def collect_repo_processing_result(
    endpoint: str,
    token: str,
    index: int,
    target: int,
    repo: dict[str, Any],
    *,
    count_commits: bool,
    count_commits_rev: str,
    run_search_pattern: str | None,
    skipped_file_reasons: bool,
    schema: GraphQLSchema,
    unavailable_values: dict[str, str],
    max_retries: int,
) -> RepoProcessingResult:
    """Build the row and run optional per-repo network queries"""
    row = build_row(repo, endpoint, unavailable_values)
    commit_count: int | None = None
    all_refs_count: int | None = None
    commit_elapsed_seconds: float | None = None
    optimization_values: list[Any] | None = None
    search_match_count: int | None = None
    search_elapsed_seconds: float | None = None
    search_limit_hit = False
    search_alert_title: str | None = None
    skipped_file_reason_search_results: list[SkippedFileReasonSearchResult] = []
    repo_name = str(repo.get("name") or "")
    if count_commits:
        (
            commit_count,
            all_refs_count,
            commit_elapsed_seconds,
            optimization_values,
        ) = fetch_commit_count(
            endpoint,
            token,
            repo_name,
            count_commits_rev,
            schema=schema,
            unavailable_values=unavailable_values,
            max_retries=max_retries,
        )
    if run_search_pattern is not None:
        (
            search_match_count,
            search_elapsed_seconds,
            search_limit_hit,
            search_alert_title,
        ) = fetch_run_search(
            endpoint,
            token,
            repo_name,
            run_search_pattern,
            max_retries=max_retries,
        )
    if skipped_file_reasons and has_skipped_files(repo):
        skipped_file_reason_search_results = collect_skipped_file_reason_search_results(
            endpoint,
            token,
            repo,
            max_retries,
        )
    return RepoProcessingResult(
        index=index,
        target=target,
        repo=repo,
        row=row,
        commit_count=commit_count,
        all_refs_count=all_refs_count,
        commit_elapsed_seconds=commit_elapsed_seconds,
        optimization_values=optimization_values,
        search_match_count=search_match_count,
        search_elapsed_seconds=search_elapsed_seconds,
        search_limit_hit=search_limit_hit,
        search_alert_title=search_alert_title,
        skipped_file_reason_search_results=skipped_file_reason_search_results,
    )


def append_processing_result_columns(
    row: list[Any],
    result: RepoProcessingResult,
    *,
    count_commits: bool,
    run_search: bool,
) -> list[Any]:
    """Append optional column blocks from a processed repo result"""
    with_commit = append_commit_count(
        row,
        result.commit_count,
        result.all_refs_count,
        result.commit_elapsed_seconds,
        result.optimization_values,
        count_commits=count_commits,
    )
    return append_run_search(
        with_commit,
        result.search_match_count,
        result.search_elapsed_seconds,
        result.search_limit_hit,
        result.search_alert_title,
        run_search=run_search,
    )


def log_processing_result(
    result: RepoProcessingResult,
    *,
    count_commits: bool,
    run_search_pattern: str | None,
) -> None:
    """Log optional per-repo query results in CSV order"""
    position = f"[{result.index}/{result.target}]"
    repo_label = (
        result.repo.get("name") or result.repo.get("url") or result.repo.get("id")
    )
    if count_commits:
        default_str = "?" if result.commit_count is None else f"{result.commit_count}"
        all_refs_str = (
            "?" if result.all_refs_count is None else f"{result.all_refs_count}"
        )
        elapsed = result.commit_elapsed_seconds or 0.0
        if result.commit_count is None:
            logger.info(
                "%s No commit count for %s (default=%s, allRefs=%s) [query took %.3fs]",
                position,
                repo_label,
                default_str,
                all_refs_str,
                elapsed,
            )
        else:
            logger.info(
                "%s Commit count for %s: default=%s, allRefs=%s [query took %.3fs]",
                position,
                repo_label,
                default_str,
                all_refs_str,
                elapsed,
            )
    if run_search_pattern is not None:
        count_str = (
            "?" if result.search_match_count is None else f"{result.search_match_count}"
        )
        limit_suffix = " (limit hit)" if result.search_limit_hit else ""
        alert_suffix = (
            f" alert={result.search_alert_title!r}" if result.search_alert_title else ""
        )
        logger.info(
            "%s Search %s in %s: matches=%s%s%s [query took %.3fs]",
            position,
            run_search_pattern,
            repo_label,
            count_str,
            limit_suffix,
            alert_suffix,
            result.search_elapsed_seconds or 0.0,
        )


def iter_repo_processing_results(
    endpoint: str,
    token: str,
    max_repos: int | None,
    *,
    page_size: int,
    scope_repo: str | None,
    can_read_protected_fields: bool,
    schema: GraphQLSchema,
    count_commits: bool,
    count_commits_rev: str,
    run_search_pattern: str | None,
    skipped_file_reasons: bool,
    unavailable_values: dict[str, str],
    concurrency: int,
    max_retries: int,
) -> Iterator[RepoProcessingResult]:
    """Yield processed repos, parallelizing optional per-repo queries"""
    repos = fetch_repos(
        endpoint,
        token,
        max_repos,
        page_size=page_size,
        scope_repo=scope_repo,
        can_read_protected_fields=can_read_protected_fields,
        schema=schema,
        max_retries=max_retries,
    )
    use_threads = concurrency > 1 and (
        count_commits or run_search_pattern is not None or skipped_file_reasons
    )
    if not use_threads:
        for index, target, repo in repos:
            yield collect_repo_processing_result(
                endpoint,
                token,
                index,
                target,
                repo,
                count_commits=count_commits,
                count_commits_rev=count_commits_rev,
                run_search_pattern=run_search_pattern,
                skipped_file_reasons=skipped_file_reasons,
                schema=schema,
                unavailable_values=unavailable_values,
                max_retries=max_retries,
            )
        return

    logger.info("Per-repo query concurrency: %d threads", concurrency)
    max_pending = concurrency * 2
    repo_iterator = iter(repos)
    pending_results: dict[concurrent.futures.Future[RepoProcessingResult], int] = {}

    def submit_repo(
        executor: concurrent.futures.ThreadPoolExecutor,
        index: int,
        target: int,
        repo: dict[str, Any],
    ) -> None:
        future = executor.submit(
            collect_repo_processing_result,
            endpoint,
            token,
            index,
            target,
            repo,
            count_commits=count_commits,
            count_commits_rev=count_commits_rev,
            run_search_pattern=run_search_pattern,
            skipped_file_reasons=skipped_file_reasons,
            schema=schema,
            unavailable_values=unavailable_values,
            max_retries=max_retries,
        )
        pending_results[future] = index

    def fill_pending(executor: concurrent.futures.ThreadPoolExecutor) -> None:
        while len(pending_results) < max_pending:
            try:
                index, target, repo = next(repo_iterator)
            except StopIteration:
                return
            submit_repo(executor, index, target, repo)

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        fill_pending(executor)
        while pending_results:
            done, _ = concurrent.futures.wait(
                pending_results,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                pending_results.pop(future)
                result = future.result()
                yield result
                fill_pending(executor)


def write_csv(
    out: TextIO,
    cloning_writer: LazyCSVWriter,
    indexing_writer: LazyCSVWriter,
    skipped_writer: LazyCSVWriter | None,
    skipped_file_reason_writer: LazyCSVWriter | None,
    endpoint: str,
    token: str,
    max_repos: int | None = None,
    *,
    reclone: bool = False,
    reindex: bool = False,
    count_commits: bool = False,
    scope_repo: str | None = None,
    count_commits_rev: str = "HEAD",
    run_search_pattern: str | None = None,
    page_size: int = PAGE_SIZE,
    concurrency: int = DEFAULT_CONCURRENCY,
    max_retries: int = DEFAULT_MAX_RETRIES,
    stats: StatsCollector | None = None,
    can_read_protected_fields: bool,
    schema: GraphQLSchema,
    unavailable_values: dict[str, str],
) -> tuple[int, int, int]:
    """Stream repos to CSVs and optionally trigger reclone/reindex mutations"""
    run_search_enabled = run_search_pattern is not None
    skipped_file_reasons_enabled = skipped_file_reason_writer is not None
    writer = csv.writer(out)
    writer.writerow(
        csv_columns_for(
            CSV_COLUMNS,
            count_commits=count_commits,
            run_search=run_search_enabled,
        ),
    )

    total = 0
    reclone_total = 0
    reindex_total = 0
    for result in iter_repo_processing_results(
        endpoint,
        token,
        max_repos,
        page_size=page_size,
        scope_repo=scope_repo,
        can_read_protected_fields=can_read_protected_fields,
        schema=schema,
        count_commits=count_commits,
        count_commits_rev=count_commits_rev,
        run_search_pattern=run_search_pattern,
        skipped_file_reasons=skipped_file_reasons_enabled,
        unavailable_values=unavailable_values,
        concurrency=concurrency,
        max_retries=max_retries,
    ):
        repo = result.repo
        row = result.row
        log_processing_result(
            result,
            count_commits=count_commits,
            run_search_pattern=run_search_pattern,
        )
        writer.writerow(
            append_processing_result_columns(
                row,
                result,
                count_commits=count_commits,
                run_search=run_search_enabled,
            ),
        )
        total += 1
        if stats is not None:
            stats.add(repo)
        repo_has_cloning_error = (
            "mirrorInfo.status" not in unavailable_values and has_cloning_error(repo)
        )
        repo_has_indexing_error = (
            "mirrorInfo.status" not in unavailable_values
            and "textSearchIndex.status" not in unavailable_values
            and has_indexing_error(repo)
        )
        if repo_has_cloning_error:
            cloning_writer.writerow(
                append_processing_result_columns(
                    row
                    + extract_csv_values(
                        repo,
                        CLONING_ERROR_EXTRA_COLUMNS,
                        unavailable_values,
                    ),
                    result,
                    count_commits=count_commits,
                    run_search=run_search_enabled,
                ),
            )
        # In single-repo (scope_repo) mode the user explicitly asked for
        # this repo, so trigger the mutation regardless of error state. In
        # full-repo mode keep the existing "only fix repos with errors"
        # guard so a blanket --reclone doesn't reclone the whole instance
        if reclone and (scope_repo is not None or repo_has_cloning_error):
            if trigger_reclone(endpoint, token, repo["id"], max_retries=max_retries):
                reclone_total += 1
        if repo_has_indexing_error:
            indexing_writer.writerow(
                append_processing_result_columns(
                    row,
                    result,
                    count_commits=count_commits,
                    run_search=run_search_enabled,
                ),
            )
        if reindex and (scope_repo is not None or repo_has_indexing_error):
            if trigger_reindex(endpoint, token, repo["id"], max_retries=max_retries):
                reindex_total += 1
        if skipped_writer is not None and has_skipped_files(repo):
            skipped_writer.writerow(
                append_processing_result_columns(
                    row
                    + extract_csv_values(
                        repo,
                        SKIPPED_FILES_EXTRA_COLUMNS,
                        unavailable_values,
                    ),
                    result,
                    count_commits=count_commits,
                    run_search=run_search_enabled,
                ),
            )
        if skipped_file_reason_writer is not None:
            write_skipped_file_reason_rows(
                skipped_file_reason_writer,
                endpoint,
                result.skipped_file_reason_search_results,
            )
    return (total, reclone_total, reindex_total)


def log_http_error(exc: HTTPRequestError) -> None:
    """Log status, headers, body, and traceback of a non-2xx HTTP response"""
    logger.error("HTTP %s %s", exc.status, exc.reason)
    logger.error("URL: %s", exc.url)
    for header, value in exc.headers:
        logger.error("  %s: %s", header, value)
    body = exc.body.decode(errors="replace")
    if body:
        logger.error("Response body:\n%s", body)
    logger.error("HTTP request failed", exc_info=exc)


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


class BlankLineHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Help formatter that spaces options and wraps explicit line breaks"""

    def _format_action(self, action: argparse.Action) -> str:
        return super()._format_action(action) + "\n"

    def _split_lines(self, text: str, width: int) -> list[str]:
        lines: list[str] = []
        for raw in text.splitlines():
            if not raw.strip():
                lines.append("")
                continue
            # Preserve any leading whitespace as the wrap indent so spaces
            # used for nesting/example lines aren't collapsed
            leading = raw[: len(raw) - len(raw.lstrip())]
            wrapped = textwrap.wrap(
                raw.lstrip(),
                width=width,
                initial_indent=leading,
                subsequent_indent=leading,
            )
            lines.extend(wrapped or [leading])
        return lines


def positive_int(value: str) -> int:
    """argparse type for integers >= 1"""
    try:
        n = int(value)
    except ValueError:
        msg = f"must be an integer, got {value!r}"
        raise argparse.ArgumentTypeError(msg) from None
    if n < 1:
        msg = f"must be a positive integer (>=1), got {n}"
        raise argparse.ArgumentTypeError(msg)
    return n


def non_negative_int(value: str) -> int:
    """argparse type for integers >= 0"""
    try:
        n = int(value)
    except ValueError:
        msg = f"must be an integer, got {value!r}"
        raise argparse.ArgumentTypeError(msg) from None
    if n < 0:
        msg = f"must be a non-negative integer (>=0), got {n}"
        raise argparse.ArgumentTypeError(msg)
    return n


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line arguments into a Namespace"""
    parser = argparse.ArgumentParser(
        description=(
            "List Sourcegraph repositories to CSVs with clone/index metadata\n"
            "\n"
            "Set SRC_ENDPOINT and SRC_ACCESS_TOKEN via env, .env, or args\n"
            "\n"
            f"Output file and column details are in {DEFAULT_CSV_SCHEMA_FILE}"
            "\n"
        ),
        epilog=("Source: https://github.com/sourcegraph/professional-services-public"),
        formatter_class=lambda prog: BlankLineHelpFormatter(
            prog,
            max_help_position=36,
        ),
    )
    parser.add_argument(
        "--limit",
        type=positive_int,
        default=None,
        metavar="int",
        help="Fetch at most <int> repos (>=1)",
    )
    parser.add_argument(
        "--statistics",
        action="store_true",
        help="Write statistics CSV files",
    )
    parser.add_argument(
        "--count-commits",
        nargs="?",
        const=True,
        default=False,
        metavar="REPO[@REV]",
        help=(
            "Append per-repo commit counts and cleanup metadata\n"
            "Optional REPO[@REV] scopes to one repo\n"
            "@REV affects only the exact ancestors count"
        ),
    )
    parser.add_argument(
        "--skipped-files",
        action="store_true",
        help="Write a CSV file for repos where Zoekt skipped files",
    )
    parser.add_argument(
        "--skipped-files-reason",
        nargs="?",
        const=True,
        metavar="REPO[@REV]",
        default=None,
        help=(
            "Write skipped-file details and reason counts for one repo\n"
            "Without REPO, write one aggregate skipped-file details CSV for "
            "all repos with skipped files"
        ),
    )
    parser.add_argument(
        "--run-search",
        metavar="PATTERN",
        default=None,
        help=("Run PATTERN once per repo and append result columns"),
    )
    parser.add_argument(
        "--reclone",
        nargs="?",
        const=True,
        default=False,
        metavar="REPO",
        help=(
            "With REPO: reclone only that repository\n"
            "Without REPO: reclone repos with cloning errors"
        ),
    )
    parser.add_argument(
        "--reindex",
        nargs="?",
        const=True,
        default=False,
        metavar="REPO",
        help=(
            "With REPO: reindex only that repository\n"
            "Without REPO: reindex all repos with indexing errors"
        ),
    )
    parser.add_argument(
        "--page-size",
        type=positive_int,
        default=PAGE_SIZE,
        metavar="int",
        help=(
            "Starting GraphQL repository page size "
            f"(default {PAGE_SIZE}; reduced automatically if rejected)"
        ),
    )
    parser.add_argument(
        "--concurrency",
        type=positive_int,
        default=DEFAULT_CONCURRENCY,
        metavar="int",
        help=(
            "Concurrent per-repo query threads for --count-commits and "
            f"--run-search (default {DEFAULT_CONCURRENCY})"
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
        "--write-csv-schema",
        action="store_true",
        help=f"Regenerate {DEFAULT_CSV_SCHEMA_FILE} and exit; no network required",
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


def collect_scope(args: argparse.Namespace) -> tuple[str, str] | None:
    """Return a shared single-repo scope for scoped flags, or None"""
    scoped: list[tuple[str, str]] = [
        (flag_name, value)
        for flag_name, value in (
            ("--count-commits", args.count_commits),
            ("--reclone", args.reclone),
            ("--reindex", args.reindex),
        )
        if isinstance(value, str)
    ]
    if not scoped:
        return None
    parsed = [
        (flag_name, parse_repo_name(value), parse_repo_rev(value))
        for flag_name, value in scoped
    ]
    repo_names = {name for _, name, _ in parsed}
    if len(repo_names) > 1:
        details = ", ".join(f"{flag}={name}" for flag, name, _ in parsed)
        die(
            "scoped flags reference different repositories ("
            + details
            + "); pass the same REPO[@REV] to each, or run them in separate "
            "invocations",
        )
    repo_name = next(iter(repo_names))
    # Only --count-commits uses rev; reclone/reindex are repo-level mutations
    rev = "HEAD"
    for flag, _, candidate_rev in parsed:
        if flag == "--count-commits":
            rev = candidate_rev
            break
    return repo_name, rev


def run(args: argparse.Namespace, endpoint: str, token: str) -> None:
    """Confirm the connection, then stream every repo to the CSV file"""
    logger.info(
        "Retry policy: %d retries per GraphQL request (backoff: 1s, 2s, 4s, ...)",
        args.max_retries,
    )
    if args.count_commits:
        # Announce the longer per-repo timeout because this mode can be slow
        logger.info(
            "--count-commits enabled: per-repo commit-count query "
            "(timeout=%ds per request)",
            REQUEST_TIMEOUT_SECONDS_WITH_COMMIT_COUNT,
        )
    scope = collect_scope(args)
    if scope is not None:
        scope_repo, scope_rev = scope
        logger.info(
            "Scoped run: repository=%s, rev=%s "
            "(reclone=%s, reindex=%s, count-commits=%s)",
            scope_repo,
            scope_rev,
            bool(args.reclone),
            bool(args.reindex),
            bool(args.count_commits),
        )
    else:
        scope_repo = None
        scope_rev = "HEAD"
    sourcegraph_version = fetch_sourcegraph_version(
        endpoint,
        token,
        max_retries=args.max_retries,
    )
    logger.info("Sourcegraph version: %s", sourcegraph_version)
    schema = fetch_graphql_schema(
        endpoint,
        token,
        max_retries=args.max_retries,
    )
    unavailable_values = unavailable_csv_column_values(
        schema,
        sourcegraph_version,
    )
    logger.info(
        "GraphQL schema checked: %d CSV column(s) unavailable",
        len(unavailable_values),
    )
    if unavailable_values:
        logger.info(
            "Unavailable CSV columns will contain %r: %s",
            next(iter(unavailable_values.values())),
            ", ".join(sorted(unavailable_values)),
        )
    username, is_site_admin, can_read_protected_fields = fetch_current_user(
        endpoint,
        token,
        max_retries=args.max_retries,
    )
    logger.info(
        "Connected to: %s as: %s (%s)",
        endpoint,
        username,
        (
            "site admin"
            if is_site_admin
            else (
                "non-admin with REPO_MANAGEMENT#READ"
                if can_read_protected_fields
                else "non-admin"
            )
        ),
    )

    # Refuse admin-only mutations before a run starts emitting per-repo warnings
    if not is_site_admin and (args.reclone or args.reindex):
        flags = ", ".join(
            flag
            for flag, set_ in (
                ("--reclone", bool(args.reclone)),
                ("--reindex", bool(args.reindex)),
            )
            if set_
        )
        die(
            f"site-admin token required for: {flags}. "
            f"{username!r} is not a site admin on {endpoint}",
        )

    if not can_read_protected_fields:
        unavailable_values.update(admin_required_csv_column_values())
        logger.warning(
            "Token lacks REPO_MANAGEMENT#READ: skipping "
            "Repository.externalServices selection; "
            "mirrorInfo.remoteURL, mirrorInfo.shard, and "
            "mirrorInfo.repositoryStatistics columns will contain 'requires admin'",
        )

    # This targeted report does not need the full repo listing
    if isinstance(args.skipped_files_reason, str):
        # Other flags only affect full-listing mode
        ignored = [
            flag
            for flag, set_ in (
                ("--reclone", args.reclone),
                ("--reindex", args.reindex),
                ("--limit", args.limit is not None),
                ("--page-size", args.page_size != PAGE_SIZE),
                ("--concurrency", args.concurrency != DEFAULT_CONCURRENCY),
                ("--skipped-files", args.skipped_files),
                ("--count-commits", args.count_commits),
                ("--run-search", args.run_search is not None),
                ("--statistics", args.statistics),
            )
            if set_
        ]
        if ignored:
            logger.warning(
                "Ignoring %s: --skipped-files-reason runs a single targeted "
                "query and does not iterate the repo list",
                ", ".join(ignored),
            )
        write_skipped_files_reason(
            endpoint,
            token,
            args.skipped_files_reason,
            max_retries=args.max_retries,
        )
        return

    # Prefix outputs with endpoint, plus scoped repo/rev when applicable
    endpoint_sanitized = sanitize_endpoint_for_filename(endpoint)
    if scope_repo is not None:
        scope_suffix = sanitize_for_filename(scope_repo)
        # Only --count-commits uses rev; reclone/reindex filenames stay repo-only
        if args.count_commits and scope_rev != "HEAD":
            scope_suffix = f"{scope_suffix}-{sanitize_for_filename(scope_rev)}"
        prefix = f"{endpoint_sanitized}-{scope_suffix}"
    else:
        prefix = endpoint_sanitized
    output_path = Path(f"{prefix}-{DEFAULT_OUTPUT_FILE}")
    cloning_errors_path = Path(f"{prefix}-{DEFAULT_CLONING_ERRORS_FILE}")
    indexing_errors_path = Path(f"{prefix}-{DEFAULT_INDEXING_ERRORS_FILE}")
    skipped_files_path = (
        Path(f"{prefix}-{DEFAULT_SKIPPED_FILES_FILE}") if args.skipped_files else None
    )
    skipped_file_reasons_path = (
        Path(f"{prefix}-{DEFAULT_SKIPPED_FILE_REASONS_FILE}")
        if args.skipped_files_reason is True
        else None
    )
    # Remove stale optional outputs; LazyCSVWriter recreates only non-empty ones
    cloning_errors_path.unlink(missing_ok=True)
    indexing_errors_path.unlink(missing_ok=True)
    if skipped_files_path is not None:
        skipped_files_path.unlink(missing_ok=True)
    if skipped_file_reasons_path is not None:
        skipped_file_reasons_path.unlink(missing_ok=True)
    # Clear stale stats outputs even when --statistics is not enabled this run
    for suffix, *_ in STATS_FILES:
        Path(f"{prefix}-{DEFAULT_STATS_FILE_PREFIX}-{suffix}.csv").unlink(
            missing_ok=True,
        )

    stats = StatsCollector() if args.statistics else None
    count_commits_enabled = bool(args.count_commits)
    run_search_pattern: str | None = args.run_search
    run_search_enabled = run_search_pattern is not None
    cloning_writer = LazyCSVWriter(
        cloning_errors_path,
        csv_columns_for(
            CLONING_ERROR_CSV_COLUMNS,
            count_commits=count_commits_enabled,
            run_search=run_search_enabled,
        ),
    )
    indexing_writer = LazyCSVWriter(
        indexing_errors_path,
        csv_columns_for(
            CSV_COLUMNS,
            count_commits=count_commits_enabled,
            run_search=run_search_enabled,
        ),
    )
    skipped_writer = (
        LazyCSVWriter(
            skipped_files_path,
            csv_columns_for(
                SKIPPED_FILES_CSV_COLUMNS,
                count_commits=count_commits_enabled,
                run_search=run_search_enabled,
            ),
        )
        if skipped_files_path is not None
        else None
    )
    skipped_file_reason_writer = (
        LazyCSVWriter(
            skipped_file_reasons_path,
            [name for name, _, _, _ in SKIPPED_FILE_REASON_COLUMNS],
        )
        if skipped_file_reasons_path is not None
        else None
    )
    # Keep optional writers in the same context-manager block
    skipped_cm = (
        skipped_writer if skipped_writer is not None else contextlib.nullcontext()
    )
    skipped_file_reason_cm = (
        skipped_file_reason_writer
        if skipped_file_reason_writer is not None
        else contextlib.nullcontext()
    )
    with (
        output_path.open("w", newline="") as out,
        cloning_writer,
        indexing_writer,
        skipped_cm,
        skipped_file_reason_cm,
    ):
        total, reclone_total, reindex_total = write_csv(
            out,
            cloning_writer,
            indexing_writer,
            skipped_writer,
            skipped_file_reason_writer,
            endpoint,
            token,
            args.limit,
            reclone=bool(args.reclone),
            reindex=bool(args.reindex),
            count_commits=bool(args.count_commits),
            scope_repo=scope_repo,
            count_commits_rev=scope_rev,
            run_search_pattern=run_search_pattern,
            page_size=args.page_size,
            concurrency=args.concurrency,
            max_retries=args.max_retries,
            stats=stats,
            can_read_protected_fields=can_read_protected_fields,
            schema=schema,
            unavailable_values=unavailable_values,
        )

    if stats is not None:
        stats_paths = write_stats(prefix, stats)
        for stats_path in stats_paths:
            logger.info("Wrote statistics to %s", stats_path.name)

    logger.info("Wrote %d repos to %s", total, output_path.name)
    if cloning_writer.count:
        logger.info(
            "Wrote %d repos with cloning errors to %s",
            cloning_writer.count,
            cloning_errors_path.name,
        )
    if indexing_writer.count:
        logger.info(
            "Wrote %d repos with indexing errors to %s",
            indexing_writer.count,
            indexing_errors_path.name,
        )
    if skipped_writer is not None and skipped_writer.count:
        logger.info(
            "Wrote %d repos with skipped files to %s",
            skipped_writer.count,
            skipped_writer.path.name,
        )
    if skipped_file_reason_writer is not None and skipped_file_reason_writer.count:
        logger.info(
            "Wrote %d skipped-file reason row(s) to %s",
            skipped_file_reason_writer.count,
            skipped_file_reason_writer.path.name,
        )
    if args.reclone:
        logger.info("Triggered recloneRepository for %d repo(s)", reclone_total)
    if args.reindex:
        logger.info("Triggered reindexRepository for %d repo(s)", reindex_total)


def redact_argv_for_log(argv: list[str]) -> str:
    """Render argv shell-safely, redacting --src-access-token values"""
    redacted: list[str] = []
    skip_next = False
    for arg in argv:
        if skip_next:
            redacted.append("REDACTED")
            skip_next = False
            continue
        if arg == "--src-access-token":
            redacted.append(arg)
            skip_next = True
        elif arg.startswith("--src-access-token="):
            redacted.append("--src-access-token=REDACTED")
        else:
            redacted.append(arg)
    return " ".join(shlex.quote(a) for a in redacted)


def timestamped_log_path() -> Path:
    """Return list-repos-YYYY-MM-DD-HH-MM-SS.log for this run"""
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    return Path(f"{DEFAULT_LOG_FILE_STEM}-{timestamp}.log")


def configure_logging(log_path: Path) -> None:
    """Send INFO-level logs to both stderr (live feedback) and log_path"""
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Clear existing handlers (e.g. on re-entry from tests)
    for handler in list(root.handlers):
        root.removeHandler(handler)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(stderr_handler)

    file_handler = logging.FileHandler(
        log_path,
        mode="w",
        encoding="utf-8",
        delay=True,
    )
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s"),
    )
    root.addHandler(file_handler)


def _log_uncaught_exception(
    exc_type: type[BaseException],
    exc_value: BaseException,
    exc_traceback: Any,
) -> None:
    """Route uncaught exceptions through the logger"""
    if issubclass(exc_type, KeyboardInterrupt):
        logger.warning("Interrupted by user (Ctrl-C); exiting")
        return
    logger.error(
        "Uncaught exception",
        exc_info=(exc_type, exc_value, exc_traceback),
    )


def main() -> None:
    """Entry point: configure logging, load env, parse args, run, handle errors"""
    configure_logging(timestamped_log_path())
    # Include pre-run failures in the timestamped log file
    sys.excepthook = _log_uncaught_exception

    args = parse_args(sys.argv[1:])
    # Schema generation is offline and credential-free
    if args.write_csv_schema:
        write_csv_schema(Path(DEFAULT_CSV_SCHEMA_FILE))
        return
    load_dotenv()
    endpoint, token = require_credentials(args)
    logger.info(
        "Running: %s (SRC_ENDPOINT=%s)",
        redact_argv_for_log(sys.argv),
        endpoint,
    )

    try:
        run(args, endpoint, token)
    except HTTPRequestError as exc:
        log_http_error(exc)
        sys.exit(1)
    except OSError:
        logger.exception(
            "Could not connect to the server. Check your network and SRC_ENDPOINT",
        )
        sys.exit(1)
    except ValueError as exc:
        die(str(exc))
    except GraphQLError as exc:
        if ":53: no such host" in str(exc):
            logger.error(
                "There's a problem with your Sourcegraph instance "
                "(DNS lookup failure for an internal service). Please try again"
            )
        else:
            logger.exception("GraphQL request failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
