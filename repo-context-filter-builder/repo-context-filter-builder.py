#!/usr/bin/env python3
"""Build agents.contextFilters exclusions from Sourcegraph streaming searches."""

import argparse
import json
import math
import os
from pathlib import Path
import re
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen


class SearchError(Exception):
    """A search failed or its stream was incomplete."""


def stream_events(lines):
    """Decode SSE events, including CRLF, comments, and multiline data."""
    event = "message"
    data = []
    for raw in lines:
        line = raw.decode("utf-8").rstrip("\r\n")
        if not line:
            if data:
                yield event, json.loads("\n".join(data))
            event, data = "message", []
        elif not line.startswith(":"):
            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if field == "event":
                event = value
            elif field == "data":
                data.append(value)
    if data:
        yield event, json.loads("\n".join(data))


def search_repositories(url, token, query, timeout):
    params = urlencode({"q": query, "v": "V3", "display": "-1"})
    request = Request(
        f"{url.rstrip('/')}/.api/search/stream?{params}",
        headers={
            "Accept": "text/event-stream",
            "User-Agent": "repo-context-filter-builder/1.0",
        },
    )
    if token:
        request.add_header("Authorization", f"token {token}")
    repositories = set()
    completed = False
    with urlopen(request, timeout=timeout) as response:
        for event, data in stream_events(response):
            if event == "matches":
                for match in data:
                    # Repository matches use name; file/commit/etc. use repository.
                    name = match.get("repository")
                    if match.get("type") == "repo":
                        name = match.get("name") or name
                    if not isinstance(name, str) or not name:
                        raise SearchError("Search returned a match without a repository name")
                    repositories.add(name)
            elif event == "error":
                raise SearchError(f"Search API error: {json.dumps(data)}")
            elif event == "alert":
                if data.get("alertType") == "error":
                    raise SearchError(f"Search alert: {json.dumps(data)}")
                print(f"Warning: search alert: {json.dumps(data)}", file=sys.stderr)
            elif event == "progress":
                for skipped in data.get("skipped", []):
                    print(
                        f"Warning: {skipped.get('reason', 'skipped')}: "
                        f"{skipped.get('message', skipped.get('title', 'Results skipped'))}",
                        file=sys.stderr,
                    )
            elif event == "done":
                completed = True
                break
    if not completed:
        raise SearchError("Search stream ended without a done event; refusing partial output")
    return repositories


def build_filters(repositories):
    # Escape regex metacharacters only (compatible with Sourcegraph's regex syntax).
    return {
        "agents.contextFilters": {
            "exclude": [
                {"repoNamePatterns": ["^" + re.sub(r"([\\.^$|?*+()\[\]{}])", r"\\\1", name) + "$"]}
                for name in sorted(set(repositories))
            ]
        }
    }


def positive_timeout(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("timeout must be a finite positive number")
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("queries", nargs="*", help="Search queries (quote each query separately)")
    parser.add_argument("--from_file", "--from-file", type=Path, help="UTF-8 file with one query per line")
    parser.add_argument("--url", default=os.environ.get("SRC_ENDPOINT", "https://sourcegraph.com"),
                        help="Sourcegraph instance URL (default: SRC_ENDPOINT or https://sourcegraph.com)")
    parser.add_argument("--timeout", type=positive_timeout, default=300,
                        help="Socket timeout in seconds (default: 300)")
    args = parser.parse_args(argv)
    if bool(args.queries) == bool(args.from_file):
        parser.error("provide either positional queries or --from_file, but not both")
    parsed_url = urlsplit(args.url)
    if parsed_url.scheme not in ("http", "https") or not parsed_url.netloc or parsed_url.query or parsed_url.fragment or parsed_url.username or parsed_url.password:
        parser.error("--url must be an HTTP(S) instance URL without credentials, query, or fragment")
    try:
        queries = args.queries
        if args.from_file:
            queries = args.from_file.read_text(encoding="utf-8").splitlines()
        queries = [query.strip() for query in queries if query.strip()]
        if not queries:
            parser.error("at least one nonblank query is required")
        repositories = set()
        for query in queries:
            repositories.update(search_repositories(
                args.url, os.environ.get("SRC_ACCESS_TOKEN"), query, args.timeout,
            ))
        json.dump(build_filters(repositories), sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0
    except HTTPError as error:
        print(f"Error: Sourcegraph returned HTTP {error.code} {error.reason}", file=sys.stderr)
    except (SearchError, URLError, OSError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
