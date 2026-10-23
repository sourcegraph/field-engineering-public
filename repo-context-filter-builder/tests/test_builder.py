import contextlib
import importlib.util
import io
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit


SCRIPT = Path(__file__).resolve().parents[1] / "repo-context-filter-builder.py"
spec = importlib.util.spec_from_file_location("builder", SCRIPT)
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


def response(events):
    return io.BytesIO("".join(
        f"event: {event}\ndata: {json.dumps(data)}\n\n" for event, data in events
    ).encode())


class BuilderTests(unittest.TestCase):
    def test_exact_sorted_unique_patterns(self):
        name = "github.com/example/repo+(test)"
        output = builder.build_filters([name, "github.com/example/a", name])
        exclusions = output["agents.contextFilters"]["exclude"]
        self.assertEqual(len(exclusions), 2)
        self.assertEqual(exclusions[0], {"repoNamePatterns": [r"^github\.com/example/a$"]})
        pattern = exclusions[1]["repoNamePatterns"][0]
        self.assertIsNotNone(re.fullmatch(pattern, name))
        self.assertIsNone(re.fullmatch(pattern, "githubXcom/example/repo+(test)"))
        self.assertEqual(json.loads(json.dumps(output)), output)

    def test_sse_multiline_comments_crlf_and_eof(self):
        stream = io.BytesIO(b': comment\r\nevent: matches\r\ndata: [\r\ndata: {"type": "repo", "name": "example/a"}]\r\n\r\nevent: done\ndata: {}')
        self.assertEqual(list(builder.stream_events(stream)), [
            ("matches", [{"type": "repo", "name": "example/a"}]), ("done", {}),
        ])

    def test_request_and_match_types(self):
        events = [("matches", [
            {"type": "repo", "name": "github.com/example/a"},
            *[{"type": kind, "repository": "github.com/example/b"}
              for kind in ("content", "path", "symbol", "commit", "diff")],
        ]), ("done", {})]
        with patch.object(builder, "urlopen", return_value=response(events)) as opener:
            self.assertEqual(builder.search_repositories(
                "https://sourcegraph.example.com/", "test-token", "repo:example a & b", 20,
            ), {"github.com/example/a", "github.com/example/b"})
        request = opener.call_args.args[0]
        self.assertEqual(request.get_header("Authorization"), "token test-token")
        self.assertEqual(request.get_header("Accept"), "text/event-stream")
        self.assertEqual(request.get_header("User-agent"), "repo-context-filter-builder/1.0")
        self.assertEqual(urlsplit(request.full_url).path, "/.api/search/stream")
        self.assertEqual(parse_qs(urlsplit(request.full_url).query), {
            "q": ["repo:example a & b"], "v": ["V3"], "display": ["-1"],
        })

    def test_failed_or_incomplete_streams(self):
        for events in (
            [("matches", [{"type": "repo", "name": "example/a"}])],
            [("error", {"message": "failed"})],
            [("alert", {"alertType": "error", "title": "Invalid query"})],
            [("matches", [{"type": "repo"}]), ("done", {})],
        ):
            with self.subTest(events=events), patch.object(builder, "urlopen", return_value=response(events)):
                with self.assertRaises(builder.SearchError):
                    builder.search_repositories("https://sourcegraph.example.com", None, "query", 1)

    def test_skipped_results_warn_on_stderr(self):
        events = [("progress", {"skipped": [{"reason": "shard-match-limit", "message": "limit hit"}]}), ("done", {})]
        stderr = io.StringIO()
        with patch.object(builder, "urlopen", return_value=response(events)), contextlib.redirect_stderr(stderr):
            self.assertEqual(builder.search_repositories("https://sourcegraph.example.com", None, "query", 1), set())
        self.assertIn("limit hit", stderr.getvalue())

    def test_cli_both_input_modes_and_union(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queries.txt"
            path.write_text("repo:example/a\n\nrepo:example/b\n", encoding="utf-8")
            for arguments in (["repo:example/a", "repo:example/b"], ["--from_file", str(path)]):
                stdout = io.StringIO()
                with patch.object(builder, "search_repositories", side_effect=[{"example/a"}, {"example/a", "example/b"}]) as search, contextlib.redirect_stdout(stdout):
                    self.assertEqual(builder.main(arguments), 0)
                self.assertEqual(search.call_count, 2)
                self.assertEqual(json.loads(stdout.getvalue()), builder.build_filters(["example/a", "example/b"]))

    def test_cli_invalid_inputs(self):
        for arguments in ([], ["query", "--from_file", "queries.txt"], [" "], ["query", "--timeout", "0"], ["query", "--timeout", "nan"], ["query", "--url", "file:///tmp"]):
            with self.subTest(arguments=arguments), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    builder.main(arguments)
                self.assertEqual(error.exception.code, 2)

    def test_failure_does_not_emit_partial_json(self):
        for failure in (builder.SearchError("incomplete"), HTTPError("https://sourcegraph.example.com", 401, "Unauthorized", {}, None)):
            stdout, stderr = io.StringIO(), io.StringIO()
            with patch.object(builder, "search_repositories", side_effect=[{"example/a"}, failure]), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                self.assertEqual(builder.main(["one", "two"]), 1)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("Error:", stderr.getvalue())

    def test_empty_results(self):
        self.assertEqual(builder.build_filters([]), {"agents.contextFilters": {"exclude": []}})


if __name__ == "__main__":
    unittest.main()
