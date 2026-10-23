# Repository context filter builder

Generate an `agents.contextFilters` JSON object that excludes repositories matching
one or more [Sourcegraph streaming search API](https://sourcegraph.com/docs/api/stream-api)

## Usage

Run from this directory. Set the instance URL and, for authenticated instances,
a Sourcegraph access token via environment variables:

```bash
export SRC_ENDPOINT='https://sourcegraph.example.com'
# Set SRC_ACCESS_TOKEN securely in your environment; do not commit tokens.
```

Pass each complete query as a separate, quoted positional argument:

```bash
python repo-context-filter-builder.py \
  'repo:github.com/example/.* select:repo count:all' \
  'file:secrets.yaml select:repo count:all' > context-filters.json
```

Or create a UTF-8 file containing one complete query per line:

```text
repo:github.com/example/.* select:repo count:all
file:secrets.yaml select:repo count:all
```

```bash
python repo-context-filter-builder.py --from_file queries.txt > context-filters.json
```

`--from-file` is also accepted. Blank lines are ignored; every other line is a
query (there is no comment syntax). File and positional inputs are mutually
exclusive. Override the instance with `--url https://sourcegraph.example.com`
and the socket timeout with `--timeout 600` (seconds; default 300).
The default endpoint, when `SRC_ENDPOINT` is unset, is `https://sourcegraph.com`.
Authentication uses `Authorization: token` with `SRC_ACCESS_TOKEN` if set.

## Output

```json
{
  "agents.contextFilters": {
    "exclude": [
      { "repoNamePatterns": ["^github\\.com/example/backend$"] },
      { "repoNamePatterns": ["^github\\.com/example/secrets$"] }
    ]
  }
}
```

The tool unions results from all queries, removes duplicates, sorts repository
names, and emits **one object with one anchored, regex-escaped repository name
per repository**. It does not generate file-path exclusions. Zero matches produces
an empty `exclude` array. Output is valid JSON, including the outer braces, ready
to merge into your configuration; it does not modify existing configuration files.

Queries are sent unchanged. Use `select:repo` to request repository results for
content searches and `count:all` to avoid the default result count limit. Native
`type:repo` queries and file, content, symbol, commit, and diff results are also
supported. Sourcegraph's search scope and defaults still apply: add
`archived:yes` or `fork:yes` when those repositories should be included.
Server limits and timeouts can still prevent exhaustive results. Skipped-result
notices and alerts are printed to stderr; **review these warnings before relying
on the output**, particularly for security-sensitive exclusions.

HTTP/API errors, invalid JSON, or a stream missing its final `done` event cause a
nonzero exit with no JSON emitted. Shell redirection still creates/truncates the
output file on failure, so check the exit status before using it. Searches run
sequentially; JSON is written only after all queries succeed. No network access
is required for the unit tests.

