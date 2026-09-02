# `site_config_history_search.py`

Find which site configuration changes on a Sourcegraph instance touched a
given setting, and who made them.

The script pages through the instance's site configuration history and prints
every change whose diff adds or removes a line containing your search text,
with its date, author, and full diff. Read-only.

Matching is a plain substring match on changed lines, so `auth` also matches
`oauth` and comment lines.

## Requirements

- Python 3.10 or newer (standard library only)
- A site-admin Sourcegraph access token, starting with `sgp_`

Works the same on Windows, Linux, and macOS. On Windows, use `python` or `py`
in place of `python3`.

## Quick start

From this directory:

```sh
# Linux / macOS
export SRC_ENDPOINT="https://sourcegraph.example.com"
export SRC_ACCESS_TOKEN="sgp_..."

# Windows PowerShell
$env:SRC_ENDPOINT = "https://sourcegraph.example.com"
$env:SRC_ACCESS_TOKEN = "sgp_..."

python3 site_config_history_search.py auth.providers
```

The script also reads a `.env` file in the current directory when the
environment variables are not set:

```sh
SRC_ENDPOINT=https://sourcegraph.example.com
SRC_ACCESS_TOKEN=sgp_...
```

Credentials can also be passed as `--src-endpoint` and `--src-access-token`,
but environment variables or `.env` keep the token out of shell history.

## Options

```sh
# One line per change: date, version, and the new value of the matching line
python3 site_config_history_search.py gitMaxCodehostRequestsPerSecond --short

# History entries fetched per GraphQL request (default 100)
python3 site_config_history_search.py auth.providers --page-size 500
```

`--short` output, newest first; `<deleted>` means the change removed the line:

```text
2026-09-01T23:51:52Z    17  <deleted>
2026-08-20T23:31:02Z     8  "gitMaxCodehostRequestsPerSecond": 0.0139,
2026-08-20T23:24:05Z     7  "gitMaxCodehostRequestsPerSecond": 0.00139,
```

A change that adds several matching lines prints one row per line.

## Output

Progress and status messages go to stderr. Matching changes go to stdout,
newest first, so you can redirect them to a file:

```text
Date:    2026-08-19T08:09:21Z
Version: 1
Author:  admin
  Name:  Ada Admin
  ID:    1
  Email: admin@example.com (verified)
@@ -1 +1,14 @@
+{
+  "auth.providers": [
...
```

- `Version` is the site configuration's row ID in the database. Versions whose
  redacted contents match the previous version are skipped, so numbers can
  have gaps.
- `Author` is the username, display name, database user ID, and primary email
  (with verification status). `Name` and `Email` are blank when the user has
  none. The whole block is
  `<none: internal process, SITE_CONFIG_FILE reload, or deleted user>` when
  the change was not made by a logged-in user, or that user has since been
  deleted; the API does not distinguish these cases.
- Secrets in the diff appear as `REDACTED-DATA-CHUNK-...`; the API never
  returns unredacted history.

The script exits non-zero if the history could not be fetched.

## Development

```sh
mise trust && mise install
mise run check
```
