# `list-repos.py`

Export repository health and size metadata from a Sourcegraph instance to CSV

The script is meant for support and troubleshooting work: it streams the repo
list through Sourcegraph's GraphQL API, writes endpoint-prefixed CSV files, and
keeps memory use flat on large instances

## Requirements

- Python 3.10 or newer
- Sourcegraph 5.2.0 or newer
- A Sourcegraph access token starting with `sgp_`
  - Some columns and repair actions need a site-admin token.
  Non-admin tokens can still list repositories, but admin-only
  CSV columns are left blank.

## Quick start

From this directory:

```sh
export SRC_ENDPOINT="https://sourcegraph.example.com"
export SRC_ACCESS_TOKEN="sgp_..."

python3 list-repos.py
```

The script also reads a local `.env` file when environment variables are not
set:

```sh
SRC_ENDPOINT=https://sourcegraph.example.com
SRC_ACCESS_TOKEN=sgp_...
```

Command-line credentials are supported, but environment variables or `.env` are
safer because they do not put the token in shell history:

```sh
python3 list-repos.py \
  --src-endpoint "https://sourcegraph.example.com" \
  --src-access-token "sgp_..."
```

## Common commands

```sh
# List every repository
python3 list-repos.py

# Smoke test against a small sample
python3 list-repos.py --limit 100

# Include repos whose latest index skipped files
python3 list-repos.py --skipped-files

# Explain skipped files for one repo and indexed revision
python3 list-repos.py --skipped-files-reason github.com/org/repo@main

# Append per-repo commit counts and cleanup metadata
python3 list-repos.py --count-commits

# Count commits for one repo only
python3 list-repos.py --count-commits github.com/org/repo@develop

# Count matches for a Sourcegraph search pattern in every repo
python3 list-repos.py --run-search 'TODO patternType:literal'

# Write size and index-ratio summary CSVs
python3 list-repos.py --statistics
```

Site admins can also trigger repair mutations:

```sh
# Reclone every repo currently in a cloning-error state
python3 list-repos.py --reclone

# Reclone one repo, whether in an error state or not
python3 list-repos.py --reclone github.com/org/repo

# Reindex every cloned repo missing a search index
python3 list-repos.py --reindex

# Reindex one repo
python3 list-repos.py --reindex github.com/org/repo
```

## Output files

- Output files are written in the current directory
- Filenames are prefixed with the hostname from `SRC_ENDPOINT`

Possible output files:

| File                                      | When written                                              |
| ----------------------------------------- | --------------------------------------------------------- |
| `<prefix>-repos.csv`                      | Every normal listing run                                  |
| `<prefix>-repos-with-cloning-errors.csv`  | When one or more repos have a cloning or corruption error |
| `<prefix>-repos-with-indexing-errors.csv` | When one or more cloned repos are missing a search index  |
| `<prefix>-repos-with-skipped-files.csv`   | With `--skipped-files` and one or more skipped-file repos |
| `<prefix>-stats-*.csv`                    | With `--statistics`                                       |
| `<prefix>-<repo>-<rev>-skipped-files.csv` | With `--skipped-files-reason REPO[@REV]`                  |
| `<prefix>-<repo>-<rev>-skipped-stats.csv` | With `--skipped-files-reason REPO[@REV]`                  |

- Optional columns from `--count-commits` and `--run-search` are appended to the
  per-repo CSVs
- See [`CSV_SCHEMA.md`](CSV_SCHEMA.md) for the exact columns, types, and
  admin-only fields

## Operational notes

- `--count-commits` sends one extra GraphQL request per repository and can be
  slow on large monorepos
- The script writes progress and failures to `list-repos.log` and stderr

## Development notes

If you add, remove, rename, or reorder CSV columns in `list-repos.py`, update
the column tuples and regenerate the generated reference:

```sh
python3 list-repos.py --write-csv-schema
```

To refresh `schema.gql` from an instance for development:

```sh
npx -y get-graphql-schema \
  -h "Authorization=token $SRC_ACCESS_TOKEN" \
  "$SRC_ENDPOINT/.api/graphql" > schema.gql
```
