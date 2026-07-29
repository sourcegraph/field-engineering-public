# `find-empty-bitbucket-projects.py`

Count the repos on a Sourcegraph instance for each Bitbucket Server project
configured in the instance's code host connections, to find empty (or
unsynced) Bitbucket projects

The script reads every code host connection of kind `BITBUCKETSERVER`, and
for each item in the connection's `projectKeys` and `repositoryQuery` config
lists, runs a Sourcegraph search to count the matching repos. Items with 0
matching repos identify empty Bitbucket projects.

## Requirements

- Python 3.9 or newer (standard library only)
- Sourcegraph 7.0.0 or newer
- A site-admin Sourcegraph access token starting with `sgp_`
  (reading code host connections requires site admin)

## Quick start

From this directory:

```sh
export SRC_ENDPOINT="https://sourcegraph.example.com"
export SRC_ACCESS_TOKEN="sgp_..."

python3 find-empty-bitbucket-projects.py
```

The script also reads a local `.env` file when environment variables are not
set:

```sh
SRC_ENDPOINT=https://sourcegraph.example.com
SRC_ACCESS_TOKEN=sgp_...
```

Command-line credentials are supported, but environment variables or `.env`
are safer because they do not put the token in shell history:

```sh
python3 find-empty-bitbucket-projects.py \
  --src-endpoint "https://sourcegraph.example.com" \
  --src-access-token "sgp_..."
```

## Options

```sh
# Write the CSV to a specific path
python3 find-empty-bitbucket-projects.py --output counts.csv

# Searches to run in parallel (default 8); higher is faster but adds
# load on the Sourcegraph instance
python3 find-empty-bitbucket-projects.py --parallelism 16

# Change the per-request retry budget (default 5; backoff 1s, 2s, 4s, ...)
python3 find-empty-bitbucket-projects.py --retries 2
```

## Output

Each item's count is logged to the shell with the code host connection's
display name, URL, and username, followed by a summary of items with 0
matching repos. All items are also written to
`<endpoint-hostname>-bitbucket-project-repo-counts.csv`, with one row per
`projectKeys` / `repositoryQuery` item:

- `externalService.displayName`: code host connection name
- `externalService.url`: Bitbucket Server URL from the connection config
- `externalService.username`: username from the connection config
- `configField`: `projectKeys` or `repositoryQuery`
- `item`: the config list item, verbatim
- `projectKey`: project key or name the item was mapped to
- `searchQuery`: the Sourcegraph search which counted the repos
- `repositoryCount`: repos on the instance matching the item; 0 means empty
- `limitHit`: true when the search hit a result limit (count may be low)
- `alert`: search alert title, when the search raised one
- `note`: why an item was skipped, or the search error

## How items map to searches

- `projectKeys` items are project keys; the script counts repos named
  `{host}/{projectKey}/...`, honouring the connection's
  `repositoryPathPattern` when set
- `repositoryQuery` items are Bitbucket Server REST API query strings; the
  script extracts the `projectname` / `projectkey` parameter when present.
  Items without a project parameter (including the `none` sentinel) cannot
  be mapped to a repo name search, and are written to the CSV with a note
  instead of a count
