# Troubleshooting why the Zoekt indexer skips files

`zoekt_skip_reasons.py` demonstrates the file checks Zoekt uses before indexing a file

It prints each skip reason in Zoekt's check order,
and if the file would match that reason,
using Sourcegraph's default settings for Zoekt:

- Maximum file size: 1 MiB
- Maximum unique trigrams: 20,000

## What is a trigram?

A trigram is a 3-character string, of any 3 adjacent characters in a file

For example, in the string `abcd` both `abc` and `bcd` are unique trigrams

## Why count unique trigrams per file?

Counting unique trigrams is a handy way to sort between code files,
vs machine-generated files

Most code files use a lot of repetitive trigrams,
ex. reusing a variable name in multiple places within a function

Many machine-generated files have a large number of unique trigrams,
ex. encrypted files

When optimizing our search and ranking algorithms, we find that customers are
more interested in getting search results from code files than encrypted files,
and 20k unique trigrams seems like a fair line in the sand

Files with many unique trigrams are computationally intensive to index,
and the index files are expensive to load into / keep in memory while searching

## Requirements

- Python >= 3.9
- Memory consumption is up to 2x the size of the checked file, plus a small overhead

## Usage

```sh
./zoekt_skip_reasons.py /path/to/file
```

Example 1

```sh
./zoekt_skip_reasons.py "test-files/4 - Contains too many trigrams.txt"
```

Example 2

```sh
for file in test-files/*; do ./zoekt_skip_reasons.py "$file"; echo; done
```

## Workaround

First open a support ticket by emailing <support@sourcegraph.com>.
If directed, add this to your Sourcegraph instance's site config,
to skip Zoekt's max file size and max unique trigram checks.
We recommend using glob patterns to target specific files,
rather than disabling these checks for all files,
as this creates much more work for the Zoekt indexer.

```json
"search.largeFiles": [
  "*",
],
```

## Internal

Context in [Slack thread](https://sourcegraph.slack.com/archives/C05MW2TMYAV/p1781549743419799)
