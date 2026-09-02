# TODO

- Mutation batches (`--fetch`, `--reclone`, `--reindex`) go through the same
  HTTP retry logic as queries. A retried reclone is reported as `skipped`
  ("another reclone is in progress"); fetch and reindex only re-enqueue. Confirm
  this against a real instance under a forced retry (e.g. a 503) before
  relying on it.
- Stream aggregate skipped-file results as each indexed ref completes to reduce
  peak client memory. First confirm that lowering retained results provides a
  meaningful improvement over the current bounded queue.
- Measure the repository-listing response before considering a two-phase query
  for large fields such as sync output, corruption logs, and indexed refs. Any
  change must preserve every CSV value and avoid per-repository follow-up calls.
- Consider caching schema capabilities and accepted page size by endpoint and
  Sourcegraph version. Define safe invalidation before implementation.
