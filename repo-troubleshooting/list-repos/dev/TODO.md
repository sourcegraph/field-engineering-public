# TODO

- Verify whether `recloneRepository` and `reindexRepository` are idempotent. Until
  then, avoid retrying a mutation after an ambiguous network failure, or verify
  its resulting state before retrying.
- Stream aggregate skipped-file results as each indexed ref completes to reduce
  peak client memory. First confirm that lowering retained results provides a
  meaningful improvement over the current bounded queue.
- Measure the repository-listing response before considering a two-phase query
  for large fields such as sync output, corruption logs, and indexed refs. Any
  change must preserve every CSV value and avoid per-repository follow-up calls.
- Consider caching schema capabilities and accepted page size by endpoint and
  Sourcegraph version. Define safe invalidation before implementation.
