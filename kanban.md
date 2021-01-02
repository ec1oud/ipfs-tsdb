# todo

- [ ] allow a remote client to read (select) using the IPNS hash rather than the
  human-readable ipns key name
- [ ] ensure sorting by timestamp (insert at right place if the datapoint is a
  historical one)
- [ ] track "lost" blocks in a file, aka most recent blocks
- [ ] maybe avoid ipns query at all on insertion, just use the most-recent-blocks
  file?
- [ ] support string fields
- [ ] support fixed-point fields
- [ ] break up into multiple blocks (it doesn't look like chunking happens
  automatically with plain dag nodes, but if there's a limit it seems to be >
  1MB so far; I wonder if it just gets slow with big blocks)
- [ ] start wtih more query options: time ranges, aggregations, sorting etc.
- [ ] csv both for initial creation and for updates
- [ ] update tokio dependency (causes trouble currently)
- [ ] support delta data types (e.g. store timestamp increment in 16 bits instead
  of absolute timestamp in 64 bits)

# done

- [x] mvp
- [x] remove old dag node after inserting new one
- [x] solve pinning (ensure the database survives ipfs repo gc)
- [x] deal with initial creation better: { "field": 1 } -> { "field": [1] }
- [x] json schema; specify field data type, new block frequency (by size or by
  timespan), database format version, etc.
- [x] separate root node (updated often), but put schema into a separate DAG node
- [x] store values in contiguous plain arrays rather than cbor arrays
