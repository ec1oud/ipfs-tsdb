# todo

- [ ] ensure sorting by timestamp (insert at right place if the datapoint is a
  historical one)
- [ ] support string fields
- [ ] support fixed-point fields
- [ ] deal with initial creation better: `{ "field": 1 } -> { "field": [1] }`
- [ ] json schema?
- [ ] csv both for initial creation and for updates

# deferred

- [ ] store values in contiguous plain arrays rather than cbor arrays

# done

- [x] mvp
- [x] remove old dag node after inserting new one
- [x] solve pinning (ensure the database survives ipfs repo gc)
