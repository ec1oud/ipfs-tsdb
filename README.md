This (so far unnamed) is a prototype of an IPFS time-series database.

## Getting Started

1.  Create a prototype json file with an object containing the fields you want
    to store, as arrays containing the first values, e.g. `{"_timestamp":[1607926339],"fieldname":[0]}`
2.  `ipfs dag put proto.json` and make a note of the CID
3.  `ipfs key gen my-database-name`
4.  `ipfs name publish -k my-database-name bafy...` (using the CID of the first
    record)
5.  now you can use the tool to insert additional records, e.g. `curl
    http://10.x.x.x/j | jq .data | ipfs-tsdb-rust my-database-name insert`
