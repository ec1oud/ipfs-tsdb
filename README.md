This (so far unnamed) is a prototype of an IPFS time-series database.

## Getting Started

1.  Create a json schema file describing the fields you want to store, e.g. at
    least

    ```json
    {
        "fields": {
            "_timestamp" : { 
                "encoding" : "cbor-bytes", 
                "type" : "u64" 
            },
            "temperature" : { 
                "encoding" : "cbor-bytes", 
                "type" : "f32" 
            } 
        }
    }
    ```
2.  `ipfs key gen my-database-name`
3.  `ipfs-tsdb-rust my-database-name create < my-schema.json`
4.  insert additional records, e.g. from a JSON object containing at least all
    the values from the schema: `curl http://10.x.x.x/j | jq .data |
    ipfs-tsdb-rust my-database-name insert`
5.  use the select command to read what has been stored, e.g. `ipfs-tsdb-rust
    my-database-name select --limit 10 _timestamp temperature`

The output of the select command is a markdown table.

The data columns are stored in separate DAG nodes in CBOR format, as an object
containing the CID of the next node, and a byte array that in turn contains the
numbers in little-endian format, so that it's ready for casting into a numeric
array.  The schema is stored in another DAG node and should not change after
initial creation. The root node is a DAG IPLD node pointing to the schema and
each of the data column head nodes.  It is republished via IPNS each time a new
data record is inserted.  Thus, publishing the data is a bit slow; but a client
anywhere on the net ought to be able to track the updates as they occur, a
client only needs to fetch the columns of interest, and those are kept as small
as possible.

