package main

import (
	"bytes"
	//~ "bufio"
	//~ "context"
	//~ "encoding/json"
	"encoding/binary"
    "fmt"
    //~ "math"
    //~ "io"
	"io/ioutil"
	//~ "log"
	"os"
	//~ "path/filepath"
	//~ "strings"
	//~ "sync"

	//~ config "github.com/ipfs/go-ipfs-config"
	//~ files "github.com/ipfs/go-ipfs-files"
	//~ ipld "github.com/ipfs/go-ipld-format"
	//~ dag "github.com/ipfs/go-merkledag"
	//~ libp2p "github.com/ipfs/go-ipfs/core/node/libp2p"
	//~ icore "github.com/ipfs/interface-go-ipfs-core"
	//~ icorepath "github.com/ipfs/interface-go-ipfs-core/path"
	//~ peerstore "github.com/libp2p/go-libp2p-peerstore"
	//~ ma "github.com/multiformats/go-multiaddr"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	ipldcrud "github.com/0zAND1z/ipldcrud"
	shell "github.com/ipfs/go-ipfs-api"

	//~ "github.com/ipfs/go-ipfs/core"
	//~ "github.com/ipfs/go-ipfs/core/coreapi"
	//~ "github.com/ipfs/go-ipfs/plugin/loader" // This package is needed so that all the preloaded plugins are loaded automatically
	//~ "github.com/ipfs/go-ipfs/repo/fsrepo"
	//~ "github.com/libp2p/go-libp2p-core/peer"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	//~ "github.com/ipld/go-ipld-prime/fluent"
)

var linkBuilder = cidlink.LinkBuilder{cid.Prefix{
	Version:  1,    // Usually '1'.
	Codec:    0x71, // dag-cbor as per multicodec
	MhType:   0x15, // sha3-384 as per multihash
	MhLength: 48,   // sha3-384 hash has a 48-byte sum.
}}

// TODO try ipldcbor.NodeBuilder ?

var sh = shell.NewShell("http://localhost:5001")

func newDagNodeFromJSONFile(path string) (ipld.Node, error) {
	//~ data, err := ioutil.ReadFile(path)
    file, err := os.Open(path)
    if err != nil {
        fmt.Println("File reading error", err)
        return nil, err
    }

    // similar to github.com/ipfs/go-ipld-prime/examples_test.go
	np := basicnode.Prototype.Any // Pick a style for the in-memory data.
	nb := np.NewBuilder()         // Create a builder.
	dagjson.Decoder(nb, file)     // Hand the builder to decoding -- decoding will fill it in!
	n := nb.Build()               // Call 'Build' to get the resulting Node.  (It's immutable!)

	//~ fmt.Printf("the data decoded was a %s kind\n", n.Kind())
	fmt.Printf("the length of the node is %d\n", n.Length())

    return n, nil
}

func decodeCBORDagNode(cid string) (ipld.Node, error) {
	cborData, err := sh.BlockGet(cid);
    if err != nil {
        fmt.Println("BlockGet error", err)
        return nil, err
    }

	builder := basicnode.Prototype.Any.NewBuilder()
	dagcbor.Decoder(builder, bytes.NewReader(cborData))
	n := builder.Build()

	fmt.Printf("the data decoded was a %s kind\n", n.ReprKind())
	fmt.Printf("the length of %s is %d\n", cid, n.Length())

	//~ dagjson.Encoder(n, os.Stdout) // fails with raw-array cbor data

    return n, nil
}

func putJSONFromFile(path string) (string, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}
	cid, err := sh.DagPut(data, "json", "cbor")
	if err != nil {
		return "", err
	}
	return cid, nil
}

func getDagNode(sh *shell.Shell, ref string) (out interface{}, err error) {
	err = sh.DagGet(ref, &out)
	return
}

func float32FromBytes(b []byte) []float32 {
    floatLen := len(b) / 4
    ret := make([]float32, floatLen, floatLen)
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.LittleEndian, &ret)
	if err != nil {
		fmt.Println(err)
	}
	return ret
}

/// -------

func main() {
	cid, err := putJSONFromFile("uradmonitor-schema.json")

	//~ keyValueMap := make(map[string]interface{})
	//~ keyValueMap[inputKey] = inputValue
	//~ entryJSON, err := json.Marshal(keyValueMap)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(cid)
	}

	fmt.Printf("read fields: ")
	res, err := ipldcrud.Get(sh, cid, "fields")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(res)

	data, err := ioutil.ReadFile("headRecord.cbor")
	if err != nil {
		fmt.Println(err)
	}
	cid, err = sh.DagPut(data, "cbor", "cbor")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(cid)
	}

	// res, err = ipldcrud.Get(sh, cid, "fields")
	//~ res, err = getDagNode(sh, cid + "/fields/values")
	//~ if err != nil {
		//~ fmt.Println(err)
	//~ } else {
		//~ fmt.Println(res)
	//~ }

	// can we parse the cbor?

	//~ res, err = sh.BlockGet(cid + "/fields/values")
	//~ if err != nil {
		//~ fmt.Println(err)
	//~ } else {
		//~ fmt.Println(res)
	//~ }

	node, err := decodeCBORDagNode(cid)
	fieldsMap, err := node.LookupByString("fields")
	fmt.Println(fieldsMap)
	values, err := fieldsMap.LookupByString("values")
	fmt.Println(values)
	bytes, err := values.AsBytes() // https://godoc.org/github.com/ipld/go-ipld-prime#Node : returns []byte
	fmt.Println(bytes)
	fmt.Println(float32FromBytes(bytes))
}
