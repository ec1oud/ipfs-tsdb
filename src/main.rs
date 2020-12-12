/****************************************************************************
**
** Copyright (C) 2020 Shawn Rutledge
**
** This file is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public License
** version 3 as published by the Free Software Foundation
** and appearing in the file LICENSE included in the packaging
** of this file.
**
** This code is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
** GNU General Public License for more details.
**
****************************************************************************/

use clap::{App, Arg};
use futures::TryStreamExt;
use ipfs_api::IpfsClient;
use serde_json;
use std::any::type_name;
use std::io;
use std::io::Cursor;
use tokio;

#[tokio::main]
async fn insert_from_json<R: io::Read>(rdr: R) {
    let json_data: serde_json::Value = serde_json::from_reader(rdr).unwrap();
    println!("deserialized input = {:?}", json_data);

    println!("reconstructed input {}", json_data.to_string());

    let client = IpfsClient::default();
    let dag_node = io::Cursor::new(json_data.to_string());

    let response = client
        .dag_put(dag_node)
        .await
        .expect("error adding dag node");

    let cid = response.cid.cid_string;
    eprintln!("added: {:?}", cid);

    match client
        .dag_get(&cid)
        .map_ok(|chunk| chunk.to_vec())
        .try_concat()
        .await
    {
        Ok(bytes) => {
            println!("{}", String::from_utf8_lossy(&bytes[..]));
            let deserialized: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            println!("deserialized dag node = {:?}", deserialized);
        }
        Err(e) => {
            eprintln!("error reading dag node: {}", e);
        }
    }
}

fn main() {
    let matches = App::new("iptsdb")
        .version("0.1")
        .about("client for an IPFS-based time-series database")
        .arg(
            Arg::with_name("format")
                .short("f")
                .long("format")
                .value_name("fmt")
                .help("Sets the input or output format")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("file")
                .help("Sets the file to use (default is stdin/stdout)")
                .index(2),
        )
        .arg(
            Arg::with_name("query")
                .help("insert or select")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .get_matches();

    let fmt = matches.value_of("format").unwrap_or("json");
    println!("Value for fmt: {}", fmt);

    if let Some(matches) = matches.value_of("file") {
        println!("Using file: {}", matches);
    }

    let _verbosity = matches.occurrences_of("v");

    match matches.value_of("query").unwrap() {
        "insert" => insert_from_json(io::stdin()),
        _ => println!("only insert is supported for now"),
    }
}
