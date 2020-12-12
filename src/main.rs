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

use futures::TryStreamExt;
use ipfs_api::IpfsClient;
use serde_json::Value;
use std::any::type_name;
use std::io::Cursor;
use tokio;

fn type_of<T>(_: T) -> &'static str {
    type_name::<T>()
}

#[tokio::main]
async fn main() {
    let client = IpfsClient::default();
    let data = r#"{"hello":423453343}"#;
    let dag_node = Cursor::new(data);
    println!("{}", type_of(data));

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
            let deserialized: Value = serde_json::from_slice(&bytes).unwrap();
            println!("deserialized = {:?}", deserialized);
        }
        Err(e) => {
            eprintln!("error reading dag node: {}", e);
        }
    }
}
