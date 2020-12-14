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
use std::collections::HashMap;
use std::io;
use std::time::SystemTime;
use tokio;

type JsonMap = HashMap<String, serde_json::Value>;

#[tokio::main]
async fn insert_from_json<R: io::Read>(ipnskey: &str, rdr: R) -> String {
	let unixtime = serde_json::json!(
		match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
			Ok(n) => n.as_secs(),
			Err(_) => 0, // before 1970?!?
		}
	);

	let json_data: JsonMap = serde_json::from_reader(rdr).unwrap();
	println!("deserialized input = {:?}", json_data);

	let client = IpfsClient::default();

	match client.key_list().await {
		Ok(list) => {
			let ipns_name = &list
				.keys
				.iter()
				.find(|&keypair| keypair.name == ipnskey)
				.unwrap()
				.id[..];
			eprintln!("found ipns_name {:?}", ipns_name);
			match client.name_resolve(Some(ipns_name), true, false).await {
				Ok(resolved) => {
					eprintln!("{} existing record: {}", ipnskey, &resolved.path);
					match client
						.dag_get(&resolved.path)
						.map_ok(|chunk| chunk.to_vec())
						.try_concat()
						.await
					{
						Ok(bytes) => {
							println!("{}", String::from_utf8_lossy(&bytes[..]));
							let mut existing: JsonMap = serde_json::from_slice(&bytes).unwrap();
							println!("existing dag node {:?}", existing);
							for (key, value) in existing.iter_mut() {
								let new_value: &serde_json::Value = match key.as_str() {
									"_timestamp" => &unixtime,
									_ => json_data.get(key).unwrap(),
								};
								println!("{:?} {:?} <- {:?}", key, value, new_value);
								let vec = value.as_array_mut().unwrap();
								vec.push(new_value.clone());
							}
							println!("updated dag node {:?}", existing);
							let cursor = io::Cursor::new(serde_json::json!(existing).to_string());
							let response = client.dag_put(cursor).await.expect("dag_put error");
							let cid = response.cid.cid_string;
							client.pin_add(&cid, false).await.expect("pin error");
							client
								.pin_rm(&resolved.path, false)
								.await
								.expect("unpin error");
							client
								.block_rm(&resolved.path)
								.await
								.expect("error removing last");
							eprintln!("added {} removed {}", cid, &resolved.path);
							match client
								.name_publish(&cid, false, Some("12h"), None, Some(ipnskey))
								.await
							{
								Ok(publish) => {
									eprintln!(
										"published data {} to: /ipns/{}",
										&cid, &publish.name
									);
									return publish.name;
								}
								Err(e) => {
									eprintln!("error publishing name: {}", e);
									return cid.to_string();
								}
							};
						}
						Err(e) => {
							eprintln!("error reading dag node: {}", e);
							return "".to_string();
						}
					}
				}
				Err(e) => {
					eprintln!("error resolving {}: {}", ipnskey, e);
					return "".to_string();
				}
			}
		}
		Err(e) => {
			eprintln!("error getting keys: {}", e);
			return "".to_string();
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
				.index(3),
		)
		.arg(
			Arg::with_name("ipnskey")
				.help("IPNS key created with 'ipfs key gen' command")
				.required(true)
				.index(1),
		)
		.arg(
			Arg::with_name("query")
				.help("insert or select")
				.required(true)
				.index(2),
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

	let key = matches.value_of("ipnskey").unwrap();

	let result = match matches.value_of("query").unwrap() {
		"insert" => insert_from_json(key, io::stdin()),
		_ => "only insert is supported so far".to_string(),
	};
	println!("{}", result);
}
