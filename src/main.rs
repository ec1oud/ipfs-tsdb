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

use bytes::BufMut;
use clap::{App, Arg};
use futures::TryStreamExt;
use ipfs_api::IpfsClient;
use serde_json;
use std::collections::HashMap;
use std::io;
use std::time::SystemTime;
use tokio;

type JsonMap = HashMap<String, serde_json::Value>;

static mut VERBOSITY: u64 = 0;

#[tokio::main]
async fn create<R: io::Read>(ipnskey: &str, schema_rdr: R) -> String {
	let begintime = SystemTime::now();
	let verbosity = unsafe { VERBOSITY };

	let schema: JsonMap = serde_json::from_reader(schema_rdr).unwrap();
	if verbosity > 1 {
		println!("given schema {:?}", schema);
	}

	for (key, value) in schema.iter() {
		if verbosity > 1 {
			println!("{:?} {:?}", key, value);
		}
	}

	return "".to_string();
}

#[tokio::main]
async fn insert_from_json<R: io::Read>(ipnskey: &str, rdr: R) -> String {
	let begintime = SystemTime::now();
	let verbosity = unsafe { VERBOSITY };
	let unixtime: u64 = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
		Ok(n) => n.as_secs(),
		Err(_) => 0, // before 1970?!?
	};

	let json_data: JsonMap = serde_json::from_reader(rdr).unwrap();
	if verbosity > 1 {
		println!("given {:?}", json_data);
	}

	let client = IpfsClient::default();

	match client.key_list().await {
		Ok(list) => {
			let ipns_name = &list
				.keys
				.iter()
				.find(|&keypair| keypair.name == ipnskey)
				.unwrap()
				.id[..];
			match client.name_resolve(Some(ipns_name), true, false).await {
				Ok(resolved) => {
					if verbosity > 1 {
						println!(
							"resolved {} after {} ms",
							ipns_name,
							begintime.elapsed().unwrap().as_millis()
						);
					}
					match client
						.dag_get(&resolved.path)
						.map_ok(|chunk| chunk.to_vec())
						.try_concat()
						.await
					{
						Ok(bytes) => {
							if verbosity > 1 {
								println!(
									"dag_get {} done @ {} ms",
									&resolved.path,
									begintime.elapsed().unwrap().as_millis()
								);
							}
							let mut existing: JsonMap = serde_json::from_slice(&bytes).unwrap();
							for (key, value) in existing.iter_mut() {
								let values_entry = value.get("values").unwrap();
								let mut buf = vec![];
								buf.put(values_entry.as_str().unwrap());
								match key.as_str() {
									"_timestamp" => buf.put_u64_le(unixtime),
									_ => {
										//~ let v: f32 = json_data.get(key).unwrap().as_f64().unwrap() as f32;
										let v = json_data.get(key).unwrap().as_f64().unwrap();
										buf.put_f64_le(v);
									}
								};
								let s = unsafe { String::from_utf8_unchecked(buf) };
								value
									.as_object_mut()
									.unwrap()
									.insert("values".to_string(), serde_json::json!(s));
							}

							let cursor = io::Cursor::new(serde_json::json!(existing).to_string());
							let response = client.dag_put(cursor).await.expect("dag_put error");
							let cid = response.cid.cid_string;
							if verbosity > 1 {
								println!(
									"ipns {} {} {} -> {} @ {} ms",
									ipnskey,
									ipns_name,
									&resolved.path,
									cid,
									begintime.elapsed().unwrap().as_millis()
								);
							}
							return "".to_string();
							//~ client.pin_add(&cid, false).await.expect("pin error");
							//~ if verbosity > 1 {
							//~ println!(
							//~ "pinned @ {} ms",
							//~ begintime.elapsed().unwrap().as_millis()
							//~ );
							//~ }
							//~ let _ = client.pin_rm(&resolved.path, false).await;
							//~ if verbosity > 1 {
							//~ println!(
							//~ "unpinned old @ {} ms",
							//~ begintime.elapsed().unwrap().as_millis()
							//~ );
							//~ }
							//~ client
							//~ .block_rm(&resolved.path)
							//~ .await
							//~ .expect("error removing last");
							//~ if verbosity > 1 {
							//~ println!(
							//~ "block_rm(old) done @ {} ms",
							//~ begintime.elapsed().unwrap().as_millis()
							//~ );
							//~ }
							//~ client
							//~ .name_publish(&cid, false, Some("12h"), None, Some(ipnskey))
							//~ .await
							//~ .expect("error publishing name");
							//~ if verbosity > 1 {
							//~ println!(
							//~ "published {} @ {} ms",
							//~ &cid,
							//~ begintime.elapsed().unwrap().as_millis()
							//~ );
							//~ }
							//~ return cid;
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
				.help("create, insert or select")
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

	//~ let fmt = matches.value_of("format").unwrap_or("json");
	//~ println!("Value for fmt: {}", fmt);

	if let Some(matches) = matches.value_of("file") {
		println!("Using file: {}", matches);
	}

	unsafe {
		VERBOSITY = matches.occurrences_of("v");
	}

	let key = matches.value_of("ipnskey").unwrap();

	let _result = match matches.value_of("query").unwrap() {
		"insert" => insert_from_json(key, io::stdin()),
		"create" => create(key, io::stdin()),
		_ => "only create and insert are supported so far".to_string(),
	};
	//~ println!("{}", result);
}
