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

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use clap::{App, Arg};
use futures::TryStreamExt;
use ipfs_api::IpfsClient;
use serde_cbor;
use serde_json;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs::File;
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

// from https://stackoverflow.com/questions/28127165/how-to-convert-struct-to-u8
unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
	::std::slice::from_raw_parts((p as *const T) as *const u8, ::std::mem::size_of::<T>())
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
						.block_get(&resolved.path)
						.map_ok(|chunk| chunk.to_vec())
						.try_concat()
						.await
					{
						Ok(bytes) => {
							if verbosity > 1 {
								println!(
									"block_get {} done @ {} ms",
									&resolved.path,
									begintime.elapsed().unwrap().as_millis()
								);
							}
							let mut existing: BTreeMap<
								String,
								BTreeMap<String, serde_cbor::Value>,
							> = serde_cbor::from_slice(&bytes).unwrap();
							println!("existing {:?}", existing);
							for (key, value) in existing.iter_mut() {
								//~ let values_entry: &serde_cbor::Value = ;
								if let serde_cbor::Value::Bytes(b) = value.get("values").unwrap() {
									let mut bm = b.to_owned();
									// append the new value (made up here)
									bm.write_f32::<LittleEndian>(3.14).unwrap();
									println!("{} {:?}", key, bm);
									//~ let mut rdr = io::Cursor::new(bm);
									//~ println!("first is {:?}", rdr.read_f32::<LittleEndian>().unwrap());
									//~ println!("next is {:?}", rdr.read_f32::<LittleEndian>().unwrap());

									value
										.insert("values".to_string(), serde_cbor::Value::Bytes(bm));
									println!("{} {:?}", key, value);
								}
							}
							println!("updated {:?}", existing);
							let f = File::create("updated.cbor").unwrap();
							serde_cbor::to_writer(f, &existing)
								.expect("error generating cbor from updated record");

							let cursor = io::Cursor::new(serde_cbor::to_vec(&existing).unwrap());
							let response = client.block_put(cursor).await.expect("dag_put error");
							let cid = response.key;
							if verbosity > 1 {
								println!(
									"ipns {} {} {} -> {:?} @ {} ms",
									ipnskey,
									ipns_name,
									&resolved.path,
									cid,
									begintime.elapsed().unwrap().as_millis()
								);
							}

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
							client
								.name_publish(&cid, false, Some("12h"), None, Some(ipnskey))
								.await
								.expect("error publishing name");
							if verbosity > 1 {
								println!(
									"published {} @ {} ms",
									&cid,
									begintime.elapsed().unwrap().as_millis()
								);
							}
							return cid;
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
