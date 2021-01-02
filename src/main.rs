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

#[macro_use]
extern crate prettytable;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrono::prelude::*;
use clap::{App, Arg, SubCommand};
use env_logger;
use futures::TryStreamExt;
use ipfs_api::IpfsClient;
use log::{debug, info, trace};
use prettytable::{format, Row, Table};
use serde::{Deserialize, Serialize};
use serde_cbor;
use serde_json;
use std::collections::BTreeMap;
use std::io;
use std::time::{Duration, SystemTime};
use tokio;
use tokio::time::timeout;

type JsonMap = BTreeMap<String, serde_json::Value>;
type CborMap = BTreeMap<String, serde_cbor::Value>;

#[derive(Serialize, Deserialize, Debug)]
struct RootNode {
	schema: String,
	column_heads: BTreeMap<String, String>,
}

const PUBLISH_TIMEOUT: u64 = 50;
const TIMESTAMP_KEY: &str = "_timestamp";

async fn resolve_root(
	client: &IpfsClient,
	ipnskey: &str,
) -> Result<String, ipfs_api::response::Error> {
	let key_list = client.key_list().await?;
	let ipns_name = &key_list
		.keys
		.iter()
		.find(|&keypair| keypair.name == ipnskey)
		.unwrap()
		.id[..];
	match client.name_resolve(Some(ipns_name), true, false).await {
		Ok(resolved) => {
			debug!("resolved {}: {}", ipns_name, resolved.path.to_string());
			Ok(resolved.path.to_string())
		}
		Err(e) => Err(e),
	}
}

async fn get_node_json(
	client: &IpfsClient,
	path: &str,
) -> Result<JsonMap, ipfs_api::response::Error> {
	match client
		.dag_get(&path)
		.map_ok(|chunk| chunk.to_vec())
		.try_concat()
		.await
	{
		Ok(bytes) => {
			debug!("dag_get {} done", &path);
			Ok(serde_json::from_slice(&bytes).unwrap())
		}
		Err(e) => Err(e),
	}
}

async fn publish_root(client: &IpfsClient, ipnskey: &str, cid: &str) {
	let fut = client.name_publish(&cid, false, Some("12h"), None, Some(ipnskey));
	match timeout(Duration::from_secs(PUBLISH_TIMEOUT), fut).await {
		Ok(_res) => {
			info!("published {}", &cid);
		}
		Err(e) => eprintln!("error or timeout while publishing {}: {:?}", &cid, e),
	}
}

#[tokio::main]
async fn select_json(ipnskey: &str, limit: i32, query: Vec<&str>) {
	debug!(
		"select from {} limit {:?} fields {:?}",
		ipnskey, limit, query
	);

	let client = IpfsClient::default();
	let resolved_path = resolve_root(&client, &ipnskey).await.unwrap();
	let root_bytes = client
		.dag_get(&resolved_path)
		.map_ok(|chunk| chunk.to_vec())
		.try_concat()
		.await
		.unwrap();
	let root: RootNode = serde_json::from_slice(&root_bytes).unwrap();
	trace!("{:#?}", root);
	let schema: JsonMap = get_node_json(&client, &root.schema).await.unwrap();
	let schema_fields = &schema["fields"];
	let mut field_readers = vec![];
	let mut title_row = Row::empty();
	for field_name in &query {
		let col_head_cid = root.column_heads.get(&field_name.to_string()).unwrap();
		let col_bytes = client
			.block_get(&col_head_cid)
			.map_ok(|chunk| chunk.to_vec())
			.try_concat()
			.await
			.unwrap();
		let col: CborMap = serde_cbor::from_slice(&col_bytes).unwrap();
		if let serde_cbor::Value::Bytes(data_bytes) = col.get("data").unwrap().to_owned() {
			debug!(
				"{}: '{}' has {} bytes",
				field_name,
				col_head_cid,
				data_bytes.len()
			);
			field_readers.push(io::Cursor::new(data_bytes));
		}
		if field_name == &TIMESTAMP_KEY {
			title_row.add_cell(cell!("timestamp (UTC)"));
		} else {
			title_row.add_cell(cell!(field_name));
		}
	}

	// TODO if "select *" (empty query?) get all fields
	let mut table = Table::new();
	let format = format::FormatBuilder::new()
		.column_separator('|')
		.borders('|')
		.separators(
			&[format::LinePosition::Title],
			format::LineSeparator::new('-', '|', '|', '|'),
		)
		.padding(1, 1)
		.build();
	table.set_format(format);
	table.set_titles(title_row);
	let mut cur_row: i32 = 0;

	// emit rows until the data runs out or we hit the limit
	loop {
		let mut row = Row::empty();
		let mut eof = false;
		for c in 0..field_readers.len() {
			let field_type = schema_fields[query[c]]["type"].as_str().unwrap();
			let rdr = &mut field_readers[c];
			match field_type {
				"u64" => {
					match query[c] {
						TIMESTAMP_KEY => {
							let ts = rdr.read_u64::<LittleEndian>();
							// TODO do error checking more elegantly
							if ts.is_ok() {
								let dt: chrono::DateTime<Utc> = DateTime::from_utc(
									NaiveDateTime::from_timestamp(ts.unwrap() as i64, 0),
									Utc,
								);
								row.add_cell(cell!(dt.format("%Y-%m-%d %H:%M")));
							} else {
								eof = true;
								break;
							}
						}
						_ => {
							let val = rdr.read_u64::<LittleEndian>().unwrap();
							row.add_cell(cell!(val.to_string()));
						}
					}
				}
				"u32" => {
					let val = rdr.read_u32::<LittleEndian>().unwrap();
					row.add_cell(cell!(val.to_string()));
				}
				"u16" => {
					let val = rdr.read_u16::<LittleEndian>().unwrap();
					row.add_cell(cell!(val.to_string()));
				}
				"u8" => {
					let val = rdr.read_u8().unwrap();
					row.add_cell(cell!(val.to_string()));
				}
				"i64" => {
					let val = rdr.read_i64::<LittleEndian>().unwrap();
					row.add_cell(cell!(val.to_string()));
				}
				"i32" => {
					let val = rdr.read_i32::<LittleEndian>().unwrap();
					row.add_cell(cell!(val.to_string()));
				}
				"i16" => {
					let val = rdr.read_i16::<LittleEndian>().unwrap();
					row.add_cell(cell!(val.to_string()));
				}
				"i8" => {
					let val = rdr.read_i8().unwrap();
					row.add_cell(cell!(val.to_string()));
				}
				"f64" => {
					let val = rdr.read_f64::<LittleEndian>().unwrap();
					row.add_cell(cell!(val.to_string()));
				}
				_ => {
					let val = rdr.read_f32::<LittleEndian>().unwrap();
					row.add_cell(cell!(val.to_string()));
				}
			};
		}
		if eof {
			break;
		}
		table.add_row(row);
		cur_row += 1;
		if limit > -1 && cur_row > limit {
			break;
		}
	}
	table.printstd();
}

#[tokio::main]
async fn create<R: io::Read>(ipnskey: &str, schema_rdr: R) {
	let schema: JsonMap = serde_json::from_reader(schema_rdr).unwrap();

	let client = IpfsClient::default();
	let cursor = io::Cursor::new(serde_json::json!(schema).to_string());
	let response = client.dag_put(cursor).await.expect("dag_put error");
	let schema_cid = response.cid.cid_string;

	info!("schema -> {}", schema_cid);

	let mut root = RootNode {
		schema: schema_cid,
		column_heads: BTreeMap::default(),
	};
	let fields = schema.get("fields").unwrap().as_object().unwrap();
	for (key, value) in fields.iter() {
		debug!("{:?} {:?}", key, value);
		root.column_heads.insert(key.to_string(), "".to_string());
	}
	let root_json = serde_json::to_string(&root).unwrap();
	let response = client
		.dag_put(io::Cursor::new(root_json))
		.await
		.expect("dag_put error");
	let root_cid = response.cid.cid_string;
	info!("root {:?} -> {}", root, root_cid);
	publish_root(&client, ipnskey, &root_cid).await;
}

#[tokio::main]
async fn insert_from_json<R: io::Read>(ipnskey: &str, rdr: R) -> String {
	let begintime = SystemTime::now();
	let unixtime = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
		Ok(n) => n.as_secs(),
		Err(_) => 0, // before 1970?!?
	};

	let mut json_data: JsonMap = serde_json::from_reader(rdr).unwrap();
	trace!("given {:#?}", json_data);
	if !json_data.contains_key(TIMESTAMP_KEY) {
		json_data.insert(TIMESTAMP_KEY.to_string(), serde_json::json!(unixtime));
	}

	let client = IpfsClient::default();

	match resolve_root(&client, &ipnskey).await {
		Ok(resolved_path) => {
			match client
				.dag_get(&resolved_path)
				.map_ok(|chunk| chunk.to_vec())
				.try_concat()
				.await
			{
				Ok(bytes) => {
					let mut root: RootNode = serde_json::from_slice(&bytes).unwrap();
					trace!("{:#?}", root);
					let schema: JsonMap = get_node_json(&client, &root.schema).await.unwrap();
					let schema_fields = &schema["fields"];
					let old_column_heads = root.column_heads.clone();
					for (field_name, col_head_cid) in &old_column_heads {
						let field_type = schema_fields[field_name]["type"].as_str().unwrap();
						// TODO respect the encoding too
						let mut existing = CborMap::default(); // { data: b'', next: "cid" }
						let mut data_bytes = vec![];
						if col_head_cid.is_empty() {
							existing.insert(
								"next".to_string(),
								serde_cbor::Value::Text("".to_string()),
							);
						} else {
							let bytes = client
								.block_get(&col_head_cid)
								.map_ok(|chunk| chunk.to_vec())
								.try_concat()
								.await
								.unwrap();
							existing = serde_cbor::from_slice(&bytes).unwrap();
							if let serde_cbor::Value::Bytes(b) = existing.get("data").unwrap() {
								data_bytes = b.to_vec();
							}
						}
						debug!(
							"{}: '{}' had {} bytes, type {}",
							field_name,
							col_head_cid,
							data_bytes.len(),
							field_type
						);
						match field_type {
							"u64" => {
								let datum = json_data.get(field_name).unwrap().as_u64().unwrap();
								data_bytes.write_u64::<LittleEndian>(datum).unwrap();
							}
							"u32" => {
								let datum =
									json_data.get(field_name).unwrap().as_u64().unwrap() as u32;
								data_bytes.write_u32::<LittleEndian>(datum).unwrap();
							}
							"u16" => {
								let datum =
									json_data.get(field_name).unwrap().as_u64().unwrap() as u16;
								data_bytes.write_u16::<LittleEndian>(datum).unwrap();
							}
							"u8" => {
								let datum =
									json_data.get(field_name).unwrap().as_u64().unwrap() as u8;
								data_bytes.write_u8(datum).unwrap();
							}
							"i64" => {
								let datum = json_data.get(field_name).unwrap().as_i64().unwrap();
								data_bytes.write_i64::<LittleEndian>(datum).unwrap();
							}
							"i32" => {
								let datum =
									json_data.get(field_name).unwrap().as_i64().unwrap() as i32;
								data_bytes.write_i32::<LittleEndian>(datum).unwrap();
							}
							"i16" => {
								let datum =
									json_data.get(field_name).unwrap().as_i64().unwrap() as i16;
								data_bytes.write_i16::<LittleEndian>(datum).unwrap();
							}
							"i8" => {
								let datum =
									json_data.get(field_name).unwrap().as_i64().unwrap() as i8;
								data_bytes.write_i8(datum).unwrap();
							}
							"f64" => {
								let datum = json_data.get(field_name).unwrap().as_f64().unwrap();
								data_bytes.write_f64::<LittleEndian>(datum).unwrap();
							}
							_ => {
								let datum =
									json_data.get(field_name).unwrap().as_f64().unwrap() as f32;
								data_bytes.write_f32::<LittleEndian>(datum).unwrap();
							}
						};
						existing.insert("data".to_string(), serde_cbor::Value::Bytes(data_bytes));
						let cursor = io::Cursor::new(serde_cbor::to_vec(&existing).unwrap());
						let response = client.block_put(cursor).await.expect("block_put error");
						let cid = response.key;
						trace!("{} {} {:?}", field_name, cid, existing);
						root.column_heads.insert(field_name.to_string(), cid);
					}
					let cursor = io::Cursor::new(serde_json::json!(root).to_string());
					let response = client.dag_put(cursor).await.expect("dag_put error");
					let cid = response.cid.cid_string;
					debug!(
						"ipns {} {} -> {} @ {} ms",
						ipnskey,
						&resolved_path,
						cid,
						begintime.elapsed().unwrap().as_millis()
					);
					let fut = client.name_publish(&cid, false, Some("12h"), None, Some(ipnskey));
					match timeout(Duration::from_secs(PUBLISH_TIMEOUT), fut).await {
						Ok(_res) => {
							info!(
								"published {} @ {} ms",
								&cid,
								begintime.elapsed().unwrap().as_millis()
							);
							// TODO deal with adding and removing pins too? (that's a lot of them though)
							client
								.block_rm(&resolved_path)
								.await
								.expect("error removing last root");
							for (field_name, col_head_cid) in &old_column_heads {
								client.block_rm(&col_head_cid).await.expect(
									&("error removing data block for ".to_owned() + &field_name),
								);
							}
							debug!(
								"block_rm(old) done @ {} ms",
								begintime.elapsed().unwrap().as_millis()
							);
						}
						Err(e) => eprintln!(
							"error or timeout while publishing {} @ {} ms: {:?}",
							&cid,
							begintime.elapsed().unwrap().as_millis(),
							e
						),
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

fn main() {
	env_logger::init();

	let matches = App::new("iptsdb")
		.version("0.1")
		.about("client for an IPFS-based time-series database")
		.arg(
			Arg::with_name("ipnskey")
				.help("IPNS key created with 'ipfs key gen' command")
				.required(true)
				.index(1),
		)
		.arg(
			Arg::with_name("format")
				.short("f")
				.long("format")
				.value_name("fmt")
				.help("Sets the input or output format")
				.takes_value(true),
		)
		.arg(
			Arg::with_name("input")
				.short("i")
				.long("input")
				.value_name("file")
				.help("Sets the input file (default is stdin)"),
		)
		.arg(
			Arg::with_name("output")
				.short("o")
				.long("output")
				.value_name("file")
				.help("Sets the output file (default is stdout)"),
		)
		.subcommand(
			SubCommand::with_name("create").about("Create a new database from a JSON schema"),
		)
		.subcommand(
			SubCommand::with_name("insert").about("Insert one record from a piped-in JSON object"),
		)
		.subcommand(
			SubCommand::with_name("select")
				.about("Select records with a query")
				.arg(
					Arg::with_name("limit")
						.long("limit")
						.value_name("count")
						.help("Limits the number of records returned"),
				)
				.arg(
					Arg::with_name("query")
						.help("list of fields")
						//~ .required(true)
						//~ .min_values(1)
						.multiple(true),
				),
		)
		.get_matches();

	let _fmt = matches.value_of("format").unwrap_or("json");

	if let Some(matches) = matches.value_of("input") {
		println!("input file: {} (not supported yet)", matches);
	}
	if let Some(matches) = matches.value_of("output") {
		println!("output file: {} (not supported yet)", matches);
	}

	let key = matches.value_of("ipnskey").unwrap();

	if let Some(matches) = matches.subcommand_matches("select") {
		let limit = matches
			.value_of("limit")
			.unwrap_or("-1")
			.parse::<i32>()
			.unwrap();
		let query: Vec<&str> = matches.values_of("query").unwrap().collect();
		select_json(key, limit, query);
	} else if let Some(_matches) = matches.subcommand_matches("insert") {
		insert_from_json(key, io::stdin());
	} else if let Some(_matches) = matches.subcommand_matches("create") {
		create(key, io::stdin());
	} else {
		eprintln!("only create, insert and select are supported so far");
	}
}
