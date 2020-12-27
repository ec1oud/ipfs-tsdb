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
use chrono::prelude::*;
use clap::{App, Arg, SubCommand};
use env_logger;
use futures::TryStreamExt;
use ipfs_api::IpfsClient;
use log::{debug, info, trace};
use prettytable::{format, Row, Table};
use serde_json;
use std::collections::HashMap;
use std::io;
use std::time::{Duration, SystemTime};
use tokio;
use tokio::time::timeout;

type JsonMap = HashMap<String, serde_json::Value>;

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
			debug!("resolved {}", ipns_name);
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

#[tokio::main]
async fn select_json(ipnskey: &str, limit: i32, query: Vec<&str>) {
	debug!(
		"select from {} limit {:?} fields {:?}",
		ipnskey, limit, query
	);

	let client = IpfsClient::default();
	let resolved_path = resolve_root(&client, &ipnskey).await.unwrap();
	match get_node_json(&client, &resolved_path).await {
		Ok(all_records) => {
			// TODO if "select *" (empty query?) replace it with
			// for (key, value) in all_records.iter_mut() { ...
			let mut row_count: usize = 0; // = all_records.values() .len();
			let mut record_values = Vec::new(); //: Vec<serde_json::Value>;
			let mut title_row = Row::empty();
			for field_key in &query {
				if field_key == &TIMESTAMP_KEY {
					title_row.add_cell(cell!("timestamp (UTC)"));
				} else {
					title_row.add_cell(cell!(field_key));
				}
				let values = all_records
					.get(*field_key)
					.unwrap()
					.as_array()
					.unwrap()
					.to_vec();
				if row_count == 0 {
					row_count = values.len();
				}
				assert_eq!(row_count, values.len()); // values were missing during some inserts?
				if limit > -1 {
					record_values.push(values[(row_count - (limit as usize))..row_count].to_vec());
				} else {
					record_values.push(values);
				}
				//~ println!("{}: {:?} records", field_key, row_count);
				//~ println!("{}: {:?}", field_key, record_values[record_values.len()]);
			}
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
			if limit > -1 {
				row_count = limit as usize;
			}
			for r in 0..row_count {
				let mut row = Row::empty();
				for c in 0..record_values.len() {
					if query[c] == TIMESTAMP_KEY {
						let ts = record_values[c][r].as_i64().unwrap();
						let dt: chrono::DateTime<Utc> =
							DateTime::from_utc(NaiveDateTime::from_timestamp(ts, 0), Utc);
						row.add_cell(cell!(dt.format("%Y-%m-%d %H:%M")));
					} else {
						row.add_cell(cell!(record_values[c][r].to_string()));
					}
				}
				table.add_row(row);
			}
			table.printstd();
		}
		Err(e) => {
			eprintln!("error reading dag node: {}", e);
		}
	}
}

#[tokio::main]
async fn insert_from_json<R: io::Read>(ipnskey: &str, rdr: R) -> String {
	let begintime = SystemTime::now();
	let unixtime = serde_json::json!(
		match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
			Ok(n) => n.as_secs(),
			Err(_) => 0, // before 1970?!?
		}
	);

	let json_data: JsonMap = serde_json::from_reader(rdr).unwrap();
	debug!("given {:?}", json_data);

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
					let mut existing: JsonMap = serde_json::from_slice(&bytes).unwrap();
					for (key, value) in existing.iter_mut() {
						let new_value: &serde_json::Value = match key.as_str() {
							TIMESTAMP_KEY => &unixtime,
							_ => json_data.get(key).unwrap(),
						};
						trace!("{:?} {:?} <- {:?}", key, value, new_value);
						let vec = value.as_array_mut().unwrap();
						vec.push(new_value.clone());
					}
					let cursor = io::Cursor::new(serde_json::json!(existing).to_string());
					let response = client.dag_put(cursor).await.expect("dag_put error");
					let cid = response.cid.cid_string;
					debug!(
						"ipns {} {} -> {} @ {} ms",
						ipnskey,
						&resolved_path,
						cid,
						begintime.elapsed().unwrap().as_millis()
					);
					client.pin_add(&cid, false).await.expect("pin error");
					debug!("pinned @ {} ms", begintime.elapsed().unwrap().as_millis());
					let fut = client.name_publish(&cid, false, Some("12h"), None, Some(ipnskey));
					match timeout(Duration::from_secs(PUBLISH_TIMEOUT), fut).await {
						Ok(_res) => {
							// [2020-12-27T23:21:33Z INFO  ipfs_tsdb_rust] published bafyreic6cqei2aoxmfv47lioibm3dpqiqwv7ahuvxxa5ww4r2oul6ron4y @ 240776 ms
							// should have timed out
							info!(
								"published {} @ {} ms",
								&cid,
								begintime.elapsed().unwrap().as_millis()
							);
							let _ = client.pin_rm(&resolved_path, false).await;
							debug!(
								"unpinned old @ {} ms",
								begintime.elapsed().unwrap().as_millis()
							);
							client
								.block_rm(&resolved_path)
								.await
								.expect("error removing last");
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
	} else {
		eprintln!("only insert and select are supported so far");
	}
}
