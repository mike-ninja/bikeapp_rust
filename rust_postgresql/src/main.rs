// use postgres::{Client, Error, NoTls};
mod ddb;

use std::process;

fn main() {
    let db_client = ddb::connect_db();
    match db_client {
        Ok(c) => c,
        Err(_e) => {
            eprintln!("DB Error on setup of database");
            process::exit(1);
        },
    };
}
