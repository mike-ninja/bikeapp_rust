use postgres::{Client, NoTls};
use csv::Reader;
use std::error::Error;

pub fn connect_db() -> Result<Client, Box<dyn Error>> {
    let mut client = Client::connect(
        "postgresql://db_solita:db_password@localhost:5243/postgres",
        NoTls,
    )?;
    println!("DB Connection succesful");
    create_if_not_exist(&mut client)?;
    update_table(&mut client)?;
    Ok(client)
}

fn create_if_not_exist(client: &mut Client) -> Result<(), Box<dyn Error>> {
    client.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS trips (
            id              SERIAL PRIMARY KEY,
            departure       VARCHAR(255),
            return          VARCHAR(255),
            departure_id    VARCHAR(255),
            departure_name  VARCHAR(255),
            return_id       VARCHAR(255),
            return_name     VARCHAR(255),
            distance        INT,
            duration        INT
        )
        ",
    )?;
    println!("DB Table exist");
    Ok(())
}

fn update_table(client: &mut Client) -> Result<(), Box<dyn Error>> {
    // Build the CSV reader and iterate over each record.
    let csv_file = vec!["csv_files/2021-05.csv"];

    for file_path in csv_file {
        let rdr = Reader::from_path(file_path);
        for result in rdr.unwrap().records() {
            let record = result?;
            let distance = record.get(6).unwrap().parse::<i32>().unwrap();
            let duration = record.get(7).unwrap().parse::<i32>().unwrap();
            
            if distance >= 10 && duration >= 10 {
                client.execute(
                    "INSERT INTO trips (departure, return, departure_id, departure_name, return_id, return_name, distance, duration) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                    &[&record.get(0).unwrap(), &record.get(1).unwrap(), &record.get(2).unwrap(), &record.get(3).unwrap(), &record.get(4).unwrap(), &record.get(5).unwrap(), &distance, &duration],
                )?;
            }
            // let trip = Trip::new(record);
            // println!("-> {:?}", trip);
            // println!("-> {}", record.get(0).unwrap());
            //
            // Check if       e
        }
    }
    for row in client.query("SELECT * FROM trips", &[])? {
        // println!("{:?}", row);
        let distance: i32 = row.get(7);
        let duration: i32 = row.get(8);
        println!(
            "trip: Distance {} Duration {}",
            distance, duration,
        );
    };
    println!("Updating table succesful");
    Ok(())
}

