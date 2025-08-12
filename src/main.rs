mod sqlite;
use sqlite::main as sqlite;
use std::{fs, io::Read, time::Instant};

use tytodb_client::{
    BOOL, INT, ToAlbaAlbaTypes, U_HUGE_INT, alba,
    handler::{BatchBuilder, CreateContainerBuilder, CreateRowBuilder, DeleteContainerBuilder},
};

const BATCH_SIZE: i32 = 10;
const STEPS: i32 = 10;

fn tytodb() {
    let password: [u8; 32] = {
        let mut f =
            fs::File::open(format!("{}/TytoDB/.secret", std::env::var("HOME").unwrap())).unwrap();
        let mut b = [0u8; 32];
        f.read_exact(&mut b).unwrap();
        b
    };

    let client = tytodb_client::client_thread::Client::connect("127.0.0.1:4287", password).unwrap();

    let _ = client.execute(
        DeleteContainerBuilder::new()
            .put_container(String::from("test_container"))
            .finish()
            .unwrap(),
    );
    println!(
        "::\tBENCHMARK TYTODB\n - scale: {}\n - range: 1-{}\n - schema: id,number,boolean\n",
        STEPS, BATCH_SIZE
    );

    client
        .execute(
            CreateContainerBuilder::new()
                .put_container(String::from("test_container"))
                .insert_header(String::from("id"), U_HUGE_INT)
                .insert_header(String::from("number"), INT)
                .insert_header(String::from("boolean"), BOOL)
                .finish()
                .unwrap(),
        )
        .unwrap();

    let mut v = 0u128;
    for i in 1..=STEPS {
        let t = Instant::now();
        for _ in 1..=(10_i128.pow(i as u32) / BATCH_SIZE as i128) {
            let mut batc = BatchBuilder::new();
            batc = batc.transaction(true);
            for _ in 1..=BATCH_SIZE {
                v += 1;
                batc = batc.push(
                    CreateRowBuilder::new()
                        .put_container(String::from("test_container"))
                        .insert_value(String::from("id"), alba!(v))
                        .insert_value(String::from("number"), alba!(i.clone()))
                        .insert_value(String::from("boolean"), alba!(true)),
                );
            }
            client.execute(batc.finish().unwrap()).unwrap();
        }
        let step = t.elapsed().as_millis();
        println!(
            " - time: {} | writes: {}",
            step,
            (10_i128.pow(i as u32) / BATCH_SIZE as i128)
        );
    }

    client
        .execute(
            DeleteContainerBuilder::new()
                .put_container(String::from("test_container"))
                .finish()
                .unwrap(),
        )
        .unwrap();
}

fn main() {
    tytodb();
    sqlite(BATCH_SIZE, STEPS).unwrap();
}
