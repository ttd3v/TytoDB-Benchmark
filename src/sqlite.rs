use rusqlite::{Connection, Result, params};
use std::{fs, time::Instant};

const BATCH_SIZE: i64 = 1000;

pub fn main() -> Result<()> {
    let db_path = "test.db";
    let _ = fs::remove_file(db_path);

    let mut conn = Connection::open(db_path)?;

    println!(
        "::\tBENCHMARK SQLITE\n - scale: {}\n - range: 1-6\n - schema: id,number,boolean\n",
        BATCH_SIZE
    );

    conn.execute(
        "CREATE TABLE test_table (
            id INTEGER,
            number INTEGER,
            boolean INTEGER
        )",
        [],
    )?;

    let mut v: i64 = 0;
    for i in 1..=6 {
        let t = Instant::now();
        let num_batches = 100i64.pow(i as u32) / BATCH_SIZE;
        for _ in 0..num_batches {
            let tx = conn.transaction()?;
            {
                let mut stmt =
                    tx.prepare("INSERT INTO test_table (id, number, boolean) VALUES (?, ?, ?)")?;
                for _ in 0..BATCH_SIZE {
                    v += 1;
                    stmt.execute(params![v, i, 1])?;
                }
            }
            tx.commit()?;
        }
        let step = t.elapsed().as_nanos();
        println!(" - time: {} | writes: {}", step, num_batches);
    }

    conn.execute("DROP TABLE test_table", [])?;

    Ok(())
}
