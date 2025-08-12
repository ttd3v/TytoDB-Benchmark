use rusqlite::{Connection, Result, params};
use std::{fs, time::Instant};

pub fn main(batch_size: i32, steps: i32) -> Result<()> {
    let db_path = "test.db";
    let _ = fs::remove_file(db_path);

    let mut conn = Connection::open(db_path)?;

    println!(
        "::\tBENCHMARK SQLITE\n - scale: {}\n - range: 1-{}\n - schema: id,number,boolean\n",
        steps, batch_size
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
    for i in 1..=steps {
        let t = Instant::now();
        let num_batches = 10i64.pow(i as u32) / batch_size as i64;
        for _ in 0..num_batches {
            let tx = conn.transaction()?;
            {
                let mut stmt =
                    tx.prepare("INSERT INTO test_table (id, number, boolean) VALUES (?, ?, ?)")?;
                for _ in 0..batch_size {
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
