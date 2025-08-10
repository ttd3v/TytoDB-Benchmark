use tytodb_client::{alba, client_thread, handler::{BatchBuilder, CreateContainerBuilder, CreateRowBuilder, DeleteContainerBuilder, SearchBuilder}, lo, logical_operators::LogicalOperator, ToAlbaAlbaTypes, BIGINT, MEDIUM_STRING};

use std::{env, fs::File, os::unix::fs::FileExt};


fn main() {
    let mut secret = [0u8;32];
    println!("--> reading the secret file");
    if let Ok(f) = File::open(format!("{}/TytoDB/.secret", std::env::var("HOME").unwrap())){
        f.read_exact_at(&mut secret,0).unwrap();
    }
    println!("\n==> secret file read succesfully");
    
    println!("--> connecting to tytodb");
    let client = client_thread::Client::connect("127.0.0.1:4287", secret).unwrap();
    println!("\n==> connected to tytodb succesfully");   
   

    for layer in 1..10{
    let a=std::time::Instant::now();
    let create_container_builder = CreateContainerBuilder::new()
    .put_container("nice_container".to_string())
    .insert_header("id".to_string(), BIGINT)
    .insert_header("content".to_string(), MEDIUM_STRING);
    client.execute(create_container_builder.finish().unwrap()).unwrap();
    for w in 1..100*layer{
    let mut batched = BatchBuilder::new();
    batched = batched.transaction(true);
    
    
    let create_main_row = CreateRowBuilder::new()
    .put_container("nice_container".to_string())
    .insert_value("id".to_string(), alba!(w))
    .insert_value("content".to_string(), alba!("legal-legal-legal".to_string()));

    batched = batched.push(create_main_row);

    //println!("\n~~> Batching multiple requests\n-> Create container builder\n-> Create main row");
    client.execute(batched.finish().unwrap()).unwrap();
    //println!("\n\n--> Batching multiple requests finished\n=> Create container builder finished\n=> Create main row finished\n\n\n");

    let search_main_row = SearchBuilder::new()
        .add_container("nice_container".to_string())
        .add_column_name("id".to_string())
        .add_conditions( ( "id".to_string(), lo!(=), alba!(w) ) , true)
        .add_conditions( ( "content".to_string(), lo!(!=), alba!("paia-paia".to_string()) ), true)
        .add_conditions( ( "content".to_string(), lo!("&>"), alba!("legal-legal-legal".to_string()) ), true)
        .add_conditions( ( "content".to_string(), lo!("&&>"), alba!("legal-legal-legal".to_string()) ), true)
        .add_conditions( ( "content".to_string(), lo!(regex), alba!("legal-legal-legal".to_string()) ), true)
        .add_conditions( ( "id".to_string() , lo!(>), alba!(0) ), true )
        .add_conditions( ( "id".to_string() , lo!(<), alba!(w+2) ), true )
        .add_conditions( ( "id".to_string() , lo!(>=), alba!(w) ), true )
        .add_conditions( ( "id".to_string() , lo!(<=), alba!(w) ), true );

    // println!("--> Search");
    let list = client.execute(search_main_row.finish().unwrap()).unwrap().row_list;
    //println!("==> Search finished without errors");
    //println!("=== ROW-LIST-LENGTH: {}",list.len());
    //println!("\n=== LIST: {:?}",list);
    }
    let delc = DeleteContainerBuilder::new().put_container("nice_container".to_string());
    client.execute(delc.finish().unwrap()).unwrap();
    println!("{}. Execution time: {}ms",layer,a.elapsed().as_millis());
    }
}
