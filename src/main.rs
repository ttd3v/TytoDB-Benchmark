use tytodb_conn::{client_tokio, handler::{BatchBuilder, CreateContainerBuilder, CreateRowBuilder, DeleteContainerBuilder, SearchBuilder}, lo, logical_operators::LogicalOperator, BIGINT, MEDIUM_STRING};
use std::{fs::File, os::unix::fs::FileExt};

#[tokio::main]
async fn main() {
    let mut secret = [0u8;32];
    
    println!("--> reading the secret file");
    if let Ok(f) = File::open("/home/theo/TytoDB/.secret"){
        f.read_exact_at(&mut secret,0).unwrap();
    }
    println!("\n==> secret file read succesfully");
    
    println!("--> connecting to tytodb");
    let client = client_tokio::Client::connect("127.0.0.1:4287", secret).await.unwrap();
    println!("\n==> connected to tytodb succesfully");     
    let mut batched = BatchBuilder::new();
    batched = batched.transaction(true);
/*
    let create_container_builder = CreateContainerBuilder::new()
    .put_container("container_for_testing".to_string())
    .insert_header("id".to_string(), BIGINT)
    .insert_header("content".to_string(), MEDIUM_STRING);

    let create_main_row = CreateRowBuilder::new()
    .put_container("container_for_testing".to_string())
    .insert_value("id".to_string(), tytodb_conn::AlbaTypes::I64(881))
    .insert_value("content".to_string(), tytodb_conn::AlbaTypes::String("legal-legal-legal".to_string()));

    batched = batched.push(create_main_row);

    println!("\n~~> Batching multiple requests\n-> Create container builder\n-> Create main row");
    client.execute(batched.finish().unwrap()).await.unwrap();
    println!("\n\n--> Batching multiple requests finished\n=> Create container builder finished\n=> Create main row finished\n\n\n");
    */
    let search_main_row = SearchBuilder::new()
        .add_container("container_for_testing".to_string())
        .add_column_name("id".to_string())/*
        .add_conditions( ( "id".to_string(), lo!(=), tytodb_conn::AlbaTypes::I64(879) ) , true)
        .add_conditions( ( "content".to_string(), lo!(!=), tytodb_conn::AlbaTypes::String("paia-paia".to_string()) ), true)
        .add_conditions( ( "content".to_string(), lo!("&>"), tytodb_conn::AlbaTypes::String("legal-legal-legal".to_string()) ), true)
        .add_conditions( ( "content".to_string(), lo!("&&>"), tytodb_conn::AlbaTypes::String("legal-legal-legal".to_string()) ), true)
        .add_conditions( ( "content".to_string(), lo!(regex), tytodb_conn::AlbaTypes::String("legal-legal-legal".to_string()) ), true)
        .add_conditions( ( "id".to_string() , lo!(>), tytodb_conn::AlbaTypes::I64(0) ), true )
        .add_conditions( ( "id".to_string() , lo!(<), tytodb_conn::AlbaTypes::I64(1000) ), true )
        .add_conditions( ( "id".to_string() , lo!(>=), tytodb_conn::AlbaTypes::I64(879) ), true )
        .add_conditions( ( "id".to_string() , lo!(<=), tytodb_conn::AlbaTypes::I64(879) ), true )*/;

    println!("--> Search");
    let list = client.execute(search_main_row.finish().unwrap()).await.unwrap().row_list;
    println!("==> Search finished without errors");
    println!("=== ROW-LIST-LENGTH: {}",list.len());
    println!("\n=== LIST: {:?}",list);
    
//    let delc = DeleteContainerBuilder::new().put_container("container_for_testing".to_string());
 //   client.execute(delc.finish().unwrap()).await.unwrap();
}
