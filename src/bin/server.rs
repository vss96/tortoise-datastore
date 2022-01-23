use std::env::current_dir;

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use tortoise_datastore::LsmEngine;
use tracing::info;
use tracing_subscriber;

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();
    info!("Starting server on port 8080");
    let mut engine = LsmEngine::open(current_dir()?).unwrap();
    engine
        .set(
            "123".to_string(),
            "{'Blabla':'Blabla'}".to_string(),
            123456789,
        )
        .unwrap();
    println!("{:?}", engine.get("123".to_string()));
    HttpServer::new(|| App::new().service(hello))
        .bind("127.0.0.1:8080")?
        .run()
        .await
}
