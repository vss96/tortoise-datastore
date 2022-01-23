use std::{env::current_dir, sync::Mutex};

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use tortoise_datastore::LsmEngine;
use tracing::info;
use tracing_subscriber;

#[get("/")]
async fn hello(engine: web::Data<Mutex<LsmEngine>>) -> impl Responder {
    engine
        .lock()
        .unwrap()
        .set("123".to_string(), "456".to_string(), 1234567);
    HttpResponse::Ok().body("Hello world!")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();
    info!("Starting server on port 8080");
    let mut engine = LsmEngine::open(current_dir()?).unwrap();

    HttpServer::new(move || App::new().data(Mutex::new(engine.clone())).service(hello))
        .bind("127.0.0.1:8088")?
        .run()
        .await
}
