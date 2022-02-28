use std::env::current_dir;

use actix_web::{get, App, HttpResponse, HttpServer, Responder, web::Data};
use tortoise_datastore::{get_probe, update_probe, LsmEngine};
use tracing::info;
use tracing_subscriber;

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();
    info!("Starting server on port 8088");
    let engine = LsmEngine::open(current_dir()?).unwrap();

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(engine.clone()))
            .service(hello)
            .service(update_probe)
            .service(get_probe)
    })
    .bind("0.0.0.0:8088")?
    .run()
    .await
}
