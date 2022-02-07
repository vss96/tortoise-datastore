use rand::Rng;
use std::{env::current_dir, sync::Mutex};

use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use tortoise_datastore::{update_probe, LsmEngine};
use tracing::info;
use tracing_subscriber;

#[get("/")]
async fn hello(engine: web::Data<LsmEngine>) -> impl Responder {
    // let mut rng = rand::thread_rng();
    //
    // engine
    //     .set(
    //         rng.gen_range(0..1000000).to_string(),
    //         "456".to_string(),
    //         1234567,
    //     )
    //     .unwrap();
    HttpResponse::Ok().body("Hello world!")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();
    info!("Starting server on port 8088");
    let engine = LsmEngine::open(current_dir()?).unwrap();

    HttpServer::new(move || {
        App::new()
            .data(engine.clone())
            .service(hello)
            .service(update_probe)
    })
    .bind("127.0.0.1:8088")?
    .run()
    .await
}
