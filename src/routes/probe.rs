use actix_web::{
    put,
    web::{self, Json, Path},
    HttpResponse, Responder,
};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

use crate::LsmEngine;

#[put("/probe/{probe_id}/event/{event_id}")]
pub async fn update_probe(
    web::Path((probe_id, event_id)): web::Path<(String, String)>,
    request_payload: Json<ProbePayload>,
    engine: web::Data<LsmEngine>,
) -> impl Responder {
    let payload = request_payload.into_inner();
    info!("{:?}", payload);
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let event_transmission_time = payload.eventTransmissionTime;
    let event_received_time = since_the_epoch.as_millis();

    let probe_value = ProbeValue {
        eventId: event_id.clone(),
        messageType: payload.messageType,
        messageData: payload.messageData,
    };

    let serialized_value = serde_json::to_string(&probe_value);
    match serialized_value {
        Ok(value) => {
            let id = probe_id.clone();
            actix_rt::spawn(async move {
                engine
                    .set(probe_id.clone(), value, event_transmission_time)
                    .await
                    .unwrap();
            });
            let probe_response = ProbeResponse {
                probeId: id,
                eventId: event_id,
                messageType: probe_value.messageType,
                messageData: probe_value.messageData,
                eventReceivedTime: event_received_time,
                eventTransmissionTime: event_transmission_time,
            };
            HttpResponse::Ok().json(probe_response)
        }
        Err(e) => {
            info!("Could not serialize the values {}", e);
            HttpResponse::BadRequest().body("Error in serializing")
        }
    }
}

#[derive(Deserialize)]
struct Info {
    username: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProbePayload {
    probeId: String,
    eventId: String,
    messageType: String,
    eventTransmissionTime: u128,
    messageData: Vec<Message>,
}
#[derive(Debug, Serialize, Deserialize)]
struct Message {
    measureName: String,
    measureCode: MeasureCode,
    measureUnit: String,
    measureValue: MeasureValueType,
    measureValueDescription: String,
    measureType: String,
    componentReading: String,
}

#[derive(Debug, Serialize, Deserialize)]
enum MeasureCode {
    SCSED,
    SCSEAA,
    SCSEPA,
    LER,
    PLSE,
    PDL,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum MeasureValueType {
    FLOAT(f32),
    TEXT(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct ProbeResponse {
    probeId: String,
    eventId: String,
    messageType: String,
    eventTransmissionTime: u128,
    messageData: Vec<Message>,
    eventReceivedTime: u128,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProbeValue {
    eventId: String,
    messageType: String,
    messageData: Vec<Message>,
}
