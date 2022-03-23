use actix_web::{
    get, put,
    web::{self, Json, Path},
    HttpResponse, Responder,
};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

use crate::LsmEngine;

#[put("/probe/{probe_id}/event/{event_id}")]
pub async fn update_probe(
    path: web::Path<(String, String)>,
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
        eventId: payload.eventId.clone(),
        messageType: payload.messageType,
        messageData: payload.messageData,
        eventReceivedTime: event_received_time,
    };

    let serialized_value = serde_json::to_string(&probe_value);
    match serialized_value {
        Ok(value) => {
            let id = payload.probeId.clone();
            tokio::spawn(async move {
                engine
                    .set(payload.probeId.clone(), value, event_transmission_time)
                    .await
                    .unwrap();
            });
            let probe_response = ProbeResponse {
                probeId: id,
                eventId: payload.eventId,
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

#[get("/probe/{probe_id}/latest")]
pub async fn get_probe(
    probe_id: web::Path<String>,
    engine: web::Data<LsmEngine>,
) -> impl Responder {
    let probe_id = probe_id.into_inner();
    let memtable_record = engine.get_memtable_record(probe_id.clone()).await;
    match engine.get(probe_id.clone()) {
        Some(entry) => {
            let event_transmission_time = entry.value().timestamp;
            if let Some(record) = memtable_record {
                if record.timestamp > event_transmission_time {
                    let probe_response =
                        get_probe_response(probe_id.clone(), record.value, record.timestamp);
                    return HttpResponse::Ok().json(probe_response);
                }
            }
            let probe_response = get_probe_response(
                probe_id.clone(),
                entry.value().value.clone(),
                event_transmission_time,
            );
            HttpResponse::Ok().json(probe_response)
        }
        None => {
            if let Some(record) = memtable_record {
                let probe_response =
                    get_probe_response(probe_id.clone(), record.value, record.timestamp);
                return HttpResponse::Ok().json(probe_response);
            }
            HttpResponse::NotFound().body("Required probe not found")
        }
    }
}

pub fn get_probe_response(
    probe_id: String,
    value: String,
    event_transmission_time: u128,
) -> ProbeResponse {
    let probe_value: ProbeValue =
        serde_json::from_str(&value).expect("Failed to deserialize probe values.");
    ProbeResponse {
        probeId: probe_id,
        eventId: probe_value.eventId,
        messageType: probe_value.messageType,
        eventTransmissionTime: event_transmission_time,
        messageData: probe_value.messageData,
        eventReceivedTime: probe_value.eventReceivedTime,
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct ProbePayload {
    probeId: String,
    eventId: String,
    messageType: String,
    eventTransmissionTime: u128,
    messageData: Vec<Message>,
}
#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct Message {
    measureName: String,
    measureCode: MeasureCode,
    measureUnit: String,
    measureValue: String,
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
#[allow(non_snake_case)]
pub struct ProbeResponse {
    probeId: String,
    eventId: String,
    messageType: String,
    eventTransmissionTime: u128,
    messageData: Vec<Message>,
    eventReceivedTime: u128,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
struct ProbeValue {
    eventId: String,
    messageType: String,
    messageData: Vec<Message>,
    eventReceivedTime: u128,
}
