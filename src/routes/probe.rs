use actix_web::{
    put,
    web::{self, Json, Path},
    HttpResponse, Responder,
};
use serde::{Deserialize, Serialize};

#[put("/probe/{probe_id}/event/{event_id}")]
pub async fn update_probe(
    probe_id: Path<String>,
    event_id: Path<String>,
    payload: Json<ProbePayload>,
) -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[derive(Debug, Serialize, Deserialize)]
struct ProbePayload {
    probeId : String,
    eventId : String,
    messageType: String,
    eventTransmissionTime: String,
    messageData: Vec<Message>,

}

#[derive(Debug, Serialize, Deserialize)]
struct Message{
    measureName: String,
    measureCode: MeasureCode,
    measureUnit: String,
    measureValue: MeasureValueType,
    measureValueDescription: String,
    measureType: String,
    componentReading: f32
}

#[derive(Debug, Serialize, Deserialize)]
enum MeasureCode{
    SCSED,
    SCSEAA,
    SCSEPA,
    LER,
    PLSE,
    PDL
}
#[derive(Debug, Serialize, Deserialize)]
enum MeasureValueType{
    FLOAT(f32),
    TEXT(String)
}