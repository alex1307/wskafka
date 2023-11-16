use std::collections::HashMap;

use crate::{rdkafka_client::KafkaClient, Clients};
use log::info;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use warp::{reject::Rejection, reply::json, Reply};

#[derive(Serialize, Debug)]
pub struct RegisterResponse {
    url: String,
}

#[derive(Deserialize, Debug)]
pub struct Event {
    correlation_id: String,
    topic: String,
    key: String,
    message: String,
}
#[derive(Serialize, Debug, Deserialize)]
pub struct ConnectRequest {
    correlation_id: Option<String>,
    broker: String,
    topic: Option<String>,
    timeout: Option<u64>,
    username: Option<String>,
    password: Option<String>,
    group: Option<String>,
    offset: Option<String>,
    mechanism: Option<String>,
    protocol: Option<String>,
}

#[derive(Serialize, Debug, Deserialize)]
pub struct ConnectResponse {
    info: String,
    correlation_id: String,
}
#[derive(Serialize, Debug)]
pub struct SendResponse {
    status: Vec<(i32, i64)>,
}

#[derive(Debug)]
pub struct AppErr {
    pub reason: String,
}

impl warp::reject::Reject for AppErr {}

pub async fn connect_handler(
    body: ConnectRequest,
    clients: Clients,
) -> Result<impl Reply, Rejection> {
    let correlation_id = if let Some(id) = body.correlation_id {
        id
    } else {
        Uuid::new_v4().to_string()
    };
    let found = clients.read().await.get(&correlation_id).cloned();
    info!("correlation_id: {}", &correlation_id);
    let kafka = if let Some(client) = found {
        client.clone()
    } else {
        let broker = body.broker.clone();
        let username = if body.username.is_some() {
            body.username.clone()
        } else {
            None
        };
        let password = if body.password.is_some() {
            body.password.clone()
        } else {
            None
        };
        let mechanism = if body.mechanism.is_some() {
            body.mechanism.clone()
        } else {
            None
        };
        let protocol = if body.protocol.is_some() {
            body.protocol.clone()
        } else {
            None
        };
        info!(
            "creating kafka client for{} and broker: {}",
            &correlation_id, &broker
        );
        let mut kafka = KafkaClient::new(broker, username, password, mechanism, protocol);
        if let Ok(_) = kafka.connect() {
            clients
                .write()
                .await
                .insert(correlation_id.clone(), kafka.clone());
            info!(
                "kafka client is created for : {:?}. All registered clients: {}",
                &correlation_id,
                clients.read().await.len()
            );
        } else {
            return Err(warp::reject::custom(AppErr {
                reason: "Failed to connect to kafka".to_string(),
            }));
        }

        kafka
    };
    let metadata = match kafka.get_metadata() {
        Ok(data) => data,
        Err(e) => {
            return Err(warp::reject::custom(AppErr {
                reason: e.to_string(),
            }))
        }
    };
    Ok(json(&ConnectResponse {
        info: metadata,
        correlation_id,
    }))
}

pub async fn message_handler(body: Event, clients: Clients) -> Result<impl Reply, Rejection> {
    let found = clients.read().await.get(&body.correlation_id).cloned();

    let mut kafka = if let Some(client) = found {
        client
    } else {
        return Err(warp::reject::custom(AppErr {
            reason: format!("No client found for topic: {}", body.correlation_id),
        }));
    };
    let mut messages: HashMap<String, String> = HashMap::new();
    for lines in body.message.lines() {
        let key_value = lines.split(":").collect::<Vec<&str>>();
        if key_value.len() != 2 {
            messages.insert(lines.to_owned(), lines.to_owned());
        } else {
            messages.insert(key_value[0].to_owned(), key_value[1].to_owned());
        }
    }
    info!("messages: {:?}", messages);
    let response = kafka.send_messages(&body.topic, messages).await;
    Ok(json(&SendResponse { status: response }))
}
