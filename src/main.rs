use std::collections::HashMap;
use std::convert::Infallible;
use std::fs;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt, TryFutureExt};
use lazy_static::lazy_static;
use log::error;
use log::info;
use rdkafka_client::KafkaClient;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, RwLock};
use tokio::time::timeout;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::reply::html;
use warp::ws::{Message, WebSocket};
use warp::Filter;

use crate::handler::connect_handler;
use crate::handler::message_handler;
use crate::handler::offset_handler;
use crate::utils::configure_log4rs;

pub mod handler;
pub mod rdkafka_client;
pub mod utils;

lazy_static! {
    static ref INIT_LOGGER: Once = Once::new();
    static ref CLIENTS: Arc<RwLock<HashMap<String, KafkaClient>>> =
        Arc::new(RwLock::new(HashMap::new()));
    static ref TOPICS: Arc<RwLock<HashMap<String, mpsc::UnboundedSender<String>>>> =
        Arc::new(RwLock::new(HashMap::new()));
    static ref CONTEXT: Arc<RwLock<HashMap<String, bool>>> = Arc::new(RwLock::new(HashMap::new())); 
}

type Topics = Arc<RwLock<HashMap<String, mpsc::UnboundedSender<String>>>>;
type Clients = Arc<RwLock<HashMap<String, KafkaClient>>>;

#[tokio::main]
async fn main() {
    configure_log4rs("config/dev_log4rs.yml");
    // Keep track of all connected users, key is usize, value
    // is a websocket sender.
    let topics = Topics::default();
    // Turn our "state" into a new Filter...
    let topics = warp::any().map(move || topics.clone());
    let clients = Clients::default();
    let connect = warp::path("connect");
    let connect_routes = connect
        .and(warp::post())
        .and(warp::body::json())
        .and(with_clients(clients.clone()))
        .and_then(connect_handler);
    let offset = warp::path("offset");
    let offset_routes = offset
        .and(warp::post())
        .and(warp::body::json())
        .and(with_clients(clients.clone()))
        .and_then(offset_handler);

    let publish = warp::path("publish");
    let publish_routes = publish
        .and(warp::post())
        .and(warp::body::json())
        .and(with_clients(clients.clone()))
        .and_then(message_handler);

    // GET /chat -> websocket upgrade
    let consumer = warp::path("messages")
        // The `ws()` filter will prepare Websocket handshake...
        .and(warp::ws())
        .and(topics)
        .map(|ws: warp::ws::Ws, topics| {
            // This will call our function if the handshake succeeds.
            ws.on_upgrade(move |socket| user_connected(socket, topics))
        });

    // GET / -> index html
    let index = warp::path::end().map(|| match fs::read_to_string("index.html") {
        Ok(body) => html(body),
        Err(_) => html("error".to_string()),
    });

    let routes = index
        .or(consumer)
        .or(connect_routes)
        .or(offset_routes)
        .or(publish_routes)
        .with(warp::cors().allow_any_origin());

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}

async fn user_connected(ws: WebSocket, topics: Topics) {
    // Use a counter to assign a new unique ID for this user.
    let my_topic = "test".to_owned();

    info!("new chat user: {}", &my_topic.clone());

    // Split the socket into a sender and receive of messages.
    let (mut user_ws_tx, mut user_ws_rx) = ws.split();

    // Use an unbounded channel to handle buffering and flushing of messages
    // to the websocket...
    let (tx, mut rx) = mpsc::unbounded_channel();
    
    tokio::task::spawn(async move {
        info!("starting to listen to messages");
        while let Some(message) = rx.recv().await {
            user_ws_tx
                .send(Message::text(message))
                .unwrap_or_else(|e| {
                    info!("websocket send error: {}", e);
                })
                .await;
        }
        info!("DONE");
    });

    let mut topic: String = String::new();
    let mut group: String = String::new();
    let mut offset: String = String::new();
    let mut correlation_id: String = String::new();
    while let Some(result) = user_ws_rx.next().await {
        match result {
            Ok(msg) => {
                info!("message from user: {}: {:?}", my_topic.clone(), msg);
                let _ack = tx.send("connected".to_string());
                let bytes = msg.as_bytes();
                let map: HashMap<String, String> =
                    if let Ok(utf8_string) = std::str::from_utf8(bytes) {
                        serde_json::from_str(utf8_string).expect("Failed to deserialize JSON")
                    } else {
                        HashMap::new()
                    };
                if let Some(value) = map.get("topic") {
                    info!("creating topics entry for {}", value);
                    topic = value.to_string();
                    TOPICS.write().await.insert(topic.clone(), tx.clone());
                }

                if let Some(value) = map.get("correlation_id") {
                    correlation_id = value.to_string();
                }

                if let Some(value) = map.get("offset") {
                    offset = value.to_string();
                }

                if let Some(value) = map.get("group") {
                    group = value.to_string();
                } else {
                    group = uuid::Uuid::new_v4().to_string();
                }

                if let Some(value) = map.get("disconnect"){
                    if "true".eq_ignore_ascii_case(&value.trim()) {
                        CONTEXT.write().await.insert(correlation_id.to_string(), true);
                    }
                }

                msg
            }
            Err(e) => {
                error!("websocket error(uid={}): {}", &my_topic, e);
                break;
            }
        };

        if let Some(kcat) = CLIENTS.read().await.get(&correlation_id) {
            user_message(topic.clone(), group.clone(), tx.clone(), kcat).await;
        }
    }

    // user_ws_rx stream will keep processing as long as the user stays
    // connected. Once they disconnect, then...
    user_disconnected("test".to_string(), &topics).await;
}

async fn user_message(
    topic: String,
    group: String,
    sender: UnboundedSender<String>,
    kcat: &KafkaClient,
) {
    // Skip any non-Text messages...

    // New message from this user, send it to everyone else (except same uid)...

    let mut cloned = kcat.clone();
    let stream = cloned.read_from_topic(
        topic.clone(),
        0,
        &group,
        rdkafka_client::Offset::Earliest,
        Some(sender.clone()),
        15
    );

    stream
        .await
        .for_each(|_| {
            // info!(">>> Consumed message: {}", m);
            // if let Err(_) = sender.send(m) {
            //     error!("failed sending message");
            // } else {
            //     info!("message sent");
            // }
            futures::future::ready(())
        })
        .await;
}

fn with_clients(clients: Clients) -> impl Filter<Extract = (Clients,), Error = Infallible> + Clone {
    warp::any().map(move || clients.clone())
}

async fn user_disconnected(my_id: String, topics: &Topics) {
    info!("good bye user: {}", my_id);

    // Stream closed up, so remove from the user list
    TOPICS.write().await.remove(&my_id);
}
