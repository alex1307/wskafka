use std::collections::HashMap;
use std::convert::Infallible;
use std::fs;
use std::sync::Arc;
use std::sync::Once;
use std::time::{self, Duration};

use futures_util::{SinkExt, StreamExt, TryFutureExt};
use lazy_static::lazy_static;
use log::info;
use rdkafka_client::{metadata, read_from_kafka, KafkaClient};
use tokio::sync::{mpsc, RwLock};
use tokio::time::timeout;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::reply::html;
use warp::ws::{Message, WebSocket};
use warp::Filter;

use crate::handler::connect_handler;
use crate::handler::message_handler;
use crate::utils::configure_log4rs;

pub mod handler;
pub mod rdkafka_client;
pub mod utils;

lazy_static! {
    static ref INIT_LOGGER: Once = Once::new();
}

type Topics = Arc<RwLock<HashMap<String, mpsc::UnboundedSender<Message>>>>;
type Clients = Arc<RwLock<HashMap<String, KafkaClient>>>;

#[tokio::main]
async fn main() {
    configure_log4rs("config/dev_log4rs.yml");
    let txt = metadata("localhost:9094", time::Duration::from_secs(15), true);
    info!("{}", txt);
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

    let publish = warp::path("publish");
    let publish_routes = publish
        .and(warp::post())
        .and(warp::body::json())
        .and(with_clients(clients.clone()))
        .and_then(message_handler);

    // GET /chat -> websocket upgrade
    let chat = warp::path("chat")
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
        .or(chat)
        .or(connect_routes)
        .or(publish_routes)
        .with(warp::cors().allow_any_origin());

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}

async fn user_connected(ws: WebSocket, topics: Topics) {
    // Use a counter to assign a new unique ID for this user.
    let my_topic = "test".to_owned();

    eprintln!("new chat user: {}", &my_topic.clone());

    // Split the socket into a sender and receive of messages.
    let (mut user_ws_tx, mut user_ws_rx) = ws.split();

    // Use an unbounded channel to handle buffering and flushing of messages
    // to the websocket...
    let (tx, rx) = mpsc::unbounded_channel();
    let mut rx = UnboundedReceiverStream::new(rx);

    tokio::task::spawn(async move {
        while let Some(message) = rx.next().await {
            user_ws_tx
                .send(message)
                .unwrap_or_else(|e| {
                    eprintln!("websocket send error: {}", e);
                })
                .await;
        }
    });

    let mut topic: String = String::new();
    let mut group: String = String::new();
    while let Some(result) = user_ws_rx.next().await {
        let msg = match result {
            Ok(msg) => {
                eprintln!("message from user: {}: {:?}", my_topic.clone(), msg);
                let bytes = msg.as_bytes();
                let map: HashMap<String, String> =
                    if let Ok(utf8_string) = std::str::from_utf8(bytes) {
                        serde_json::from_str(utf8_string).expect("Failed to deserialize JSON")
                    } else {
                        HashMap::new()
                    };
                if let (Some(t), Some(g)) = (map.get("topic"), map.get("group")) {
                    topic = t.to_string();
                    group = g.to_string();
                    topics.write().await.insert(topic.clone(), tx.clone());
                }
                msg
            }
            Err(e) => {
                eprintln!("websocket error(uid={}): {}", &my_topic, e);
                break;
            }
        };
        user_message(topic.clone(), group.clone(), &topics).await;
    }

    // user_ws_rx stream will keep processing as long as the user stays
    // connected. Once they disconnect, then...
    user_disconnected("test".to_string(), &topics).await;
}

async fn user_message(topic: String, group: String, topics: &Topics) {
    // Skip any non-Text messages...

    // New message from this user, send it to everyone else (except same uid)...
    for (&ref uid, tx) in topics.read().await.iter() {
        if topic.clone() == uid.to_owned() {
            let stream = read_from_kafka(vec![topic.clone()], &group).await;
            let timeout_duration = Duration::from_secs(5);

            // Create a new stream that completes or times out after the specified duration

            let timed_out_future = async {
                stream
                    .for_each(|m| {
                        println!("Consumed message: {}", m);
                        let _ = tx.send(Message::text(m));
                        return futures::future::ready(());
                    })
                    .await;
            };

            match timeout(timeout_duration, timed_out_future).await {
                Ok(_) => println!("Stream completed"),
                Err(_) => println!("Timeout occurred"),
            }
        }
    }
}
fn with_clients(clients: Clients) -> impl Filter<Extract = (Clients,), Error = Infallible> + Clone {
    warp::any().map(move || clients.clone())
}

async fn user_disconnected(my_id: String, topics: &Topics) {
    eprintln!("good bye user: {}", my_id);

    // Stream closed up, so remove from the user list
    topics.write().await.remove(&my_id);
}
