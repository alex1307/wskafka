extern crate rdkafka;

use std::collections::HashMap;
use std::time::Duration;

use log::info;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Header, Message, OwnedHeaders};
use rdkafka::TopicPartitionList;

use futures::{stream, Stream, StreamExt};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::sync::mpsc::{self};

#[derive(Debug, Clone)]
pub struct KafkaClient {
    brokers: String,
    username: Option<String>,
    password: Option<String>,
    mechanism: Option<String>,
    protocol: Option<String>,
    config: ClientConfig,
}

pub enum Offset {
    Earliest,
    Latest,
    Last(i64),
}

impl KafkaClient {
    pub fn new(
        brokers: String,
        username: Option<String>,
        password: Option<String>,
        mechanism: Option<String>,
        protocol: Option<String>,
    ) -> Self {
        KafkaClient {
            brokers,
            username,
            password,
            mechanism,
            protocol,
            config: ClientConfig::new(),
        }
    }

    pub fn connect(&mut self) -> Result<(), String> {
        if let Some(username) = &self.username {
            self.config.set("sasl.username", username);
        }

        if let Some(password) = &self.password {
            self.config.set("sasl.password", password);
        }

        if let Some(mechanism) = &self.mechanism {
            self.config.set("sasl.mechanism", mechanism);
        }

        if let Some(protocol) = &self.protocol {
            self.config.set("security.protocol", protocol);
        } else {
            self.config.set("security.protocol", "plaintext");
        }
        self.config.set("bootstrap.servers", &self.brokers);

        Ok(())
    }

    pub fn get_metadata(&self) -> Result<String, String> {
        let base_consumer: BaseConsumer = match self.config.create() {
            Ok(consumer) => consumer,
            Err(e) => return Err(format!("Failed to create consumer: {}", e)),
        };

        let metadata = match base_consumer.fetch_metadata(None, Duration::from_secs(5)) {
            Ok(metadata) => metadata,
            Err(e) => return Err(format!("Failed to fetch metadata: {}", e)),
        };
        let mut txt = Vec::new();

        txt.push("Cluster information:".to_string());
        txt.push(format!("Broker count: {}", metadata.brokers().len()));
        txt.push(format!("  Topics count: {}", metadata.topics().len()));
        txt.push(format!(
            "  Metadata broker name: {}",
            metadata.orig_broker_name()
        ));
        txt.push(format!(
            "  Metadata broker id: {}\n",
            metadata.orig_broker_id()
        ));

        for broker in metadata.brokers() {
            txt.push(format!(
                "  Id: {}  Host: {}:{}  ",
                broker.id(),
                broker.host(),
                broker.port()
            ));
        }

        txt.push("\nTopics:".to_string());
        for topic in metadata.topics() {
            if topic.name().starts_with(r#"__"#) {
                continue;
            }
            txt.push(format!(
                "  Topic: {}  Err: {:?}",
                topic.name(),
                topic.error()
            ));
            for partition in topic.partitions() {
                txt.push(format!(
                    "     Partition: {}  Leader: {}  Replicas: {:?}  ISR: {:?}  Err: {:?}",
                    partition.id(),
                    partition.leader(),
                    partition.replicas(),
                    partition.isr(),
                    partition.error()
                ));
            }
        }
        Ok(txt.join("\n"))
    }

    pub fn get_offsets(&self) -> Result<String, String> {
        let base_consumer: BaseConsumer = match self.config.create() {
            Ok(consumer) => consumer,
            Err(e) => return Err(format!("Failed to create consumer: {}", e)),
        };

        let metadata = match base_consumer.fetch_metadata(None, Duration::from_secs(5)) {
            Ok(metadata) => metadata,
            Err(e) => return Err(format!("Failed to fetch metadata: {}", e)),
        };
        let mut message_count = 0;
        let mut txt = Vec::new();

        for topic in metadata.topics() {
            if topic.name().starts_with(r#"__"#) {
                continue;
            }
            txt.push(format!(
                "  Topic: {}  Err: {:?}",
                topic.name(),
                topic.error()
            ));
            for partition in topic.partitions() {
                txt.push(format!(
                    "     Partition: {}  Leader: {}  Replicas: {:?}  ISR: {:?}  Err: {:?}",
                    partition.id(),
                    partition.leader(),
                    partition.replicas(),
                    partition.isr(),
                    partition.error()
                ));

                let (low, high) = base_consumer
                    .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
                    .unwrap_or((-1, -1));
                txt.push(format!(
                    "       Low watermark: {}  High watermark: {} (difference: {})",
                    low,
                    high,
                    high - low
                ));
                message_count += high - low;
            }
        }
        txt.push(format!("     Total message count: {}", message_count));
        Ok(txt.join("\n"))
    }

    pub async fn read_from_topic(
        &mut self,
        topic: String,
        partition: i32,
        group: &str,
        offset: Offset,
    ) -> impl Stream<Item = String> {
        self.config.set("group.id", group);
        match offset {
            Offset::Earliest => self.config.set("auto.offset.reset", "earliest"),
            _ => self.config.set("auto.offset.reset", "latest"),
        };

        let consumer: BaseConsumer = match self.config.create() {
            Ok(consumer) => consumer,
            Err(e) => panic!("Consumer creation failed: {}", e),
        };
        let sub_topic = &[topic.as_str()];
        consumer
            .subscribe(sub_topic)
            .expect("Failed to subscribe to topic");
        if let Offset::Last(value) = offset {
            let latest_offset = consumer
                .fetch_watermarks(topic.as_str(), 0, Duration::from_secs(10))
                .map(|(_low, high)| high)
                .unwrap();
            let starting_offset = latest_offset.saturating_sub(value);
            let mut partition_list = TopicPartitionList::new();
            let _ = partition_list.add_partition_offset(
                &topic,
                partition,
                rdkafka::Offset::Offset(starting_offset),
            );
            let _ = consumer.assign(&partition_list);
        }

        // Consume Kafka messages
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                if let Some(m) = consumer.poll(Duration::from_secs(1)) {
                    match m {
                        Err(e) => println!("Kafka error: {}", e),
                        Ok(m) => {
                            eprintln!(
                                "Consumed message: topic: {}, partition: {}, offset: {}",
                                m.topic(),
                                m.partition(),
                                m.offset()
                            );
                            match m.payload_view::<str>() {
                                None => println!("Error while deserializing message payload"),
                                Some(Ok(s)) => {
                                    println!("Consumed message: topic: {}, partition: {}, offset: {}, payload: {}", m.topic(), m.partition(), m.offset(), s);
                                    tx.send(s.to_string()).unwrap();
                                }
                                Some(Err(e)) => {
                                    println!("Error while deserializing message payload: {:?}", e)
                                }
                            };
                        }
                    };
                } else {
                    println!("No message available");
                }
            }
        });
        stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|t| (t, rx)) })
    }

    pub async fn send_messages(
        &mut self,
        topic: &str,
        messages: HashMap<String, String>,
    ) -> String {
        info!(
            "Sending messages to kafka broker: {} and topic: {}",
            &self.brokers, topic
        );
        self.config.set("message.timeout.ms", "5000");
        let producer: &FutureProducer = &match self.config.create() {
            Ok(producer) => producer,
            Err(e) => panic!("Producer creation error: {}", e),
        };
        let total_number = &messages.len();
        let mut messages_to_send: Vec<_> = vec![];
        for (key, value) in messages {
            let cloned_key = key.clone();
            let cloned_value = value.clone();
            let future = async move {
                let delivery_status = producer.send(
                    FutureRecord::to(topic)
                        .payload(&cloned_value)
                        .key(&cloned_key)
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "header_key",
                            value: Some("header_value"),
                        })),
                    Duration::from_secs(0),
                );
                delivery_status.await
            };
            messages_to_send.push(future);
        }

        // This loop will wait until all delivery statuses have been received.

        let mut last_offset = 0;
        let mut success_counter = 0;
        let mut err_counter = 0;

        for message in messages_to_send {
            match message.await {
                Ok((_partition, offset)) => {
                    if offset > last_offset {
                        last_offset = offset;
                    }
                    success_counter += 1;
                }
                Err(e) => {
                    info!("Future completed. Result: {:?}", e);
                    err_counter += 1;
                }
            }
        }
        let response = vec![
            format!("Total messages: {}", total_number),
            format!("Success: {}", success_counter),
            format!("Errors: {}", err_counter),
            format!("Last offset: {}", last_offset),
        ];
        info!("Response: {:?}", response);
        response.join("\n")
    }
}

pub async fn read_from_kafka(topic: Vec<String>, group: &str) -> impl Stream<Item = String> {
    let broker = "localhost:9094";

    // Create a Kafka client configuration
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", broker)
        .set("security.protocol", "plaintext")
        .set("group.id", group) // Specify your consumer group ID
        .set("auto.offset.reset", "earliest"); // Set the offset reset option as needed

    let consumer: StreamConsumer = client_config.create().expect("Consumer creation failed");
    let str_slice: Vec<&str> = topic.iter().map(|s| s.as_str()).collect();
    let topic_slice = str_slice.as_slice();
    // Subscribe to Kafka topic(s)
    consumer
        .subscribe(topic_slice)
        .expect("Failed to subscribe to topic");

    // Consume Kafka messages
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            if let Some(m) = consumer.stream().next().await {
                match m {
                    Err(e) => println!("Kafka error: {}", e),
                    Ok(m) => {
                        match m.payload_view::<str>() {
                            None => println!("Error while deserializing message payload"),
                            Some(Ok(s)) => {
                                println!("Consumed message: topic: {}, partition: {}, offset: {}, payload: {}", m.topic(), m.partition(), m.offset(), s);
                                tx.send(s.to_string()).unwrap();
                            }
                            Some(Err(e)) => {
                                println!("Error while deserializing message payload: {:?}", e)
                            }
                        };
                    }
                };
            }
        }
    });
    stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|t| (t, rx)) })
}

mod test_kafka_client {

    use crate::utils::configure_log4rs;
    use futures_util::StreamExt;

    #[test]
    fn connect_to_kafka() {
        let mut kafka =
            super::KafkaClient::new(String::from("localhost:9094"), None, None, None, None);

        let connected = kafka.connect();
        assert_eq!(connected, Ok(()));
        let matadata = kafka.get_metadata().unwrap();
        assert!(matadata.contains("Cluster information:"));
        assert!(matadata.contains("Broker count:"));
        assert!(matadata.contains("Topics:"));
        println!("{}", matadata);

        let offsets = kafka.get_offsets().unwrap();
        assert!(offsets.contains("Low watermark:"));
        assert!(offsets.contains("High watermark:"));
        assert!(offsets.contains("Total message count:"));
        println!("{}", offsets);
    }

    #[tokio::test]
    async fn read_the_last_10_messages_test() {
        configure_log4rs("config/dev_log4rs.yml");
        let mut kafka =
            super::KafkaClient::new(String::from("localhost:9094"), None, None, None, None);

        let connected = kafka.connect();
        assert_eq!(connected, Ok(()));
        let group = uuid::Uuid::new_v4().to_string();
        eprint!("{}", group);
        let stream = kafka
            .read_from_topic(String::from("test"), 0, &group, super::Offset::Last(10))
            .await;
        stream
            .for_each(|msg| {
                println!("{}", msg);
                futures::future::ready(())
            })
            .await;
    }

    #[tokio::test]
    async fn read_from_beginning_test() {
        let mut kafka =
            super::KafkaClient::new(String::from("localhost:9094"), None, None, None, None);

        let connected = kafka.connect();
        assert_eq!(connected, Ok(()));
        let group = uuid::Uuid::new_v4().to_string();
        eprint!("{}", group);
        let stream = kafka
            .read_from_topic(String::from("test"), 0, &group, super::Offset::Earliest)
            .await;
        stream
            .for_each(|msg| {
                println!("{}", msg);
                futures::future::ready(())
            })
            .await;
    }

    #[tokio::test]
    async fn send_messages_test() {
        configure_log4rs("config/dev_log4rs.yml");
        let mut kafka =
            super::KafkaClient::new(String::from("localhost:9094"), None, None, None, None);

        let connected = kafka.connect();
        assert_eq!(connected, Ok(()));
        let mut messages = std::collections::HashMap::new();
        messages.insert(
            String::from("key1"),
            String::from(uuid::Uuid::new_v4().to_string()),
        );
        messages.insert(
            String::from("key2"),
            String::from(uuid::Uuid::new_v4().to_string()),
        );
        messages.insert(
            String::from("key3"),
            String::from(uuid::Uuid::new_v4().to_string()),
        );
        messages.insert(
            String::from("key4"),
            String::from(uuid::Uuid::new_v4().to_string()),
        );
        messages.insert(
            String::from("key5"),
            String::from(uuid::Uuid::new_v4().to_string()),
        );
        messages.insert(
            String::from("key6"),
            String::from(uuid::Uuid::new_v4().to_string()),
        );
        messages.insert(
            String::from("key7"),
            String::from(uuid::Uuid::new_v4().to_string()),
        );

        kafka.send_messages("test", messages).await;
    }
}
