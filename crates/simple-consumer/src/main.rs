use std::{collections::BTreeMap, marker::PhantomData, pin::pin};

use futures::{Stream, StreamExt};
use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::KafkaError,
    message::BorrowedMessage,
};
use serde::{Deserialize, de::DeserializeOwned};
use thiserror::Error;

use event::Event;
use util::shutdown_signal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let broker_address =
        std::env::var("BROKER_ADDRESS").expect("BROKER_ADDRESS env var must be set");

    let event_consumer =
        KafkaConsumerConfig::new(broker_address, "events", "main").build::<Event>()?;

    event_consumer
        .subscribe(
            async |delivery| {
                println!("received delivery: {:?}", delivery);
                Ok(())
            },
            shutdown_signal(),
        )
        .await?;

    Ok(())
}

const RESERVED_SETTINGS: &[&str] = &[
    "bootstrap.servers",
    "group.id",
    "enable.auto.commit",
    "enable.auto.offset.store",
];

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("invalid kafka config")]
    Config(#[source] KafkaError),

    #[error("subscribe failed")]
    Subscribe(#[source] KafkaError),

    #[error("setting `{0}` can't be overridden")]
    ReservedSetting(String),
}

#[derive(Debug, Error)]
pub enum RecvError {
    #[error("kafka error: {0}")]
    Kafka(#[from] KafkaError),
    #[error("empty payload at partition {}, offset {}", .0.partition, .0.offset)]
    EmptyPayload(Meta),
    #[error("invalid json at partition {}, offset {}: {source}", meta.partition, meta.offset)]
    Decode {
        meta: Meta,
        raw: Vec<u8>,
        #[source]
        source: serde_json::Error,
    },
}

pub type ListenerError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum SubscribeError {
    #[error("handler failed at partition {partition}, offset {offset}")]
    Handler {
        partition: i32,
        offset: i64,
        #[source]
        source: ListenerError,
    },

    #[error("final offset commit failed")]
    Flush(#[source] KafkaError),
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct KafkaConsumerConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
}

impl KafkaConsumerConfig {
    pub fn new(
        brokers: impl Into<String>,
        topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            topic: topic.into(),
            group_id: group_id.into(),
            settings: BTreeMap::new(),
        }
    }

    pub fn set(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.insert(key.into(), value.into());
        self
    }

    pub fn build<T: DeserializeOwned>(self) -> Result<KafkaConsumer<T>, BuildError> {
        if let Some(key) = RESERVED_SETTINGS
            .iter()
            .find(|key| self.settings.contains_key(**key))
        {
            return Err(BuildError::ReservedSetting(key.to_string()));
        }

        let mut config = ClientConfig::new();

        config
            .set("bootstrap.servers", self.brokers)
            .set("broker.address.family", "v4")
            .set("group.id", self.group_id)
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "false");

        for (key, value) in self.settings {
            config.set(key, value);
        }

        let inner = config
            .create::<StreamConsumer>()
            .map_err(BuildError::Config)?;

        inner
            .subscribe(&[&self.topic])
            .map_err(BuildError::Subscribe)?;

        Ok(KafkaConsumer {
            inner,
            topic: self.topic,
            _marker: PhantomData,
        })
    }
}

pub struct KafkaConsumer<T> {
    inner: StreamConsumer,
    topic: String,
    _marker: PhantomData<fn() -> T>,
}

#[derive(Clone, Debug)]
pub struct Meta {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub key: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct Delivery<T> {
    pub value: T,
    pub meta: Meta,
}

impl<T: DeserializeOwned> KafkaConsumer<T> {
    pub fn stream(&self) -> impl Stream<Item = Result<Delivery<T>, RecvError>> + '_ {
        self.inner.stream().map(|result| decode(&result?))
    }

    pub async fn subscribe<F>(
        &self,
        mut listener: F,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), SubscribeError>
    where
        F: AsyncFnMut(&Delivery<T>) -> Result<(), ListenerError>,
    {
        let result: Result<(), SubscribeError> = async {
            let mut events = pin!(self.stream().take_until(shutdown));

            while let Some(item) = events.next().await {
                match item {
                    Ok(delivery) => {
                        listener(&delivery)
                            .await
                            .map_err(|source| SubscribeError::Handler {
                                partition: delivery.meta.partition,
                                offset: delivery.meta.offset,
                                source,
                            })?;
                        self.ack_or_warn(&delivery.meta);
                    }
                    Err(RecvError::Decode { meta, source, .. }) => {
                        tracing::warn!(
                            partition = meta.partition, offset = meta.offset,
                            error = %source, "skipping undecodable message"
                        );
                        self.ack_or_warn(&meta);
                    }
                    Err(RecvError::EmptyPayload(meta)) => {
                        tracing::debug!(
                            partition = meta.partition,
                            offset = meta.offset,
                            "skipping empty payload"
                        );
                        self.ack_or_warn(&meta);
                    }
                    Err(RecvError::Kafka(e)) => {
                        tracing::warn!(error = %e, "kafka consumer error");
                    }
                }
            }
            Ok(())
        }
        .await;

        let flushed = self.flush();

        match (result, flushed) {
            (Err(e), _) => Err(e),
            (Ok(()), Err(e)) => Err(SubscribeError::Flush(e)),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    pub fn ack(&self, meta: &Meta) -> Result<(), KafkaError> {
        self.inner
            .store_offset(&self.topic, meta.partition, meta.offset)
    }

    pub fn flush(&self) -> Result<(), KafkaError> {
        self.inner.commit_consumer_state(CommitMode::Sync)
    }

    fn ack_or_warn(&self, meta: &Meta) {
        if let Err(e) = self.ack(meta) {
            tracing::warn!(
                partition = meta.partition, offset = meta.offset,
                error = %e, "ack failed"
            );
        }
    }
}

fn decode<T: DeserializeOwned>(msg: &BorrowedMessage<'_>) -> Result<Delivery<T>, RecvError> {
    let meta = Meta {
        partition: msg.partition(),
        offset: msg.offset(),
        timestamp: msg.timestamp().to_millis(),
        key: msg.key().map(<[u8]>::to_vec),
    };

    let Some(bytes) = msg.payload() else {
        return Err(RecvError::EmptyPayload(meta));
    };

    match serde_json::from_slice(bytes) {
        Ok(value) => Ok(Delivery { value, meta }),
        Err(source) => Err(RecvError::Decode {
            meta,
            raw: bytes.to_vec(),
            source,
        }),
    }
}
