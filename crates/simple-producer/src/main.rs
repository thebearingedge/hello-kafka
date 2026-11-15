use std::{collections::BTreeMap, marker::PhantomData, time::Duration};

use rdkafka::{
    ClientConfig,
    error::KafkaError,
    producer::{FutureProducer, FutureRecord, Producer, future_producer::Delivery},
    util::Timeout,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use event::Event;
use util::shutdown_signal;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let broker_address =
        std::env::var("BROKER_ADDRESS").expect("BROKER_ADDRESS env var must be set");

    let event_producer = KafkaProducerConfig::new(broker_address, "events").build::<Event>()?;

    let shutdown = shutdown_signal();

    tokio::pin!(shutdown);

    let mut ticker = tokio::time::interval(Duration::from_secs(3));

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            _ = ticker.tick() => {
                let event = Event {
                    id: Uuid::now_v7(),
                    detail: "Hello, Kafka!".into()
                };
                event_producer.send(None, &event).await?;
            }
        }
    }

    event_producer.flush()?;

    Ok(())
}

#[derive(Debug, Error)]
pub enum ProducerBuildError {
    #[error("invalid kafka config")]
    Config(#[source] KafkaError),

    #[error("setting `{0}` can't be overridden")]
    ReservedSetting(&'static str),
}

#[derive(Debug, Error)]
pub enum SendError {
    #[error("failed to encode payload")]
    Encode(#[from] serde_json::Error),

    #[error("kafka delivery failed")]
    Kafka(#[source] KafkaError),
}

#[derive(Clone, Copy, Debug)]
pub struct Ack {
    pub partition: i32,
    pub offset: i64,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct KafkaProducerConfig {
    pub brokers: String,
    pub topic: String,
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
}

impl KafkaProducerConfig {
    pub fn new(brokers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            topic: topic.into(),
            settings: BTreeMap::new(),
        }
    }

    pub fn set(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.insert(key.into(), value.into());
        self
    }

    pub fn build<T: Serialize>(self) -> Result<KafkaProducer<T>, ProducerBuildError> {
        if self.settings.contains_key("bootstrap.servers") {
            return Err(ProducerBuildError::ReservedSetting("bootstrap.servers"));
        }

        let mut config = ClientConfig::new();

        config
            .set("bootstrap.servers", self.brokers)
            .set("broker.address.family", "v4")
            .set("enable.idempotence", "true");

        for (key, value) in self.settings {
            config.set(key, value);
        }

        let inner = config
            .create::<FutureProducer>()
            .map_err(ProducerBuildError::Config)?;

        Ok(KafkaProducer {
            inner,
            topic: self.topic,
            _marker: PhantomData,
        })
    }
}

pub struct KafkaProducer<T> {
    inner: FutureProducer,
    topic: String,
    _marker: PhantomData<fn(&T)>,
}

impl<T: Serialize> KafkaProducer<T> {
    pub async fn send(&self, key: Option<&[u8]>, value: &T) -> Result<Ack, SendError> {
        let payload = serde_json::to_vec(value)?;

        let mut record = FutureRecord::<[u8], [u8]>::to(&self.topic).payload(&payload);

        if let Some(key) = key {
            record = record.key(key);
        }

        self.inner
            .send(record, Timeout::Never)
            .await
            .map(
                |Delivery {
                     partition, offset, ..
                 }| Ack { partition, offset },
            )
            .map_err(|(e, _msg)| SendError::Kafka(e))
    }

    pub fn flush(&self) -> Result<(), KafkaError> {
        self.inner.flush(Timeout::After(Duration::from_secs(10)))
    }
}
