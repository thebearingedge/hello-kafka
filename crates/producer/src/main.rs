use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::time;
use uuid::Uuid;

use event::Event;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ssl_ca_location =
        std::env::var("SSL_CA_LOCATION").expect("SSL_CA_LOCATION env var required");
    let ssl_key_location =
        std::env::var("SSL_KEY_LOCATION").expect("SSL_KEY_LOCATION env var required");
    let ssl_cert_location =
        std::env::var("SSL_CERT_LOCATION").expect("SSL_CERT_LOCATION env var required");

    let mut config = ClientConfig::new();

    config
        .set("bootstrap.servers", "127.0.0.1:9093")
        .set("security.protocol", "ssl")
        .set("ssl.ca.location", ssl_ca_location)
        .set("ssl.key.location", ssl_key_location)
        .set("ssl.certificate.location", ssl_cert_location);

    let producer = config
        .create::<FutureProducer>()
        .expect("failed to create producer client");

    loop {
        let event = Event {
            id: Uuid::now_v7(),
            detail: "Hello, Kafka!".into(),
        };

        let payload = serde_json::to_string(&event).unwrap();

        let record = FutureRecord::to("main").key("0").payload(&payload);

        if let Err((e, _)) = producer.send(record, time::Duration::from_secs(0)).await {
            return Err(e)?;
        }

        println!("sent event: {:?}", event);

        time::sleep(time::Duration::from_secs(3)).await;
    }
}
