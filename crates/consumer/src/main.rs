use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};

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
        .set("group.id", "main")
        .set("security.protocol", "ssl")
        .set("ssl.ca.location", ssl_ca_location)
        .set("ssl.certificate.location", ssl_cert_location)
        .set("ssl.key.location", ssl_key_location);

    let consumer = config
        .create::<StreamConsumer>()
        .expect("failed to create client");

    consumer
        .subscribe(&["main"])
        .expect("failed to subscribe to topic 'main'");

    println!("consuming topic 'main'...");

    loop {
        match consumer.recv().await {
            Err(e) => eprintln!("kafka error: {e}"),
            Ok(message) => match message.payload() {
                None => {}
                Some(payload) => {
                    println!("payload: {:?}", payload);
                }
            },
        }
    }
}
