# hello-kafka

Playing around with Apache Kafka locally.

```shell
docker compose up --build -d
process-compose -f process-compose.yaml up -n simple
```

### Producer

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
```

### Consumer

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
```
