use mini_ray_client::Client;
use mini_ray_core::Result;
use mini_ray_runtime::{ActorRegistry, TaskRegistry};
use mini_ray_worker::{Worker, WorkerConfig};
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

#[derive(Debug)]
struct Counter {
    value: u64,
}

async fn start_head() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    tokio::spawn(async move {
        mini_ray_head::serve(address).await.unwrap();
    });

    address
}

async fn connect_client(address: SocketAddr) -> Client {
    let endpoint = format!("http://{address}");
    for _ in 0..50 {
        if let Ok(client) = Client::connect(&endpoint).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("head server did not start at {endpoint}");
}

#[tokio::test]
async fn task_runs_end_to_end() -> Result<()> {
    let address = start_head().await;
    let client = connect_client(address).await;
    let mut registry = TaskRegistry::new();
    registry.register_unary("add_one", |value: u64| value + 1)?;
    let mut worker =
        Worker::connect(WorkerConfig::new(format!("http://{address}"), 1), registry).await?;
    worker.register().await?;

    let input = client.put(41u64).await?;
    let output = client.submit::<u64, u64>("add_one", vec![input]).await?;

    assert_eq!(worker.poll_and_execute_once().await?, 1);
    assert_eq!(client.get(output).await?, 42u64);
    Ok(())
}

#[tokio::test]
async fn actor_preserves_state_end_to_end() -> Result<()> {
    let address = start_head().await;
    let client = connect_client(address).await;
    let tasks = TaskRegistry::new();
    let mut actors = ActorRegistry::new();
    actors.register_constructor_unary("Counter", |initial: u64| Counter { value: initial })?;
    actors.register_method_unary(
        "Counter",
        "increment",
        |counter: &mut Counter, delta: u64| {
            counter.value += delta;
            counter.value
        },
    )?;
    let mut worker = Worker::connect_with_actors(
        WorkerConfig::new(format!("http://{address}"), 1),
        tasks,
        actors,
    )
    .await?;
    worker.register().await?;

    let initial = client.put(10u64).await?;
    let counter = client
        .create_actor::<Counter, u64>("Counter", vec![initial])
        .await?;

    let first_delta = client.put(2u64).await?;
    let first = client
        .call_actor::<Counter, u64, u64>(counter, "increment", vec![first_delta])
        .await?;
    assert_eq!(worker.poll_and_execute_once().await?, 1);
    assert_eq!(client.get(first).await?, 12u64);

    let second_delta = client.put(5u64).await?;
    let second = client
        .call_actor::<Counter, u64, u64>(counter, "increment", vec![second_delta])
        .await?;
    assert_eq!(worker.poll_and_execute_once().await?, 1);
    assert_eq!(client.get(second).await?, 17u64);
    Ok(())
}
