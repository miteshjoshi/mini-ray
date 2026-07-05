use mini_ray_client::Client;
use mini_ray_core::{MiniRayError, ObjectRef, Result};
use std::time::Duration;

#[derive(Debug)]
struct Counter;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::connect("http://127.0.0.1:50051").await?;
    let initial = client.put(10u64).await?;
    let counter = client
        .create_actor::<Counter, u64>("Counter", vec![initial])
        .await?;

    let add_two = client.put(2u64).await?;
    let add_five = client.put(5u64).await?;
    let add_three = client.put(3u64).await?;

    let first = client
        .call_actor::<Counter, u64, u64>(counter, "increment", vec![add_two])
        .await?;
    let second = client
        .call_actor::<Counter, u64, u64>(counter, "increment", vec![add_five])
        .await?;
    let third = client
        .call_actor::<Counter, u64, u64>(counter, "increment", vec![add_three])
        .await?;

    println!("start: 10");
    println!("after increment(2): {}", wait_for(&client, first).await?);
    println!("after increment(5): {}", wait_for(&client, second).await?);
    println!("after increment(3): {}", wait_for(&client, third).await?);
    Ok(())
}

async fn wait_for(client: &Client, object_ref: ObjectRef<u64>) -> Result<u64> {
    for _ in 0..100 {
        match client.get(object_ref).await {
            Ok(value) => return Ok(value),
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    Err(MiniRayError::TaskFailed(
        "timed out waiting for actor result".to_string(),
    ))
}
