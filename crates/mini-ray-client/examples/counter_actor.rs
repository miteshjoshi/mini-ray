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

    let first_delta = client.put(2u64).await?;
    let first = client
        .call_actor::<Counter, u64, u64>(counter, "increment", vec![first_delta])
        .await?;
    println!("{}", wait_for(&client, first).await?);

    let second_delta = client.put(5u64).await?;
    let second = client
        .call_actor::<Counter, u64, u64>(counter, "increment", vec![second_delta])
        .await?;
    println!("{}", wait_for(&client, second).await?);
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
