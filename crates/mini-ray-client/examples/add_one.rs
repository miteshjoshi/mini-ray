use mini_ray_client::Client;
use mini_ray_core::{MiniRayError, ObjectRef, Result};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::connect("http://127.0.0.1:50051").await?;
    let input = client.put(41u64).await?;
    let output = client.submit::<u64, u64>("add_one", vec![input]).await?;
    let result = wait_for(&client, output).await?;

    println!("{result}");
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
        "timed out waiting for add_one result".to_string(),
    ))
}
