use clap::Parser;
use mini_ray_core::Result;
use mini_ray_runtime::{ActorRegistry, TaskRegistry};
use mini_ray_worker::{Worker, WorkerConfig};

#[derive(Debug)]
struct Counter {
    value: u64,
}

#[derive(Debug, Parser)]
#[command(name = "mini-ray-worker")]
#[command(about = "Run a mini-ray worker process")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    head: String,
    #[arg(long, default_value_t = 1)]
    slots: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut registry = TaskRegistry::new();
    registry.register_unary("add_one", |value: u64| value + 1)?;
    registry.register_binary("add", |left: u64, right: u64| left + right)?;
    let mut actor_registry = ActorRegistry::new();
    actor_registry
        .register_constructor_unary("Counter", |initial: u64| Counter { value: initial })?;
    actor_registry.register_method_unary(
        "Counter",
        "increment",
        |counter: &mut Counter, delta: u64| {
            counter.value += delta;
            counter.value
        },
    )?;

    let worker = Worker::connect_with_actors(
        WorkerConfig::new(args.head, args.slots),
        registry,
        actor_registry,
    )
    .await?;
    worker.run().await
}
