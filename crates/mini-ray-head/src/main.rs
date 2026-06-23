use clap::Parser;
use std::net::SocketAddr;

#[derive(Debug, Parser)]
#[command(name = "mini-ray-head")]
#[command(about = "Run a mini-ray head node")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:50051")]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    mini_ray_head::serve(args.bind).await?;
    Ok(())
}
