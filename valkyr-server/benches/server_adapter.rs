#[allow(dead_code)]
mod fixture {
    include!("../tests/server_adapter.rs");
}

#[tokio::main]
async fn main() {
    let iterations = std::env::var("VALKYR_BENCH_ITERATIONS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(100);
    fixture::run_benchmark(iterations).await;
}
