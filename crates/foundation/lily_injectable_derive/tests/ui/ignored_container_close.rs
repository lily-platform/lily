#![deny(unused_must_use)]

use lily_injection::ApplicationContainer;

async fn shutdown(container: &ApplicationContainer) {
    container.close().await;
}

fn main() {}
