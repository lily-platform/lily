use lily_queue_derive::queue;

#[queue("audit.standalone", version = 1, content = "json")]
fn silently_unregistered_handler() {}

fn main() {}
