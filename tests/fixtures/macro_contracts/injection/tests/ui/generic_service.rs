use lily_injectable_derive::Injectable;

#[derive(Injectable)]
struct GenericService<T> {
    value: T,
}

fn main() {}
