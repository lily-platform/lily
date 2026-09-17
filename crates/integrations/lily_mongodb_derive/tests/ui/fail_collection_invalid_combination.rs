use lily_mongodb_derive::MongoCollection;

#[derive(MongoCollection)]
#[collection("audit")]
#[collection_type(())]
#[unique_combination("only_one")]
struct AuditCollection {
    db: (),
    collection: (),
}

fn main() {}
