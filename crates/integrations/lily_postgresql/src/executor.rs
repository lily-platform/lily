use std::future::Future;

use diesel_async::AsyncPgConnection;

use crate::{PgConnectionFuture, PgDatabaseService, PgResult, PgTransaction};

/// Executes repository work on an explicitly selected connection source.
///
/// Generated `*_in` methods accept `&mut AsyncPgConnection` (including the
/// connection supplied by [`PgDatabaseService::transaction`]), `&PgTransaction`,
/// or `&PgDatabaseService`. Connections and transaction handles never acquire
/// another pool connection. Only the database-service implementation acquires
/// one. No implementation here starts, commits, or rolls back a transaction.
///
/// The executor selects the database, independently of the repository's
/// configured database. Callers must choose the intended database/transaction,
/// particularly when using multiple factory cells. Operations own their inputs
/// so they can also run on a transaction handle's owner task.
pub trait PgExecutor: Send {
    /// Runs an owned operation on this executor's connection.
    fn with_connection<T, Operation>(
        self,
        operation: Operation,
    ) -> impl Future<Output = PgResult<T>> + Send
    where
        T: Send + 'static,
        Operation: for<'connection> FnOnce(
                &'connection mut AsyncPgConnection,
            ) -> PgConnectionFuture<'connection, T>
            + Send
            + 'static;
}

impl PgExecutor for &mut AsyncPgConnection {
    async fn with_connection<T, Operation>(self, operation: Operation) -> PgResult<T>
    where
        T: Send + 'static,
        Operation: for<'connection> FnOnce(
                &'connection mut AsyncPgConnection,
            ) -> PgConnectionFuture<'connection, T>
            + Send
            + 'static,
    {
        operation(self).await
    }
}

impl PgExecutor for &PgTransaction {
    async fn with_connection<T, Operation>(self, operation: Operation) -> PgResult<T>
    where
        T: Send + 'static,
        Operation: for<'connection> FnOnce(
                &'connection mut AsyncPgConnection,
            ) -> PgConnectionFuture<'connection, T>
            + Send
            + 'static,
    {
        PgTransaction::with_connection(self, operation).await
    }
}

impl PgExecutor for &PgDatabaseService {
    async fn with_connection<T, Operation>(self, operation: Operation) -> PgResult<T>
    where
        T: Send + 'static,
        Operation: for<'connection> FnOnce(
                &'connection mut AsyncPgConnection,
            ) -> PgConnectionFuture<'connection, T>
            + Send
            + 'static,
    {
        PgDatabaseService::with_connection(self, |connection, _| operation(connection), None).await
    }
}
