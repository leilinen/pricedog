/// Database connection module.
/// Currently minimal — just establishes a connection.
/// Will be extended with table creation and caching as needed.

use tokio_postgres::Client;
use tokio_postgres::NoTls;

pub async fn connect(database_url: &str) -> anyhow::Result<Client> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;

    // Spawn the connection driver
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("database connection error: {}", e);
        }
    });

    Ok(client)
}
