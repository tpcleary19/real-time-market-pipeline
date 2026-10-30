use sqlx::{
    Connection,
    postgres::{PgConnectOptions, PgConnection, PgSslMode},
};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let options: PgConnectOptions = std::env::var("DATABASE_URL")?.parse()?;
    let mut connection =
        PgConnection::connect_with(&options.ssl_mode(PgSslMode::VerifyFull)).await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('trades', 'pattern_alerts')").fetch_one(&mut connection).await?;
    assert_eq!(count, 2);
    println!(
        "Rust SQLx connection passed with full TLS certificate and hostname verification; both tables exist."
    );
    Ok(())
}
