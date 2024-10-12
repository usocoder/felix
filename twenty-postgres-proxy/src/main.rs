use anyhow::{anyhow, Result};
use futures::StreamExt;
use log::{error, info};
use pgwire::api::auth::{AuthStartupHandler, LoginInfo, ServerParameterProvider};
use pgwire::api::query::{SimpleQueryHandler, StatementOrPortal};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler};
use pgwire::tokio::process_socket;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[derive(Deserialize)]
struct Config {
    listen_addr: String,
    postgres_url: String,
    allowed_schemas: HashMap<String, String>,
}

struct ProxyHandler {
    config: Arc<Config>,
    client: tokio_postgres::Client,
}

impl SimpleQueryHandler for ProxyHandler {
    fn do_query<C>(
        &self,
        client: &C,
        query: &str,
    ) -> Result<Vec<pgwire::api::query::SimpleQueryResponse>>
    where
        C: ClientInfo,
    {
        let schema = client.get_attribute("schema").ok_or_else(|| anyhow!("Schema not set"))?;
        
        if !self.config.allowed_schemas.contains_key(schema) {
            return Err(anyhow!("Invalid schema"));
        }

        let rows = self.client.query(query, &[]).await?;
        let responses = rows
            .into_iter()
            .map(|row| {
                let values: Vec<Option<String>> = (0..row.len())
                    .map(|i| row.get::<_, Option<String>>(i))
                    .collect();
                pgwire::api::query::SimpleQueryResponse::Row(values)
            })
            .collect();

        Ok(responses)
    }
}

struct CustomStartupHandler {
    config: Arc<Config>,
}

impl AuthStartupHandler for CustomStartupHandler {
    fn handle_startup<C: ClientInfo>(
        &self,
        login: &LoginInfo,
        client: &mut C,
    ) -> Result<Option<ServerParameterProvider>> {
        let username = login.username().ok_or_else(|| anyhow!("Username not provided"))?;
        let schema = self.config.allowed_schemas.get(username).ok_or_else(|| anyhow!("Invalid user"))?;

        client.set_attribute("schema", schema);
        Ok(None)
    }

    fn handle_authentication<C: ClientInfo>(
        &self,
        _login: &LoginInfo,
        _password: Option<&[u8]>,
        _client: &mut C,
    ) -> Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let config: Config = serde_json::from_str(include_str!("../config.json"))?;
    let config = Arc::new(config);

    let (client, connection) = tokio_postgres::connect(&config.postgres_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Connection error: {}", e);
        }
    });

    let listener = TcpListener::bind(&config.listen_addr).await?;
    info!("Listening on {}", config.listen_addr);

    let handler = Arc::new(ProxyHandler { config: config.clone(), client });
    let make_handler = StatelessMakeHandler::new(handler);
    let startup_handler = Arc::new(CustomStartupHandler { config: config.clone() });

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        info!("New connection from {}", peer_addr);

        let make_handler = make_handler.clone();
        let startup_handler = startup_handler.clone();
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, make_handler, startup_handler).await {
                error!("Error processing connection: {:?}", e);
            }
        });
    }
}
