//! Typed result API: `query_as` / `run_as` / `MutationResult`.

use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use vision_graphql::ast::OrderDir;
use vision_graphql::schema::{PgType, Schema, Table};
use vision_graphql::{Engine, Mutation, MutationResult, Query};

#[derive(Debug, Deserialize, PartialEq)]
struct User {
    id: i64,
    name: Option<String>,
}

fn users_schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, true)
                .column("active", "active", PgType::Bool, false)
                .primary_key(&["id"]),
        )
        .build()
}

async fn setup() -> (
    Engine,
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .expect("start pg");
    let host_port = container.get_host_port_ipv4(5432).await.expect("port");

    let url = format!("postgres://postgres:postgres@127.0.0.1:{host_port}/postgres");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("pool");

    sqlx::raw_sql(
        r#"
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    active BOOL NOT NULL
                );
                INSERT INTO users (name, active) VALUES
                    ('alice', TRUE),
                    ('bob',   FALSE);
                "#,
    )
    .execute(&pool)
    .await
    .expect("seed");

    (Engine::new(pool, users_schema()), container)
}

#[tokio::test]
async fn run_as_list_returns_vec() {
    let (engine, _c) = setup().await;
    let users: Vec<User> = engine
        .run_as(
            Query::from("users")
                .select(&["id", "name"])
                .order_by("id", OrderDir::Asc),
        )
        .await
        .expect("run_as");
    assert_eq!(
        users,
        vec![
            User {
                id: 1,
                name: Some("alice".into())
            },
            User {
                id: 2,
                name: Some("bob".into())
            },
        ]
    );
}

#[tokio::test]
async fn run_as_by_pk_returns_option() {
    let (engine, _c) = setup().await;
    let user: Option<User> = engine
        .run_as(Query::by_pk("users", &[("id", json!(1))]).select(&["id", "name"]))
        .await
        .expect("run_as by_pk");
    assert_eq!(
        user,
        Some(User {
            id: 1,
            name: Some("alice".into())
        })
    );

    let missing: Option<User> = engine
        .run_as(Query::by_pk("users", &[("id", json!(999))]).select(&["id", "name"]))
        .await
        .expect("run_as by_pk missing");
    assert_eq!(missing, None);
}

#[tokio::test]
async fn run_as_insert_returns_mutation_result() {
    let (engine, _c) = setup().await;
    let obj: BTreeMap<String, serde_json::Value> = [
        ("name".to_string(), json!("cara")),
        ("active".to_string(), json!(true)),
    ]
    .into();
    let res: MutationResult<User> = engine
        .run_as(Mutation::insert("users", vec![obj]).returning(&["id", "name"]))
        .await
        .expect("run_as insert");
    assert_eq!(res.affected_rows, 1);
    assert_eq!(
        res.returning,
        vec![User {
            id: 3,
            name: Some("cara".into())
        }]
    );
}

#[tokio::test]
async fn query_as_deserializes_data_envelope() {
    let (engine, _c) = setup().await;

    #[derive(Debug, Deserialize)]
    struct Data {
        users: Vec<User>,
    }

    let data: Data = engine
        .query_as("query { users(order_by: {id: asc}) { id name } }", None)
        .await
        .expect("query_as");
    assert_eq!(data.users.len(), 2);
    assert_eq!(data.users[0].name.as_deref(), Some("alice"));
}

#[tokio::test]
async fn run_as_inside_transaction() {
    let (engine, _c) = setup().await;
    let users: Vec<User> = engine
        .transaction(async |tx| {
            tx.run_as(
                Query::from("users")
                    .select(&["id", "name"])
                    .order_by("id", OrderDir::Asc),
            )
            .await
        })
        .await
        .expect("tx run_as");
    assert_eq!(users.len(), 2);
}
