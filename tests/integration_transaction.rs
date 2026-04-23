use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::{Engine, Error};

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .primary_key(&["id"])
                .relation("posts", Relation::array("posts").on([("id", "user_id")])),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, false)
                .primary_key(&["id"])
                .relation("user", Relation::object("users").on([("user_id", "id")])),
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
    let mut cfg = Config::new();
    cfg.host = Some("127.0.0.1".into());
    cfg.port = Some(host_port);
    cfg.user = Some("postgres".into());
    cfg.password = Some("postgres".into());
    cfg.dbname = Some("postgres".into());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).expect("pool");
    {
        let client = pool.get().await.expect("client");
        client
            .batch_execute(
                r#"
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL CONSTRAINT users_name_key UNIQUE
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    user_id INT NOT NULL REFERENCES users(id)
                );
                "#,
            )
            .await
            .expect("seed");
    }
    let engine = Engine::new(pool, schema());
    (engine, container)
}

#[tokio::test]
async fn commit_path_persists_writes() {
    let (engine, _c) = setup().await;
    let out: Value = engine
        .transaction(async |tx| {
            let u = tx
                .query(
                    r#"mutation { insert_users_one(object: {name: "alice"}) { id } }"#,
                    None,
                )
                .await?;
            let uid = u["insert_users_one"]["id"].as_i64().unwrap();
            tx.query(
                r#"mutation($uid: Int!) {
                     insert_posts(objects: [{ title: "hello", user_id: $uid }]) {
                       affected_rows
                     }
                   }"#,
                Some(json!({ "uid": uid })),
            )
            .await?;
            Ok::<_, Error>(json!({ "uid": uid }))
        })
        .await
        .expect("tx ok");

    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_eq: "alice"}}) { id posts { title } } }"#,
            None,
        )
        .await
        .expect("select ok");
    let rows = v["users"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], out["uid"]);
    assert_eq!(rows[0]["posts"][0]["title"], json!("hello"));
}
