use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::Value;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::Schema;
use vision_graphql::Engine;

async fn setup_pool() -> (
    deadpool_postgres::Pool,
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
                    name TEXT NOT NULL,
                    secret TEXT
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    user_id INT NOT NULL REFERENCES users(id)
                );
                INSERT INTO users (name, secret) VALUES ('alice', 's1'), ('bob', 's2');
                INSERT INTO posts (title, user_id) VALUES ('p1', 1), ('p2', 2);
                "#,
            )
            .await
            .expect("seed");
    }
    (pool, container)
}

#[tokio::test]
async fn introspect_auto_derives_relations() {
    let (pool, _c) = setup_pool().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();
    assert!(schema.table("users").is_some());
    assert!(schema.table("posts").is_some());
    assert!(
        schema
            .table("users")
            .unwrap()
            .find_relation("posts")
            .is_some(),
        "expected users.posts array relation"
    );
    assert!(
        schema
            .table("posts")
            .unwrap()
            .find_relation("user")
            .is_some(),
        "expected posts.user object relation"
    );
}

#[tokio::test]
async fn introspect_runs_queries_end_to_end() {
    let (pool, _c) = setup_pool().await;
    let schema = Schema::introspect(&pool).await.expect("introspect").build();
    let engine = Engine::new(pool, schema);
    let v: Value = engine
        .query("query { users { name posts { title } } }", None)
        .await
        .expect("query ok");
    let users = v["users"].as_array().unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["posts"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn load_config_renames_and_hides() {
    let (pool, _c) = setup_pool().await;
    let schema = Schema::introspect(&pool)
        .await
        .expect("introspect")
        .load_config("tests/fixtures/schema.toml")
        .expect("load toml")
        .build();
    assert!(schema.table("users").is_none());
    let profiles = schema.table("profiles").expect("renamed table");
    assert!(profiles.find_column("name").is_some());
    assert!(profiles.find_column("secret").is_none(), "should be hidden");

    let engine = Engine::new(pool, schema);
    let err = engine
        .query("query { profiles { secret } }", None)
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("unknown column 'secret'"));
}
