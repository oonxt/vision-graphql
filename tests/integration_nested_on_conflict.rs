use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use serde_json::{json, Value};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::Engine;

fn schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("organizations", "public", "organizations")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .primary_key(&["id"]),
        )
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .column("email", "email", PgType::Text, true)
                .column("organization_id", "organization_id", PgType::Int4, true)
                .primary_key(&["id"])
                .relation("posts", Relation::array("posts").on([("id", "user_id")]))
                .relation(
                    "organization",
                    Relation::object("organizations").on([("organization_id", "id")]),
                ),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, false)
                .primary_key(&["id"])
                .relation("user", Relation::object("users").on([("user_id", "id")]))
                .relation("comments", Relation::array("comments").on([("id", "post_id")])),
        )
        .table(
            Table::new("comments", "public", "comments")
                .column("id", "id", PgType::Int4, false)
                .column("body", "body", PgType::Text, false)
                .column("post_id", "post_id", PgType::Int4, false)
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
                CREATE TABLE organizations (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL CONSTRAINT organizations_name_key UNIQUE
                );
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL CONSTRAINT users_name_key UNIQUE,
                    email TEXT,
                    organization_id INT REFERENCES organizations(id)
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL CONSTRAINT posts_title_key UNIQUE,
                    user_id INT NOT NULL REFERENCES users(id)
                );
                CREATE TABLE comments (
                    id SERIAL PRIMARY KEY,
                    body TEXT NOT NULL,
                    post_id INT NOT NULL REFERENCES posts(id)
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
async fn nested_object_on_conflict_do_nothing_links_to_existing() {
    let (engine, _c) = setup().await;

    let seeded: Value = engine
        .query(
            r#"mutation {
                 insert_users_one(object: { name: "alice", email: "old@e.com" }) { id }
               }"#,
            None,
        )
        .await
        .expect("seed ok");
    let alice_id = seeded["insert_users_one"]["id"].as_i64().unwrap();

    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "p1",
                   user: {
                     data: { name: "alice", email: "ignored@e.com" },
                     on_conflict: { constraint: "users_name_key", update_columns: [] }
                   }
                 }]) {
                   affected_rows
                   returning { title user { id email } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");

    let row = &v["insert_posts"]["returning"][0];
    assert_eq!(row["title"], json!("p1"));
    assert_eq!(row["user"]["id"].as_i64().unwrap(), alice_id);
    // Email unchanged — DO NOTHING means existing row wins.
    assert_eq!(row["user"]["email"], json!("old@e.com"));
}

#[tokio::test]
async fn nested_wrapper_unknown_key_is_error() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "t",
                   user: { data: { name: "alice" }, foo: "bar" }
                 }]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(
        msg.contains("'data' and 'on_conflict'"),
        "error should mention both supported keys; was: {msg}"
    );
}

#[tokio::test]
async fn nested_on_conflict_missing_constraint_is_error() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "t",
                   user: {
                     data: { name: "alice" },
                     on_conflict: { update_columns: [] }
                   }
                 }]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(
        msg.contains("'constraint'"),
        "error should mention missing constraint; was: {msg}"
    );
}

#[tokio::test]
async fn nested_object_on_conflict_do_update_updates_existing() {
    let (engine, _c) = setup().await;

    let seeded: Value = engine
        .query(
            r#"mutation {
                 insert_users_one(object: { name: "alice", email: "old@e.com" }) { id }
               }"#,
            None,
        )
        .await
        .expect("seed ok");
    let alice_id = seeded["insert_users_one"]["id"].as_i64().unwrap();

    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "p1",
                   user: {
                     data: { name: "alice", email: "new@e.com" },
                     on_conflict: { constraint: "users_name_key", update_columns: ["email"] }
                   }
                 }]) {
                   returning { title user { id email } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    let row = &v["insert_posts"]["returning"][0];
    assert_eq!(row["title"], json!("p1"));
    assert_eq!(row["user"]["id"].as_i64().unwrap(), alice_id);
    assert_eq!(row["user"]["email"], json!("new@e.com"));
}

#[tokio::test]
async fn nested_array_on_conflict_do_update_updates_existing() {
    let (engine, _c) = setup().await;

    let seeded_bob: Value = engine
        .query(
            r#"mutation { insert_users_one(object: { name: "bob" }) { id } }"#,
            None,
        )
        .await
        .expect("seed bob");
    let bob_id = seeded_bob["insert_users_one"]["id"].as_i64().unwrap();

    let _: Value = engine
        .query(
            &format!(
                r#"mutation {{
                     insert_posts_one(object: {{ title: "fixed-slug", user_id: {bob_id} }}) {{ id }}
                   }}"#
            ),
            None,
        )
        .await
        .expect("seed post");

    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "carol",
                   posts: {
                     data: [
                       { title: "fixed-slug" },
                       { title: "carol-fresh" }
                     ],
                     on_conflict: {
                       constraint: "posts_title_key",
                       update_columns: ["user_id"]
                     }
                   }
                 }]) {
                   returning {
                     id
                     name
                     posts(order_by: [{ title: asc }]) { title user_id }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");

    let row = &v["insert_users"]["returning"][0];
    assert_eq!(row["name"], json!("carol"));
    let carol_id = row["id"].as_i64().unwrap();
    let posts = row["posts"].as_array().unwrap();
    assert_eq!(posts.len(), 2);
    // Both posts should now belong to carol — the existing post was DO UPDATE'd.
    for p in posts {
        assert_eq!(p["user_id"].as_i64().unwrap(), carol_id);
    }
}

#[tokio::test]
async fn nested_array_on_conflict_do_nothing_preserves_existing() {
    let (engine, _c) = setup().await;

    let seeded: Value = engine
        .query(
            r#"mutation { insert_users_one(object: { name: "alice" }) { id } }"#,
            None,
        )
        .await
        .expect("seed alice");
    let alice_id = seeded["insert_users_one"]["id"].as_i64().unwrap();
    let _: Value = engine
        .query(
            &format!(
                r#"mutation {{
                     insert_posts_one(object: {{ title: "already-there", user_id: {alice_id} }}) {{ id }}
                   }}"#
            ),
            None,
        )
        .await
        .expect("seed existing post");

    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "bob",
                   posts: {
                     data: [
                       { title: "already-there" },
                       { title: "bob-fresh" }
                     ],
                     on_conflict: { constraint: "posts_title_key", update_columns: [] }
                   }
                 }]) {
                   returning {
                     id
                     name
                     posts(order_by: [{ title: asc }]) { title user_id }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");

    let row = &v["insert_users"]["returning"][0];
    assert_eq!(row["name"], json!("bob"));
    let bob_id = row["id"].as_i64().unwrap();

    let posts = row["posts"].as_array().unwrap();
    assert_eq!(posts.len(), 2);

    let already = posts
        .iter()
        .find(|p| p["title"] == json!("already-there"))
        .expect("already-there present");
    assert_eq!(
        already["user_id"].as_i64().unwrap(),
        alice_id,
        "DO NOTHING preserves original owner"
    );

    let fresh = posts
        .iter()
        .find(|p| p["title"] == json!("bob-fresh"))
        .expect("bob-fresh present");
    assert_eq!(fresh["user_id"].as_i64().unwrap(), bob_id);
}

#[tokio::test]
async fn top_level_on_conflict_do_nothing_unchanged() {
    let (engine, _c) = setup().await;

    let _: Value = engine
        .query(
            r#"mutation { insert_users_one(object: { name: "dup" }) { id } }"#,
            None,
        )
        .await
        .expect("seed dup");

    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(
                   objects: [{ name: "dup" }],
                   on_conflict: { constraint: "users_name_key", update_columns: [] }
                 ) {
                   affected_rows
                   returning { name }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(0));
    assert_eq!(v["insert_users"]["returning"], json!([]));
}

#[tokio::test]
async fn two_level_nested_on_conflict_on_innermost() {
    let (engine, _c) = setup().await;

    let seeded: Value = engine
        .query(
            r#"mutation { insert_organizations_one(object: { name: "acme" }) { id } }"#,
            None,
        )
        .await
        .expect("seed org");
    let acme_id = seeded["insert_organizations_one"]["id"].as_i64().unwrap();

    let v: Value = engine
        .query(
            r#"mutation {
                 insert_posts(objects: [{
                   title: "p1",
                   user: { data: {
                     name: "alice",
                     organization: {
                       data: { name: "acme" },
                       on_conflict: { constraint: "organizations_name_key", update_columns: [] }
                     }
                   } }
                 }]) {
                   returning {
                     title
                     user { name organization { id name } }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");

    let row = &v["insert_posts"]["returning"][0];
    assert_eq!(row["title"], json!("p1"));
    assert_eq!(row["user"]["name"], json!("alice"));
    assert_eq!(row["user"]["organization"]["id"].as_i64().unwrap(), acme_id);
    assert_eq!(row["user"]["organization"]["name"], json!("acme"));
}
