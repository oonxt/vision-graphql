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
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, false)
                .primary_key(&["id"])
                .relation("posts", Relation::array("posts").on([("id", "user_id")]))
                .relation("reactions", Relation::array("reactions").on([("id", "user_id")])),
        )
        .table(
            Table::new("posts", "public", "posts")
                .column("id", "id", PgType::Int4, false)
                .column("title", "title", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, false)
                .column("published", "published", PgType::Bool, true)
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
        .table(
            Table::new("reactions", "public", "reactions")
                .column("id", "id", PgType::Int4, false)
                .column("kind", "kind", PgType::Text, false)
                .column("user_id", "user_id", PgType::Int4, false)
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
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL
                );
                CREATE TABLE posts (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    user_id INT NOT NULL REFERENCES users(id),
                    published BOOL
                );
                CREATE TABLE comments (
                    id SERIAL PRIMARY KEY,
                    body TEXT NOT NULL,
                    post_id INT NOT NULL REFERENCES posts(id)
                );
                CREATE TABLE reactions (
                    id SERIAL PRIMARY KEY,
                    kind TEXT NOT NULL,
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
async fn insert_one_parent_with_one_child() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [
                   { name: "alice", posts: { data: [{ title: "p1" }] } }
                 ]) {
                   affected_rows
                   returning { id name posts { title } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(2));
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], json!("alice"));
    let posts = rows[0]["posts"].as_array().unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0]["title"], json!("p1"));
}

#[tokio::test]
async fn nested_insert_missing_data_key_is_error() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{ name: "x", posts: {} }]) {
                   affected_rows
                 }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(msg.contains("'data'"), "error was: {msg}");
}

#[tokio::test]
async fn nested_insert_non_array_data_is_error() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{ name: "x", posts: { data: {} } }]) {
                   affected_rows
                 }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(msg.contains("expected array"), "error was: {msg}");
}

#[tokio::test]
async fn nested_insert_child_fk_column_rejected() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation {
                 insert_users(objects: [
                   { name: "x", posts: { data: [{ title: "t", user_id: 99 }] } }
                 ]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected error");
    let msg = format!("{err}");
    assert!(msg.contains("populated from the parent"), "error was: {msg}");
}

#[tokio::test]
async fn nested_insert_multi_parent_correlation() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [
                   { name: "u1", posts: { data: [{ title: "u1-p1" }, { title: "u1-p2" }] } },
                   { name: "u2", posts: { data: [{ title: "u2-p1" }] } }
                 ]) {
                   affected_rows
                   returning {
                     name
                     posts(order_by: [{ id: asc }]) { title }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(5));
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 2);

    let u1 = rows.iter().find(|r| r["name"] == json!("u1")).expect("u1");
    let u1_titles: Vec<_> = u1["posts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["title"].clone())
        .collect();
    assert_eq!(u1_titles, vec![json!("u1-p1"), json!("u1-p2")]);

    let u2 = rows.iter().find(|r| r["name"] == json!("u2")).expect("u2");
    let u2_titles: Vec<_> = u2["posts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["title"].clone())
        .collect();
    assert_eq!(u2_titles, vec![json!("u2-p1")]);
}

#[tokio::test]
async fn nested_insert_correlation_stress() {
    let (engine, _c) = setup().await;

    let mutation = r#"mutation {
        insert_users(objects: [
          { name: "a", posts: { data: [{ title: "a-child" }] } },
          { name: "b", posts: { data: [{ title: "b-child" }] } },
          { name: "c", posts: { data: [{ title: "c-child" }] } },
          { name: "d", posts: { data: [{ title: "d-child" }] } },
          { name: "e", posts: { data: [{ title: "e-child" }] } }
        ]) {
          affected_rows
          returning { id name }
        }
      }"#;
    let v: Value = engine.query(mutation, None).await.expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(10));
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 5);

    for r in rows {
        let name = r["name"].as_str().unwrap().to_string();
        let id = r["id"].as_i64().unwrap();

        let v2: Value = engine
            .query(
                &format!(
                    r#"query {{ posts(where: {{ user_id: {{_eq: {id} }} }}) {{ title }} }}"#
                ),
                None,
            )
            .await
            .expect("lookup ok");
        let titles: Vec<_> = v2["posts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["title"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(titles, vec![format!("{name}-child")]);
    }
}

#[tokio::test]
async fn nested_insert_three_levels() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "a",
                   posts: {
                     data: [{
                       title: "p1",
                       comments: { data: [{ body: "c1" }, { body: "c2" }] }
                     }]
                   }
                 }]) {
                   affected_rows
                   returning {
                     name
                     posts {
                       title
                       comments(order_by: [{ id: asc }]) { body }
                     }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(4));
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let posts = rows[0]["posts"].as_array().unwrap();
    assert_eq!(posts.len(), 1);
    let comments = posts[0]["comments"].as_array().unwrap();
    assert_eq!(comments.len(), 2);
    assert_eq!(comments[0]["body"], json!("c1"));
    assert_eq!(comments[1]["body"], json!("c2"));
}

#[tokio::test]
async fn nested_insert_sibling_array_relations() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "a",
                   posts:     { data: [{ title: "p1" }] },
                   reactions: { data: [{ kind: "like" }, { kind: "wow" }] }
                 }]) {
                   affected_rows
                   returning {
                     name
                     posts     { title }
                     reactions(order_by: [{ id: asc }]) { kind }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(4));
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows[0]["posts"].as_array().unwrap().len(), 1);
    let kinds: Vec<_> = rows[0]["reactions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["kind"].clone())
        .collect();
    assert_eq!(kinds, vec![json!("like"), json!("wow")]);
}

#[tokio::test]
async fn nested_insert_unrelated_sibling_returns_empty() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "a",
                   posts: { data: [{ title: "p1" }] }
                 }]) {
                   returning {
                     name
                     posts     { title }
                     reactions { kind }
                   }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    let rows = v["insert_users"]["returning"].as_array().unwrap();
    assert_eq!(rows[0]["posts"].as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["reactions"], json!([]));
}

#[tokio::test]
async fn nested_insert_empty_children_array() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "a",
                   posts: { data: [] }
                 }]) {
                   affected_rows
                   returning { name posts { title } }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    assert_eq!(v["insert_users"]["affected_rows"], json!(1));
    assert_eq!(v["insert_users"]["returning"][0]["name"], json!("a"));
    assert_eq!(v["insert_users"]["returning"][0]["posts"], json!([]));
}

#[tokio::test]
async fn nested_insert_one_with_children() {
    let (engine, _c) = setup().await;
    let v: Value = engine
        .query(
            r#"mutation {
                 insert_users_one(object: {
                   name: "solo",
                   posts: { data: [{ title: "p1" }, { title: "p2" }] }
                 }) {
                   id
                   name
                   posts(order_by: [{ id: asc }]) { title }
                 }
               }"#,
            None,
        )
        .await
        .expect("mutation ok");
    let one = &v["insert_users_one"];
    assert_eq!(one["name"], json!("solo"));
    let titles: Vec<_> = one["posts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["title"].clone())
        .collect();
    assert_eq!(titles, vec![json!("p1"), json!("p2")]);
}

#[tokio::test]
async fn nested_insert_rolls_back_on_child_failure() {
    let (engine, _c) = setup().await;

    let err = engine
        .query(
            r#"mutation {
                 insert_users(objects: [{
                   name: "rb",
                   posts: { data: [{
                     title: "t",
                     comments: { data: [{ body: null }] }
                   }] }
                 }]) { affected_rows }
               }"#,
            None,
        )
        .await
        .err()
        .expect("expected DB error");
    let _ = err;

    let v: Value = engine
        .query(
            r#"query { users(where: {name: {_eq: "rb"}}) { id } }"#,
            None,
        )
        .await
        .expect("lookup ok");
    assert_eq!(v["users"], json!([]));
}

#[tokio::test]
async fn multi_field_mutation_returning_reads_real_table_not_prior_insert_cte() {
    let (engine, _c) = setup().await;

    // Seed: insert a user directly via a mutation (pre-existing data),
    // then give them a post via a separate mutation. After setup, the
    // database contains user "seed" with one post titled "seeded-post".
    let setup_v: Value = engine
        .query(
            r#"mutation {
                 insert_users_one(object: { name: "seed" }) { id }
               }"#,
            None,
        )
        .await
        .expect("seed user");
    let seed_user_id = setup_v["insert_users_one"]["id"].as_i64().unwrap();

    let _: Value = engine
        .query(
            &format!(
                r#"mutation {{
                     insert_posts_one(object: {{ title: "seeded-post", user_id: {seed_user_id} }}) {{ id }}
                   }}"#
            ),
            None,
        )
        .await
        .expect("seed post");

    // The bug-triggering mutation: two fields.
    // Field 1 (alpha): insert_users with nested posts — populates
    //   ctx.inserted_ctes["posts"] = "m0_posts"
    // Field 2 (beta): update_users_by_pk on the SEED user — its returning
    //   selects posts{title}. Without the fix, the posts subquery reads
    //   from m0_posts (alpha's freshly-inserted posts), which is wrong;
    //   it should read from public.posts and return the "seeded-post".
    let mutation = format!(
        r#"mutation {{
             alpha: insert_users(objects: [{{
               name: "alpha",
               posts: {{ data: [{{ title: "alpha-p1" }}] }}
             }}]) {{
               returning {{ name posts {{ title }} }}
             }}
             beta: update_users_by_pk(
               pk_columns: {{ id: {seed_user_id} }},
               _set: {{ name: "seed-renamed" }}
             ) {{
               name
               posts {{ title }}
             }}
           }}"#
    );
    let v: Value = engine.query(&mutation, None).await.expect("mutation ok");

    // Alpha is fine — its nested returning should show the post it just inserted.
    let alpha_titles: Vec<_> = v["alpha"]["returning"][0]["posts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["title"].clone())
        .collect();
    assert_eq!(alpha_titles, vec![json!("alpha-p1")]);

    // Beta is the bug scenario. Its posts{title} should show "seeded-post"
    // (pre-existing data read from the real table), NOT "alpha-p1" (which
    // would mean it incorrectly read from alpha's insert CTE).
    assert_eq!(v["beta"]["name"], json!("seed-renamed"));
    let beta_titles: Vec<_> = v["beta"]["posts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["title"].clone())
        .collect();
    assert_eq!(
        beta_titles,
        vec![json!("seeded-post")],
        "beta should see its own pre-existing post, not alpha's newly-inserted one"
    );
}
