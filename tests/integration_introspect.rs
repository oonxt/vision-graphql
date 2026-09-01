use serde_json::Value;
use vision_graphql::schema::Schema;
use vision_graphql::Engine;

mod common;

async fn setup_pool() -> (sqlx::PgPool, common::TestDb) {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();

    sqlx::raw_sql(
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
    .execute(&pool)
    .await
    .expect("seed");
    (pool, db)
}

#[tokio::test]
async fn introspect_auto_derives_relations() {
    let (pool, _db) = setup_pool().await;
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

/// Uniqueness introspection must see what Postgres actually accepts as proof
/// of uniqueness, not just `information_schema` constraints: a plain
/// `CREATE UNIQUE INDEX` is a valid FK target, so a schema relying on one is
/// provably fine and must not warn — while a *partial* unique index pins rows
/// only under its predicate and must not count.
#[tokio::test]
async fn unique_index_counts_for_warnings_partial_does_not() {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();
    sqlx::raw_sql(
        r#"
        CREATE TABLE sdict (serial TEXT NOT NULL);
        CREATE UNIQUE INDEX sdict_serial_idx ON sdict(serial);
        CREATE TABLE half_dict (serial TEXT NOT NULL, active BOOL NOT NULL);
        CREATE UNIQUE INDEX half_dict_serial_idx ON half_dict(serial) WHERE active;
        -- an FK referencing the plain unique index: Postgres accepts it, and
        -- the auto-derived object relation must come out silent.
        CREATE TABLE refs (
            id SERIAL PRIMARY KEY,
            serial TEXT NOT NULL REFERENCES sdict(serial)
        );
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed");

    let cfg = vision_graphql::schema::config::parse(
        r#"
        [[tables.refs.relations]]
        name = "half"
        kind = "object"
        target = "half_dict"
        mapping = [["serial", "serial"]]
        "#,
    )
    .expect("overlay");
    let schema = Schema::introspect(&pool)
        .await
        .expect("introspect")
        .apply_config(&cfg)
        .build();

    let warnings = schema.warnings();
    assert_eq!(warnings.len(), 1, "got {warnings:?}");
    match &warnings[0] {
        vision_graphql::SchemaWarning::NonDeterministicObjectRelation {
            table, relation, ..
        } => assert_eq!((table.as_str(), relation.as_str()), ("refs", "half")),
        other => panic!("unexpected warning: {other:?}"),
    }
}

#[tokio::test]
async fn introspect_runs_queries_end_to_end() {
    let (pool, _db) = setup_pool().await;
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
    let (pool, _db) = setup_pool().await;
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
