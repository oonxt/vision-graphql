//! What a failed request looks like on the wire, and what it does not say.

use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres::Postgres, testcontainers::runners::AsyncRunner};
use vision_graphql::error::ErrorCode;
use vision_graphql::{Engine, Schema};

async fn setup() -> (
    Engine,
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
) {
    let c = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .unwrap();
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .unwrap();
    sqlx::raw_sql(
        r#"CREATE TABLE users (
               id SERIAL PRIMARY KEY,
               email TEXT UNIQUE,
               secret_ssn TEXT
           );
           INSERT INTO users (email) VALUES ('taken@example.com');"#,
    )
    .execute(&pool)
    .await
    .unwrap();
    let engine = Engine::new(
        pool.clone(),
        Schema::introspect(&pool).await.unwrap().build(),
    );
    (engine, c)
}

/// PostgreSQL's own message names the constraint, the table, and sometimes a
/// file and line inside the server. The SQLSTATE is the part that is both
/// standard and safe, so that is the part that travels.
#[tokio::test]
async fn a_database_error_travels_as_its_sqlstate_and_nothing_else() {
    let (engine, _c) = setup().await;
    let err = engine
        .query(
            r#"mutation { insert_users(objects: [{email: "taken@example.com"}]) {
                 affected_rows } }"#,
            None,
        )
        .await
        .unwrap_err();

    assert_eq!(err.code(), ErrorCode::DatabaseError);

    // The full text is there for the log…
    let logged = format!("{err}");
    assert!(logged.contains("users_email_key"), "{logged}");

    // …and not in what goes back.
    let body = err.to_graphql_response();
    let message = body["errors"][0]["message"].as_str().unwrap();
    assert!(!message.contains("users_email_key"), "{message}");
    assert!(!message.contains("users"), "{message}");
    assert!(
        message.contains("23505"),
        "the SQLSTATE is the useful part: {message}"
    );
    assert_eq!(body["errors"][0]["extensions"]["sqlstate"], "23505");
    assert_eq!(body["errors"][0]["extensions"]["code"], "DATABASE_ERROR");
    assert!(body.get("data").is_none(), "{body}");
}

/// A request error names the position the caller wrote, because that is what
/// makes it fixable.
#[tokio::test]
async fn request_errors_carry_a_code_and_a_position() {
    let (engine, _c) = setup().await;

    let cases: Vec<(&str, ErrorCode)> = vec![
        ("{ users { nonexistent } }", ErrorCode::ValidationFailed),
        (
            "{ users(where: {id: {_eq: null}}) { id } }",
            ErrorCode::ValidationFailed,
        ),
        (
            "query($n: Int!) { users(limit: $n) { id } }",
            ErrorCode::VariableMissing,
        ),
        ("{ users { id }", ErrorCode::ParseFailed),
    ];
    for (q, expected) in cases {
        let err = engine.query(q, None).await.unwrap_err();
        assert_eq!(err.code(), expected, "for {q}");
        let body = err.to_graphql_response();
        assert_eq!(body["errors"][0]["extensions"]["code"], expected.as_str());
    }

    // The position points at what to change.
    let err = engine
        .query("{ users(where: {id: {_eq: \"x\"}}) { id } }", None)
        .await
        .unwrap_err();
    assert_eq!(
        err.to_graphql_response()["errors"][0]["extensions"]["path"],
        "where.id"
    );
}

/// A column the schema withholds must be indistinguishable from one that was
/// never there. An error that said "hidden" rather than "unknown" would confirm
/// the column exists, which is most of what hiding it was for.
#[tokio::test]
async fn a_hidden_column_is_reported_as_unknown_not_as_hidden() {
    let c = Postgres::default()
        .with_tag("17.4-alpine")
        .start()
        .await
        .unwrap();
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE users (id SERIAL PRIMARY KEY, secret_ssn TEXT);")
        .execute(&pool)
        .await
        .unwrap();

    let cfg = vision_graphql::schema::config::parse(
        r#"
        [tables.users]
        hide_columns = ["secret_ssn"]
        "#,
    )
    .unwrap();
    let schema = Schema::introspect(&pool)
        .await
        .unwrap()
        .apply_config(&cfg)
        .build();
    let engine = Engine::new(pool, schema);

    let hidden = engine
        .query("{ users { secret_ssn } }", None)
        .await
        .unwrap_err();
    let never = engine
        .query("{ users { no_such_column } }", None)
        .await
        .unwrap_err();

    assert_eq!(hidden.code(), never.code());
    let a = hidden.to_graphql_response()["errors"][0]["message"]
        .as_str()
        .unwrap()
        .replace("secret_ssn", "X");
    let b = never.to_graphql_response()["errors"][0]["message"]
        .as_str()
        .unwrap()
        .replace("no_such_column", "X");
    assert_eq!(a, b, "the two must read the same but for the name");
    assert!(!a.contains("hidden"), "{a}");
}
