use vision_graphql::{Engine, Schema};

mod common;

/// The query graphql-js `getIntrospectionQuery()` produces, trimmed of the
/// deprecation flags but keeping the fragments and the seven-deep ofType chain.
const INTROSPECTION_QUERY: &str = r#"
query IntrospectionQuery {
  __schema {
    queryType { name }
    mutationType { name }
    subscriptionType { name }
    types { ...FullType }
    directives { name locations args { ...InputValue } }
  }
}
fragment FullType on __Type {
  kind name description
  fields(includeDeprecated: true) {
    name description
    args { ...InputValue }
    type { ...TypeRef }
    isDeprecated deprecationReason
  }
  inputFields { ...InputValue }
  interfaces { ...TypeRef }
  enumValues(includeDeprecated: true) { name description isDeprecated deprecationReason }
  possibleTypes { ...TypeRef }
}
fragment InputValue on __InputValue {
  name description type { ...TypeRef } defaultValue
}
fragment TypeRef on __Type {
  kind name
  ofType { kind name ofType { kind name ofType { kind name
    ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } } }
}
"#;

async fn engine(introspection: bool) -> (Engine, common::TestDb) {
    let db = common::fresh_db().await;
    let pool = db.pool.clone();
    sqlx::raw_sql(
        r#"CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT UNIQUE, data JSONB);
           CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INT NOT NULL REFERENCES users(id), score NUMERIC);
           CREATE VIEW top AS SELECT id, score FROM posts;
           INSERT INTO users (name) VALUES ('alice');"#,
    ).execute(&pool).await.unwrap();
    let builder = Schema::introspect(&pool).await.unwrap();
    let schema = if introspection {
        builder.enable_introspection().build()
    } else {
        builder.build()
    };
    (Engine::new(pool, schema), db)
}

#[tokio::test]
async fn graphiql_introspection_query_is_answered() {
    let (engine, _db) = engine(true).await;
    let v = engine.query(INTROSPECTION_QUERY, None).await.unwrap();
    let s = &v["__schema"];
    assert_eq!(s["queryType"]["name"], "query_root");
    assert_eq!(s["mutationType"]["name"], "mutation_root");
    assert_eq!(s["subscriptionType"], serde_json::Value::Null);

    let types = s["types"].as_array().unwrap();
    let names: Vec<&str> = types.iter().map(|t| t["name"].as_str().unwrap()).collect();

    // A row type carries its columns and its relations, each with the arguments
    // the engine really accepts.
    let users = types.iter().find(|t| t["name"] == "users").unwrap();
    let fields = users["fields"].as_array().unwrap();
    let id = fields.iter().find(|f| f["name"] == "id").unwrap();
    assert_eq!(id["type"]["kind"], "NON_NULL");
    assert_eq!(id["type"]["ofType"]["name"], "Int");
    let posts = fields.iter().find(|f| f["name"] == "posts").unwrap();
    assert_eq!(posts["type"]["ofType"]["ofType"]["ofType"]["name"], "posts");
    let args: Vec<&str> = posts["args"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        args,
        vec!["where", "order_by", "limit", "offset", "distinct_on"]
    );

    // A jsonb column publishes `path`, and only a jsonb column does.
    let data = fields.iter().find(|f| f["name"] == "data").unwrap();
    assert_eq!(data["args"][0]["name"], "path");

    // The unique constraint introspection found is nameable in on_conflict.
    let constraints = types
        .iter()
        .find(|t| t["name"] == "users_constraint")
        .unwrap();
    let values: Vec<&str> = constraints["enumValues"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["name"].as_str().unwrap())
        .collect();
    assert!(values.contains(&"users_name_key"), "{values:?}");

    // No directives are implemented, so none are advertised.
    assert_eq!(s["directives"], serde_json::json!([]));

    // Every type a field or argument refers to is itself in the list, or a
    // client walking the schema hits a hole.
    for t in types {
        if let Some(fs) = t["fields"].as_array() {
            for f in fs {
                let mut ty = &f["type"];
                while ty["name"].is_null() {
                    ty = &ty["ofType"];
                }
                let n = ty["name"].as_str().unwrap();
                assert!(names.contains(&n), "field type {n} is not in the type list");
            }
        }
    }

    // The view is readable but carries no mutation fields.
    let mroot = types.iter().find(|t| t["name"] == "mutation_root").unwrap();
    let mfields: Vec<&str> = mroot["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert!(
        !mfields.iter().any(|f| f.contains("top")),
        "view must not be writable"
    );
}

#[tokio::test]
async fn introspection_is_off_unless_enabled_and_mixes_with_data() {
    let (e1, _db1) = engine(false).await;
    let err = e1
        .query("{ __schema { queryType { name } } }", None)
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("introspection is disabled"));

    let (e2, _db2) = engine(true).await;
    // One request, one statement: introspection beside real rows.
    let v = e2
        .query(
            "{ __type(name: \"users\") { name kind } users { id name } }",
            None,
        )
        .await
        .unwrap();
    assert_eq!(v["__type"]["name"], "users");
    assert_eq!(v["users"][0]["name"], "alice");

    // Directives are rejected rather than silently ignored, which is what makes
    // the empty directive list above honest.
    let err = e2
        .query("{ users { id @include(if: true) } }", None)
        .await
        .unwrap_err();
    assert!(
        format!("{err}").contains("directives are not supported"),
        "{err}"
    );
}
