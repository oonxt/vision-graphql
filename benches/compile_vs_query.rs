//! What the two halves of a request cost: the per-request path (parse → lower →
//! render → resolve) against a compiled statement's per-request remainder
//! (resolve the parameters), plus the parse cache's share of the former.

use criterion::{criterion_group, criterion_main, Criterion};
use vision_graphql::parse_cache::ParseCache;
use vision_graphql::parser::{lower, lower_with, parse_document, Bindings};
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::sql::{render, render_now};
use vision_graphql::types::{resolve_binds, Inputs};

fn sample_schema() -> Schema {
    Schema::builder()
        .table(
            Table::new("users", "public", "users")
                .column("id", "id", PgType::Int4, false)
                .column("name", "name", PgType::Text, true)
                .column("active", "active", PgType::Bool, false)
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

const Q: &str = r#"
query GetUsers($active: Boolean!) {
  users(where: {active: {_eq: $active}}, limit: 10, order_by: {id: asc}) {
    id
    name
    posts(limit: 5, where: {title: {_ilike: "%rust%"}}) {
      title
      user { id name }
    }
  }
}
"#;

fn bench(c: &mut Criterion) {
    let schema = sample_schema();
    let vars = serde_json::json!({"active": true});

    // Per-request path, as `Engine::query` runs it without a cache.
    c.bench_function("uncached_request", |b| {
        b.iter(|| {
            let doc = parse_document(Q).unwrap();
            let op = lower(&doc, &vars, None, &schema).unwrap();
            let _ = render_now(&op, &schema, &Inputs::none()).unwrap();
        });
    });

    // Same, with the document served from the parse cache.
    let cache = ParseCache::default();
    cache.get(Q).unwrap();
    c.bench_function("cached_request", |b| {
        b.iter(|| {
            let doc = cache.get(Q).unwrap();
            let op = lower(&doc, &vars, None, &schema).unwrap();
            let _ = render_now(&op, &schema, &Inputs::none()).unwrap();
        });
    });

    // All that is left of a request once the query is compiled.
    let doc = parse_document(Q).unwrap();
    let op = lower_with(&doc, Bindings::Symbolic, None, &schema).unwrap();
    let (_sql, specs) = render(&op, &schema).unwrap();
    c.bench_function("compiled_request", |b| {
        b.iter(|| {
            let _ = resolve_binds(&specs, &Inputs::variables(&vars)).unwrap();
        });
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
