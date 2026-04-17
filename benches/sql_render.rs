use criterion::{criterion_group, criterion_main, Criterion};
use vision_graphql::ast::{BoolExpr, CmpOp, Field, Operation, QueryArgs, RootBody, RootField};
use vision_graphql::schema::{PgType, Relation, Schema, Table};
use vision_graphql::sql::render;

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

fn moderately_complex_query() -> Operation {
    Operation::Query(vec![RootField {
        table: "users".into(),
        alias: "users".into(),
        args: QueryArgs {
            where_: Some(BoolExpr::Compare {
                column: "active".into(),
                op: CmpOp::Eq,
                value: serde_json::json!(true),
            }),
            limit: Some(10),
            ..Default::default()
        },
        body: RootBody::List {
            selection: vec![
                Field::Column {
                    physical: "id".into(),
                    alias: "id".into(),
                },
                Field::Column {
                    physical: "name".into(),
                    alias: "name".into(),
                },
                Field::Relation {
                    name: "posts".into(),
                    alias: "posts".into(),
                    args: QueryArgs {
                        limit: Some(5),
                        ..Default::default()
                    },
                    selection: vec![Field::Column {
                        physical: "title".into(),
                        alias: "title".into(),
                    }],
                },
            ],
        },
    }])
}

fn bench_render(c: &mut Criterion) {
    let schema = sample_schema();
    let op = moderately_complex_query();
    c.bench_function("render_moderately_complex", |b| {
        b.iter(|| {
            let _ = render(&op, &schema).unwrap();
        });
    });
}

criterion_group!(benches, bench_render);
criterion_main!(benches);
