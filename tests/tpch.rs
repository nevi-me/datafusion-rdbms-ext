// TODO: this would live better in a benchmark.
// The purpose is to check how many queries pass or fail.

use std::sync::Arc;

use datafusion::{arrow::util::pretty, prelude::SessionContext};
use datafusion_rdbms_ext::{
    make_rdbms_context,
    sqldb::{postgres::PostgresConnection, ConnectionParameters},
};

async fn prepare_context() -> SessionContext {
    let ctx = make_rdbms_context();
    // let mut ctx = ExecutionContext::new();

    // Register catalog
    let connection = PostgresConnection::new(
        ConnectionParameters::new("postgresql://postgres:password@localhost/bench"),
        "bench",
    );
    let catalog = connection.load_catalog().await.unwrap();

    ctx.register_catalog("bench", Arc::new(catalog));

    ctx
}

async fn run_query(ctx: &mut SessionContext, query: &str) {
    let df = ctx.sql(query).await.unwrap();
    let plan = df.to_logical_plan().unwrap();
    let optimized_plan = ctx.optimize(&plan).unwrap();

    println!("Logical plan:\n\n {:?}", plan);
    println!("Optimized plan:\n\n {:?}", optimized_plan);

    let batches = df.collect().await.unwrap();

    if !batches.is_empty() {
        let batch = batches[0].slice(0, batches[0].num_rows().min(10));
        pretty::print_batches(&[batch]).unwrap();
    } else {
        // Print an empty result
        pretty::print_batches(&batches).unwrap();
    }
}

// TODO: make it async
fn get_query_from_file(file: &str) -> String {
    let path = format!("./testdata/queries/{file}");
    let data = std::fs::read(&path).unwrap();
    String::from_utf8(data).unwrap()
}

#[tokio::test]
async fn test_tpch_q1() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q1.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q2() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q2.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q3() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q3.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q4() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q4.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q5() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q5.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q6() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q6.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q7() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q7.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q8() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q8.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q9() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q9.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q10() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q10.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q11() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q11.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q12() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q12.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q13() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q13.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q14() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q14.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q15() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q15.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q16() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q16.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q17() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q17.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q18() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q18.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q19() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q19.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q20() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q20.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q21() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q21.sql");
    run_query(&mut ctx, &query).await;
}

#[tokio::test]
async fn test_tpch_q22() {
    let mut ctx = prepare_context().await;
    let query = get_query_from_file("q22.sql");
    run_query(&mut ctx, &query).await;
}
