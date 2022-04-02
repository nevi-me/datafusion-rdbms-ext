//! This tests that avarious joins can be detected and pushed
//! to the RDMBS source.

use std::sync::Arc;

use datafusion::{arrow::util::pretty, error::Result};
use datafusion_rdbms_ext::{
    make_rdbms_context,
    sqldb::{postgres::PostgresConnection, ConnectionParameters},
};

#[tokio::test]
async fn test_simple_join_pushdown() -> Result<()> {
    let ctx = make_rdbms_context();

    // Register catalog
    let connection = PostgresConnection::new(
        ConnectionParameters::new("postgresql://postgres:password@localhost/bench"),
        "bench",
    );
    let catalog = connection.load_catalog().await.unwrap();

    ctx.register_catalog("bench", Arc::new(catalog));

    let query = r#"
    select *
    from bench.public.customer 
    inner join bench.public.nation
    on c_nationkey = n_nationkey
    "#;

    let df = ctx.sql(query).await.unwrap();
    let plan = df.to_logical_plan();
    let optimized_plan = ctx.optimize(&plan)?;

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

    Ok(())
}
