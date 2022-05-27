//! This tests that avarious joins can be detected and pushed
//! to the RDMBS source.

use std::sync::Arc;

use datafusion::{arrow::util::pretty, error::Result};
use datafusion_rdbms_ext::{
    make_rdbms_context,
    sqldb::{postgres::PostgresConnection, ConnectionParameters},
};

#[tokio::test]
async fn test_aggregate_group1() -> Result<()> {
    let ctx = make_rdbms_context();

    // Register catalog
    let connection = PostgresConnection::new(
        ConnectionParameters::new("postgresql://postgres:password@localhost/bench"),
        "bench",
    );
    let catalog = connection.load_catalog().await.unwrap();

    ctx.register_catalog("bench", Arc::new(catalog));

    let query = r#"
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        bench.public.lineitem
    where
        l_shipdate <= date '1998-09-02'
    group by
        l_returnflag,
        l_linestatus
    "#;

    let df = ctx.sql(query).await.unwrap();
    let plan = df.to_logical_plan().unwrap();
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
