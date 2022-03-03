//! This tests that avarious joins can be detected and pushed
//! to the RDMBS source.

use std::sync::Arc;

use datafusion::{
    error::Result,
    execution::runtime_env::{RuntimeConfig, RuntimeEnv},
    physical_plan::common,
};
use datafusion_rdbms_ext::{
    make_rdbms_context,
    sqldb::{postgres::PostgresConnection, ConnectionParameters},
};

#[tokio::test]
async fn test_simple_join_pushdown() -> Result<()> {
    let mut ctx = make_rdbms_context();

    // Register catalog
    let connection = PostgresConnection::new(
        ConnectionParameters::new("postgresql://postgres:password@localhost/bench"),
        "bench",
    );
    let catalog = connection.load_catalog().await.unwrap();

    ctx.register_catalog("bench", Arc::new(catalog));

    let query = r#"
    select c_custkey, c_name, n_name
    from bench.public.customer 
    inner join bench.public.nation
    on c_nationkey = n_nationkey
    "#;

    // let query = "select cast(p_retailprice as double precision) from bench.public.part";

    let plan = ctx.sql(query).await.unwrap();
    let plan = plan.to_logical_plan();

    dbg!(&plan);

    let optimized_plan = ctx.optimize(&plan)?;

    dbg!(&optimized_plan);

    let physical_plan = ctx.create_physical_plan(&optimized_plan).await.unwrap();
    let result = physical_plan
        .execute(
            1,
            Arc::new(RuntimeEnv::new(RuntimeConfig::default()).unwrap()),
        )
        .await
        .unwrap();

    let batches = common::collect(result).await.unwrap();

    dbg!(batches);

    Ok(())
}
