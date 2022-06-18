# DataFusion RDBMS Extension

This is an attempt to create a DataFusion datasource that connects to SQL databases.
One can think of it as trying to achieve the same functionality that the Spark DataSource for JDBC supports.

It's an ambitious undertaking, which I have slowly been experimenting on when I have a few hours here and there, so I'm mainly putting this out in case it serves as motivation for someone else to do a better job.

## Why Implement This?

While data lakes are moving more people away from directly querying databases, there are still prevalent ETL and analytic use cases that depend on querying databases. I personally consider Spark to be the gold standard here, and in my fascination, I almost always go to the Spark UI to see what magic it's doing behind the scenes with the queries it pushes to the database.

The ongoing work on Spark convinces me that DataFusion could benefit from similar capabilities.

## Goals

Should I be able to progress with this, my goals would be to allow:

- [ ] Connecting DF to a SQL server, and letting it generate a catalog from schemas and tables
  - This is working for PostgreSQL, we rely on the `INFORMATION_SCHEMA` catalog
- [ ] Accessing tables in the normal `LogicalPlan::TableScan` fashion, with filter and projection pushdown
  - Partially working for PostgreSQL, filter predicates are somehow not working yet
- [ ] Custom logical and physical nodes/extensions to recognise when plans that could be pushed to the database, can be.
  - [ ] Aggregations and window functions, as these can significantly reduce the volume of data coming from the source
  - [ ] Joins between tables in the same database (or server in MSSQL case)
    - Joins are partially working, but have to iron out issues and find better ways of writing SQL
- [ ] Some offline/online gathering of statistics to aid with choosing when to read & compute on DF vs on the DB

## Target Databases

It is tedious to write connectors for every database out there, I've previously tried.
Arrow has Flight SQL, which could be a first-class citizen for connections. While databases do not yet support it, I could leverage `connector-x` or some of my old SQL->Arrow work.

## What's Working?

Nothing. I mostly have code from the DataFusion TopK planner, which I've been converting to support my desired use-case.
There's some changes needed in DF, such as being able to specify a table source so we can know if we're reading from a DB or flat file.

### TPC-H Derived Queries

There are some general blockers affecting the pushing down of some TPC-H derived queries.
These blockers include:

- [ ] Literals are formatted with their type, e.g. `Uint32(1)` instead of plain value (`1`), this results in invalid SQL queries generated.

Here is a list of the queries, that shows which tests pass, or what is missing.

- [x] Q1
  - [x] Passes
  - [x] Accurate result
  - Relies on fixing query with regex
- [ ] Q2
  - `ScalarSubquery` not supported in `expr_to_sql`
- [x] Q3
- [ ] Q4
  - Error: `Execution("DateIntervalExpr does not support IntervalYearMonth")`
- [X] Q5
- [-] Q6
  - [x] Passes
  - [ ] Accurate result
- [ ] Q7
- [ ] Q8
  - Query parser error
- [ ] Q9
  - Query parser error
- [-] Q10
  - [X] Passes
  - [ ] Accurate result
- [ ] Q11
   - Physical plan does not support logical expression subquery
- [ ] Q12
- [ ] Q13
- [ ] Q14
  - String literal interpreted as column name
- [ ] Q15
  - DataFusion context only supports a single statement
- [ ] Q16
  - HashJoin error on `equal_rows_elem!(Int64Array, l, r, left, right, null_equals_null)`
- [ ] Q17
  - `ScalarSubquery` not supported in `expr_to_sql`
- [ ] Q18
  - `group by x having y` is not parsed correctly
- [ ] Q19
  - DF creates a cross join, not yet supported in parser
  - `ScalarSubquery` not supported in `expr_to_sql`
- [ ] Q20
   - Physical plan does not support logical expression subquery
- [ ] Q21
  - Requires `Expr::Exists` to be supported by physical planner (should be possible to bypass this)
- [ ] Q22


## Counter-Points

DataFusion excels at retrieving data and performing compute on it in-memory. So why would one want to pass queries through to a database?

Data transfers can be slow for large volumes, so finding more ways of reducing the transferred data can speed DF queries up.
We see this benefit even in flat files, where being able to evaluate filters at source can speed queries significantly.
A heavily optimised OLAP database can often perform joins + aggregates with a lower cost than reading N tables, joining and aggregating them on the client.

I personally see DF (more Ballista) as very fitting in a federated query engine space. I would want to replace Dremio, SAP HANA, Presto, etc with it. For DF/Ballista to reach a position where one can replace these engines with it, it'd need a lot of data source connectivity. Not only for pulling data, but also for efficiently choosing when to push compute to source vs loading data.

For example, I've previously worked on a draft of a MongoDB DataFusion source, which worked reasonably, and with effort and tuning could grant the user a neat SQL interface into MondoDB data.

# Testing

We test using generated TPC-H data. It is currently up to the end-user/developer to generate this data into the `testdata/data` directory.
After generating the data, a Postgres DB `bench` can be started. It will load the data on its first run.
