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
- [ ] Accessing tables in the normal `LogicalPlan::TableScan` fashion, with filter and projection pushdown
- [ ] Custom logical and physical nodes/extensions to recognise when plans that could be pushed to the database, can be.
  - [ ] Aggregations and window functions, as these can significantly reduce the volume of data coming from the source
  - [ ] Joins between tables in the same database (or server in MSSQL case)
- [ ] Some offline/online gathering of statistics to aid with choosing when to read & compute on DF vs on the DB

## Target Databases

It is tedious to write connectors for every database out there, I've previously tried.
Arrow has Flight SQL, which could be a first-class citizen for connections. While databases do not yet support it, I could leverage `connector-x` or some of my old SQL->Arrow work.

## What's Working?

Nothing. I mostly have code from the DataFusion TopK planner, which I've been converting to support my desired use-case.
There's some changes needed in DF, such as being able to specify a table source so we can know if we're reading from a DB or flat file.

The dirty code can be found in the `origins` branch.

## Counter-Points

DataFusion excels at retrieving data and performing compute on it in-memory. So why would one want to pass queries through to a database?

Data transfers can be slow for large volumes, so finding more ways of reducing the transferred data can speed DF queries up.
We see this benefit even in flat files, where being able to evaluate filters at source can speed queries significantly.
A heavily optimised OLAP database can often perform joins + aggregates with a lower cost than reading N tables, joining and aggregating them on the client.

I personally see DF (more Ballista) as very fitting in a federated query engine space. I would want to replace Dremio, SAP HANA, Presto, etc with it. For DF/Ballista to reach a position where one can replace these engines with it, it'd need a lot of data source connectivity. Not only for pulling data, but also for efficiently choosing when to push compute to source vs loading data.

For example, I've previously worked on a draft of a MongoDB DataFusion source, which worked reasonably, and with effort and tuning could grant the user a neat SQL interface into MongoDB data.
