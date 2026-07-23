# Spark SQL Performance Concepts in Databricks Free Edition

This tutorial covers:

1. Sorting
2. Spark execution partitioning
3. Table partitioning
4. Bucketing
5. Liquid clustering
6. Spark caching
7. A real-world implementation in Databricks Free Edition

The examples use an **online food-delivery order analytics** scenario.

> Databricks Free Edition uses managed serverless compute. Some traditional Spark features behave differently or may be unavailable compared with a classic Databricks cluster. For modern Delta tables, liquid clustering is generally preferred over traditional partitioning, bucketing, or Z-Ordering for many use cases.

---

# 1. Real-world scenario

Assume a food-delivery company receives orders from several cities.

Analysts regularly run queries such as:

- Show completed orders for Chennai.
- Calculate monthly revenue.
- Find the highest-value orders.
- Compare restaurant performance by city.
- Reuse the same filtered dataset in several reports.

```text
Raw order records
       │
       ▼
Spark DataFrame / SQL table
       │
       ├── Sort query results
       │
       ├── Repartition intermediate data
       │
       ├── Store Delta table
       │
       ├── Organize table using clustering
       │
       └── Cache frequently reused results
       ▼
Dashboard and analytical queries
```

---

# 2. Set up the Databricks Free Edition notebook

## Step 1: Create a notebook

1. Open the Databricks workspace.
2. Select **Workspace**.
3. Select **Create → Notebook**.
4. Enter the notebook name:

```text
Spark_SQL_Performance_Optimization
```

5. Select **SQL** as the default language.
6. Attach the available serverless compute.

Switch individual cells when required:

```text
%sql
```

```text
%python
```

---

# 3. Create the sample schema

```sql
CREATE SCHEMA IF NOT EXISTS workspace.spark_sql_performance;

USE CATALOG workspace;
USE SCHEMA spark_sql_performance;
```

If your catalog or schema differs:

```sql
SHOW CATALOGS;
SHOW SCHEMAS;
```

---

# 4. Create a realistic sample dataset

The following SQL creates 10,000 food-delivery orders without requiring a CSV upload.

```sql
CREATE OR REPLACE TABLE food_delivery_orders
USING DELTA
AS
SELECT
    id AS order_id,

    CONCAT('CUST-', LPAD(CAST((id % 1200) + 1 AS STRING), 5, '0'))
        AS customer_id,

    CONCAT('REST-', LPAD(CAST((id % 120) + 1 AS STRING), 4, '0'))
        AS restaurant_id,

    CASE id % 6
        WHEN 0 THEN 'Chennai'
        WHEN 1 THEN 'Bengaluru'
        WHEN 2 THEN 'Hyderabad'
        WHEN 3 THEN 'Mumbai'
        WHEN 4 THEN 'Delhi'
        ELSE 'Kochi'
    END AS city,

    CASE id % 5
        WHEN 0 THEN 'South Indian'
        WHEN 1 THEN 'North Indian'
        WHEN 2 THEN 'Chinese'
        WHEN 3 THEN 'Fast Food'
        ELSE 'Biryani'
    END AS cuisine,

    CASE id % 4
        WHEN 0 THEN 'COMPLETED'
        WHEN 1 THEN 'COMPLETED'
        WHEN 2 THEN 'CANCELLED'
        ELSE 'IN_PROGRESS'
    END AS order_status,

    ROUND(150 + ((id * 17) % 850), 2) AS order_amount,

    ROUND(20 + ((id * 7) % 80), 2) AS delivery_fee,

    DATE_ADD(DATE '2025-01-01', CAST(id % 540 AS INT))
        AS order_date,

    TIMESTAMPADD(
        HOUR,
        CAST(id % 24 AS INT),
        TIMESTAMP(DATE_ADD(DATE '2025-01-01', CAST(id % 540 AS INT)))
    ) AS order_timestamp

FROM RANGE(1, 10001);
```

Check the table:

```sql
SELECT *
FROM food_delivery_orders
LIMIT 10;
```

Count the rows:

```sql
SELECT COUNT(*) AS total_orders
FROM food_delivery_orders;
```

Expected result:

```text
10000
```

---

# 5. Sorting in Spark SQL

Sorting controls the order in which rows are returned or arranged during query processing.

| Clause | Meaning |
|---|---|
| `ORDER BY` | Globally sorts the complete result |
| `SORT BY` | Sorts rows inside each Spark partition |
| `DISTRIBUTE BY` | Redistributes rows across partitions |
| Query `CLUSTER BY` | Redistributes and sorts inside partitions |

## 5.1 ORDER BY

```sql
SELECT
    order_id,
    city,
    order_amount
FROM food_delivery_orders
ORDER BY order_amount DESC
LIMIT 10;
```

```text
Input partitions
 P1      P2      P3      P4
  │       │       │       │
  └────── Shuffle and global comparison ──────┐
                                               ▼
                                Globally ordered result
```

Use `ORDER BY` for:

- Final reports
- Top or bottom records
- Ranked output
- Exported sorted results

Global ordering is expensive because it generally requires a shuffle.

## 5.2 Multiple-column sorting

```sql
SELECT
    city,
    order_date,
    order_amount
FROM food_delivery_orders
ORDER BY
    city ASC,
    order_amount DESC;
```

## 5.3 Null handling

```sql
SELECT
    order_id,
    city,
    order_amount
FROM food_delivery_orders
ORDER BY order_amount DESC NULLS LAST;
```

---

# 6. SORT BY

`SORT BY` sorts rows inside each Spark execution partition. It does not guarantee global ordering.

```sql
SELECT
    order_id,
    city,
    order_amount
FROM food_delivery_orders
SORT BY order_amount DESC;
```

```text
Partition 1: 900, 700, 400
Partition 2: 950, 600, 200
Partition 3: 800, 500, 100
```

| Feature | `ORDER BY` | `SORT BY` |
|---|---|---|
| Global ordering | Yes | No |
| Sort inside partitions | Yes | Yes |
| Full shuffle | Usually | Not always |
| Final report use | Yes | Usually no |

---

# 7. Understanding partitioning

## 7.1 Spark execution partitions

Execution partitions are logical data blocks processed by Spark tasks.

```text
10,000 records
      │
      ▼
┌──────────┬──────────┬──────────┬──────────┐
│Partition1│Partition2│Partition3│Partition4│
│  Task 1  │  Task 2  │  Task 3  │  Task 4  │
└──────────┴──────────┴──────────┴──────────┘
```

They affect:

- Parallel processing
- Number of tasks
- Shuffle cost
- File counts
- Memory use

## 7.2 Table partitions

Table partitioning physically organizes data by column values.

```text
food_delivery_orders/
├── order_year=2025/
│   ├── order_month=1/
│   ├── order_month=2/
│   └── order_month=3/
└── order_year=2026/
    ├── order_month=1/
    └── order_month=2/
```

A query filtering partition columns can use partition pruning.

```sql
SELECT *
FROM orders_partitioned
WHERE order_year = 2026
  AND order_month = 7;
```

## 7.3 Window partitions

```sql
SELECT
    city,
    order_id,
    order_amount,

    ROW_NUMBER() OVER (
        PARTITION BY city
        ORDER BY order_amount DESC
    ) AS city_order_rank

FROM food_delivery_orders;
```

This creates calculation groups. It does not physically partition the table.

---

# 8. DISTRIBUTE BY

`DISTRIBUTE BY` redistributes rows across Spark execution partitions.

```sql
SELECT
    order_id,
    city,
    order_amount
FROM food_delivery_orders
DISTRIBUTE BY city;
```

```text
All Chennai records   ──► selected partition
All Mumbai records    ──► selected partition
All Kochi records     ──► selected partition
```

It does not guarantee:

- One city per partition
- Exactly one partition for each city
- Sorted rows
- Permanent table organization

---

# 9. Query-level CLUSTER BY

In a query:

```sql
CLUSTER BY city
```

is logically similar to:

```sql
DISTRIBUTE BY city
SORT BY city
```

Example:

```sql
SELECT
    order_id,
    city,
    order_amount
FROM food_delivery_orders
CLUSTER BY city;
```

Important distinction:

```sql
SELECT * FROM food_delivery_orders CLUSTER BY city;
```

affects query execution.

```sql
CREATE TABLE food_delivery_orders
USING DELTA
CLUSTER BY (city);
```

defines a Delta table layout strategy.

---

# 10. Traditional table partitioning implementation

```sql
CREATE OR REPLACE TABLE food_orders_partitioned
USING DELTA
PARTITIONED BY (order_year, order_month)
AS
SELECT
    order_id,
    customer_id,
    restaurant_id,
    city,
    cuisine,
    order_status,
    order_amount,
    delivery_fee,
    order_date,
    order_timestamp,
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month
FROM food_delivery_orders;
```

Inspect it:

```sql
SHOW CREATE TABLE food_orders_partitioned;
```

Query it:

```sql
SELECT
    city,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_amount), 2) AS total_order_value
FROM food_orders_partitioned
WHERE order_year = 2026
  AND order_month = 3
GROUP BY city
ORDER BY total_order_value DESC;
```

```text
All partitions
 ├── 2025/01   skipped
 ├── 2025/02   skipped
 ├── 2026/01   skipped
 ├── 2026/02   skipped
 └── 2026/03   read
```

---

# 11. When partitioning becomes harmful

Avoid partitioning by high-cardinality columns such as:

```text
order_id
customer_id
transaction_id
email_address
timestamp
```

Bad example:

```sql
PARTITIONED BY (order_id)
```

This can create many tiny directories and files.

Good candidates usually have:

- Low or moderate cardinality
- Frequent filtering
- Balanced values
- Enough rows per partition

Examples:

```text
order_year
order_month
event_date
region
country
```

---

# 12. Bucketing overview

Bucketing assigns rows to a fixed number of buckets using a hash.

```text
Customer ID
    │
    ▼
hash(customer_id) % 8
    │
    ├── Bucket 0
    ├── Bucket 1
    ├── Bucket 2
    ├── ...
    └── Bucket 7
```

Formula:

```text
bucket_number = hash(column_value) modulo number_of_buckets
```

| Partitioning | Bucketing |
|---|---|
| Uses actual column values | Uses hash values |
| Creates value-based directories | Creates fixed bucket groups |
| Good for low-cardinality columns | Handles higher-cardinality columns |
| Partition count may grow | Bucket count stays fixed |
| Supports partition pruning | Historically helps compatible joins |

Traditional syntax:

```sql
CREATE TABLE old_style_bucketed_orders (
    order_id BIGINT,
    customer_id STRING,
    city STRING,
    order_amount DOUBLE
)
USING PARQUET
CLUSTERED BY (customer_id)
INTO 8 BUCKETS;
```

This syntax is mainly useful for understanding traditional Spark bucketing. It should not be the default design for new Delta tables in Databricks Free Edition.

---

# 13. Why bucketing was used

Suppose two large tables are joined by `customer_id`.

```text
orders     ── shuffle ──┐
                        ├── join
customers  ── shuffle ──┘
```

Compatible bucketing may reduce redistribution when:

- Both tables use the same bucket column
- Bucket counts are compatible
- Spark preserves bucket metadata
- The optimizer chooses the bucketed layout

For modern Databricks Delta workloads, liquid clustering is normally a better starting point.

---

# 14. Liquid clustering

Liquid clustering organizes Delta files around selected clustering columns.

Advantages:

- No rigid value-based directory layout
- Clustering keys can be changed
- Multiple columns can be used
- Better support for changing query patterns
- Works with data-skipping statistics

---

# 15. Implement a liquid-clustered table

```sql
CREATE OR REPLACE TABLE food_orders_clustered
USING DELTA
CLUSTER BY (city, order_date)
AS
SELECT *
FROM food_delivery_orders;
```

Inspect the definition:

```sql
SHOW CREATE TABLE food_orders_clustered;
```

```sql
DESCRIBE DETAIL food_orders_clustered;
```

Run a filtering query:

```sql
SELECT
    restaurant_id,
    COUNT(*) AS completed_orders,
    ROUND(SUM(order_amount), 2) AS revenue
FROM food_orders_clustered
WHERE city = 'Chennai'
  AND order_date BETWEEN DATE '2026-01-01' AND DATE '2026-03-31'
  AND order_status = 'COMPLETED'
GROUP BY restaurant_id
ORDER BY revenue DESC
LIMIT 10;
```

```text
Query for Chennai
    │
    ▼
Use file statistics
    │
    ▼
Skip files unlikely to contain Chennai
    │
    ▼
Read fewer relevant files
```

This is called **data skipping**.

---

# 16. Run OPTIMIZE

```sql
OPTIMIZE food_orders_clustered;
```

Inspect history:

```sql
DESCRIBE HISTORY food_orders_clustered;
```

The exact availability can depend on workspace capabilities, permissions, and serverless runtime behavior.

---

# 17. Change clustering columns

```sql
ALTER TABLE food_orders_clustered
CLUSTER BY (restaurant_id, order_date);
```

Then:

```sql
OPTIMIZE food_orders_clustered;
```

Choose clustering columns frequently used in:

- `WHERE`
- `JOIN`
- `GROUP BY`

Examples:

| Query pattern | Suggested columns |
|---|---|
| City and date reports | `city, order_date` |
| Restaurant reporting | `restaurant_id, order_date` |
| Customer order history | `customer_id, order_date` |
| Status monitoring | `order_status, order_date` |

---

# 18. Spark caching overview

Caching stores computed data so it can be reused.

Without caching:

```text
Query 1 ──► Read files ──► Filter ──► Aggregate
Query 2 ──► Read files ──► Filter ──► Aggregate
Query 3 ──► Read files ──► Filter ──► Aggregate
```

With caching:

```text
First query
Files ──► Filter ──► Cache result

Later queries
Cache ──► Aggregate
Cache ──► Join
Cache ──► Report
```

---

# 19. Cache a table using SQL

```sql
CACHE TABLE food_delivery_orders;
```

Materialize the cache:

```sql
SELECT COUNT(*)
FROM food_delivery_orders;
```

Reuse it:

```sql
SELECT
    city,
    COUNT(*) AS total_orders
FROM food_delivery_orders
GROUP BY city
ORDER BY total_orders DESC;
```

```sql
SELECT
    cuisine,
    ROUND(AVG(order_amount), 2) AS average_order_amount
FROM food_delivery_orders
GROUP BY cuisine
ORDER BY average_order_amount DESC;
```

---

# 20. Cache a filtered result

```sql
CACHE TABLE completed_orders_cache AS
SELECT
    order_id,
    customer_id,
    restaurant_id,
    city,
    cuisine,
    order_amount,
    delivery_fee,
    order_date
FROM food_delivery_orders
WHERE order_status = 'COMPLETED';
```

Use the cached result:

```sql
SELECT
    city,
    COUNT(*) AS completed_orders,
    ROUND(SUM(order_amount), 2) AS revenue
FROM completed_orders_cache
GROUP BY city
ORDER BY revenue DESC;
```

```sql
SELECT
    cuisine,
    ROUND(AVG(order_amount), 2) AS average_order_value
FROM completed_orders_cache
GROUP BY cuisine
ORDER BY average_order_value DESC;
```

---

# 21. Check cache status

Use a Python cell:

```python
spark.catalog.isCached("food_delivery_orders")
```

```python
spark.catalog.isCached("completed_orders_cache")
```

---

# 22. Remove cached data

```sql
UNCACHE TABLE food_delivery_orders;
```

```sql
UNCACHE TABLE completed_orders_cache;
```

Clear all Spark caches:

```sql
CLEAR CACHE;
```

---

# 23. Free Edition cache considerations

Databricks Free Edition uses serverless compute, so explicit Spark cache behavior may differ from a classic cluster.

Possible considerations:

- `CACHE TABLE` may be limited or behave differently.
- `.cache()` and `.persist()` may be restricted in some serverless environments.
- Cache contents may disappear after session or compute recycling.
- Cache must never be treated as permanent storage.
- Platform-managed disk and SQL result caching may already improve repeated queries.

Fallback temporary view:

```sql
CREATE OR REPLACE TEMP VIEW completed_orders_view AS
SELECT *
FROM food_delivery_orders
WHERE order_status = 'COMPLETED';
```

A temporary view does not store computed data permanently.

Persistent reusable Delta table:

```sql
CREATE OR REPLACE TABLE completed_orders
USING DELTA
AS
SELECT *
FROM food_delivery_orders
WHERE order_status = 'COMPLETED';
```

---

# 24. Caching layers

| Cache type | Stores | Controlled by |
|---|---|---|
| Spark cache | Computed table or DataFrame data | User |
| Disk cache | Local copies of remote data files | Databricks |
| Query result cache | Final SQL results | Databricks SQL |

---

# 25. Use EXPLAIN

```sql
EXPLAIN
SELECT
    city,
    SUM(order_amount) AS revenue
FROM food_orders_clustered
WHERE city = 'Chennai'
GROUP BY city;
```

Detailed plan:

```sql
EXPLAIN FORMATTED
SELECT
    city,
    COUNT(*) AS completed_orders,
    SUM(order_amount) AS revenue
FROM food_orders_clustered
WHERE city IN ('Chennai', 'Bengaluru')
  AND order_status = 'COMPLETED'
GROUP BY city;
```

Look for:

| Plan term | Meaning |
|---|---|
| `Scan` | Read table files |
| `Filter` | Remove nonmatching rows |
| `Exchange` | Shuffle data |
| `Sort` | Sort rows |
| `HashAggregate` | Grouped aggregation |
| `BroadcastHashJoin` | Broadcast a small table |
| `PartitionFilters` | Partition pruning |
| `DataFilters` | Row-level filtering |

---

# 26. Compare query plans

Global ordering:

```sql
EXPLAIN FORMATTED
SELECT
    city,
    order_id,
    order_amount
FROM food_delivery_orders
ORDER BY order_amount DESC;
```

Partition-level sorting:

```sql
EXPLAIN FORMATTED
SELECT
    city,
    order_id,
    order_amount
FROM food_delivery_orders
SORT BY order_amount DESC;
```

Distribution plus local sorting:

```sql
EXPLAIN FORMATTED
SELECT
    city,
    order_id,
    order_amount
FROM food_delivery_orders
DISTRIBUTE BY city
SORT BY order_amount DESC;
```

---

# 27. Window partitioning example

Find the top three completed orders in each city:

```sql
WITH ranked_orders AS (
    SELECT
        order_id,
        city,
        restaurant_id,
        order_amount,

        DENSE_RANK() OVER (
            PARTITION BY city
            ORDER BY order_amount DESC
        ) AS amount_rank

    FROM food_delivery_orders
    WHERE order_status = 'COMPLETED'
)

SELECT *
FROM ranked_orders
WHERE amount_rank <= 3
ORDER BY city, amount_rank;
```

```text
Completed orders
      │
      ▼
Group records by city
      │
      ├── Chennai
      ├── Bengaluru
      ├── Mumbai
      └── ...
      │
      ▼
Sort amounts within each city
      │
      ▼
Calculate DENSE_RANK
```

---

# 28. Complete real-world workflow

## Bronze table

```sql
CREATE OR REPLACE TABLE bronze_food_orders
USING DELTA
AS
SELECT *
FROM food_delivery_orders;
```

## Silver table

```sql
CREATE OR REPLACE TABLE silver_food_orders
USING DELTA
CLUSTER BY (city, order_date)
AS
SELECT
    order_id,
    customer_id,
    restaurant_id,
    TRIM(city) AS city,
    TRIM(cuisine) AS cuisine,
    UPPER(order_status) AS order_status,
    order_amount,
    delivery_fee,
    order_amount + delivery_fee AS customer_total,
    order_date,
    order_timestamp
FROM bronze_food_orders
WHERE order_id IS NOT NULL
  AND order_amount >= 0;
```

## Gold table

```sql
CREATE OR REPLACE TABLE gold_daily_city_sales
USING DELTA
CLUSTER BY (city, order_date)
AS
SELECT
    order_date,
    city,
    COUNT(*) AS total_orders,

    SUM(
        CASE
            WHEN order_status = 'COMPLETED' THEN 1
            ELSE 0
        END
    ) AS completed_orders,

    ROUND(
        SUM(
            CASE
                WHEN order_status = 'COMPLETED'
                THEN order_amount
                ELSE 0
            END
        ),
        2
    ) AS completed_revenue,

    ROUND(AVG(order_amount), 2) AS average_order_amount

FROM silver_food_orders
GROUP BY
    order_date,
    city;
```

Query:

```sql
SELECT *
FROM gold_daily_city_sales
WHERE city = 'Chennai'
ORDER BY order_date DESC
LIMIT 30;
```

---

# 29. Recommended strategy

## Sorting

Use `ORDER BY` for final output.

Use `SORT BY` when only partition-level sorting is required.

## Execution partitioning

Use `DISTRIBUTE BY` and query-level `CLUSTER BY` selectively.

## Table partitioning

Use only after proving that pruning benefits a sufficiently large table.

## Bucketing

Learn it for Spark concepts and legacy systems. Do not use it as the default for new Delta workloads.

## Liquid clustering

Use table-level:

```sql
CLUSTER BY (city, order_date)
```

for frequently filtered Delta tables.

## Caching

Use explicit caching only when:

- An expensive result is reused several times
- It fits available resources
- The serverless environment supports it
- It is removed after use

---

# 30. Common mistakes

## Mistake 1: Believing ORDER BY changes storage

```sql
SELECT *
FROM food_delivery_orders
ORDER BY order_amount;
```

It sorts only the output.

## Mistake 2: Confusing query clustering with table clustering

```sql
SELECT * FROM food_delivery_orders CLUSTER BY city;
```

affects query execution.

```sql
CREATE TABLE food_delivery_orders
USING DELTA
CLUSTER BY (city);
```

defines table layout.

## Mistake 3: Partitioning by unique IDs

```sql
PARTITIONED BY (order_id)
```

This may produce many tiny files.

## Mistake 4: Treating cache as permanent

Cache can disappear after:

- Session termination
- Compute recycling
- Memory pressure
- `UNCACHE TABLE`
- `CLEAR CACHE`

## Mistake 5: Caching a table used once

Caching adds work and only helps when the result is reused.

---

# 31. Cleanup

```sql
CLEAR CACHE;

DROP TABLE IF EXISTS gold_daily_city_sales;
DROP TABLE IF EXISTS silver_food_orders;
DROP TABLE IF EXISTS bronze_food_orders;
DROP TABLE IF EXISTS completed_orders;
DROP TABLE IF EXISTS food_orders_clustered;
DROP TABLE IF EXISTS food_orders_partitioned;
DROP TABLE IF EXISTS food_delivery_orders;

DROP VIEW IF EXISTS completed_orders_view;
DROP VIEW IF EXISTS completed_orders_cache;
```

Optional schema cleanup:

```sql
USE SCHEMA default;

DROP SCHEMA IF EXISTS workspace.spark_sql_performance CASCADE;
```

---

# Final comparison

| Concept | Purpose | Permanent? | Recommended? |
|---|---|---:|---|
| `ORDER BY` | Globally sort output | No | Yes, when required |
| `SORT BY` | Sort within partitions | No | Yes, selectively |
| `DISTRIBUTE BY` | Redistribute rows | No | Selectively |
| Query `CLUSTER BY` | Redistribute and locally sort | No | Selectively |
| Table partitioning | Value-based physical layout | Yes | Only when proven |
| Bucketing | Hash-based fixed groups | Yes | Mostly legacy use |
| Liquid clustering | Flexible Delta layout | Yes | Yes |
| Spark cache | Reuse computed data | No | Selectively |
| Query result cache | Reuse SQL result | Temporary | Automatic |
| Disk cache | Reuse remote files locally | Temporary | Automatic |

For a modern Databricks Free Edition implementation, prefer:

```text
Delta tables
    +
Liquid clustering
    +
Selective final-result sorting
    +
Platform-managed caching
    +
EXPLAIN FORMATTED for verification
```
