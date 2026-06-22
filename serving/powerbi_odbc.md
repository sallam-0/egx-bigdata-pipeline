# Connecting Power BI to Hive via ODBC

## Prerequisites
- Power BI Desktop installed on Windows
- Hive server running and accessible on port 10000
- Simba Hive ODBC Driver (64-bit): https://www.cloudera.com/downloads/connectors/hive/odbc/2-6-26.html

## Step 1 — Install the Simba Hive ODBC driver
Download and run the installer. Restart your machine after installation.

## Step 2 — Create a DSN
1. Open **ODBC Data Sources (64-bit)** from the Start menu.
2. Click **Add** → select **Simba Hive ODBC Driver**.
3. Fill in:
   - **DSN Name**: `EGX_Hive`
   - **Host(s)**: `localhost` (or your Hive server IP)
   - **Port**: `10000`
   - **Database**: `egx_db`
   - **Authentication**: No Authentication (for local dev) or Username only.
4. Click **Test** → should say "Successfully connected".

## Step 3 — Connect Power BI
1. Open Power BI Desktop → **Get Data** → **Other** → **ODBC**.
2. Select DSN `EGX_Hive` → **OK**.
3. Choose **DirectQuery** mode (avoids stale import cache).
4. Browse tables: you should see `curated_ohlcv`, `v_daily_summary`, `v_rsi_signals`, etc.
5. Load the views you need.

## Recommended tables to load
| View | Use case |
|---|---|
| `v_daily_summary` | Main price & indicator dashboard |
| `v_weekly_rollup` | Weekly performance bar charts |
| `v_top_movers` | Top 10 movers card visual |
| `v_rsi_signals` | RSI overbought/oversold table |
| `v_bollinger_squeeze` | Volatility alerts |

## Scheduled refresh
DirectQuery mode fetches live from Hive on every report interaction —
no manual refresh needed. For Power BI Service (cloud), you would need
a Data Gateway; for local/desktop use, DirectQuery is sufficient.

## Troubleshooting
- **"Cannot connect"**: Ensure HiveServer2 is running (`hive --service hiveserver2`).
- **"No tables found"**: Run `MSCK REPAIR TABLE` in Hive to register partitions.

### ❌ ODBC: ERROR [HY000] [Cloudera][Hardy] (35) MetaException(NullPointerException)

**Root cause**: This is a **known, unfixable bug** in the Simba/Hardy ODBC driver with Hive 2.x.
It has two triggers:

| Trigger | When it happens |
|---|---|
| Navigator table browse | Hardy calls ODBC catalog APIs (`SQLTables`/`SQLColumns`) which translate to Hive metadata Thrift calls that NPE on partitioned tables |
| `DATE`-typed columns | Hardy has a null-path when mapping Hive native `DATE` → ODBC SQL type during `GetColumns` introspection |

**The Power BI Navigator will NEVER work reliably with Hive 2.x + partitioned tables.**
This was fixed in Hive 3.x. Upgrading Hive is the only permanent fix.

### ✅ Correct workflow — always use SQL via Advanced Options

**Never use the Navigator to browse/load tables.** Instead:

1. **Get Data → ODBC → DSN: `EGX_Hive`**
2. Expand **Advanced options**
3. Enter a SQL statement for each view you need:

```sql
-- Main dashboard
SELECT * FROM egx_db.v_daily_summary

-- RSI signals
SELECT * FROM egx_db.v_rsi_signals

-- Weekly rollup
SELECT * FROM egx_db.v_weekly_rollup

-- Top movers
SELECT * FROM egx_db.v_top_movers

-- Bollinger squeeze
SELECT * FROM egx_db.v_bollinger_squeeze
```

4. In Power Query Editor, change `trade_date` column type to **Date**.

> All views cast the `date` column to `STRING AS trade_date` to avoid the Hardy DATE-type NPE.
> Power BI will auto-detect or you can manually set it to Date type in Power Query.

- **Slow queries**: Add `LIMIT` clauses to views used in large visuals.
- **Stale data after ETL**: Re-run `ANALYZE TABLE egx_db.curated_ohlcv PARTITION(symbol) COMPUTE STATISTICS;` after each ETL job to keep metastore stats accurate.

