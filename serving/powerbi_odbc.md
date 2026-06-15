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
- **ODBC: ERROR [HY000] [Cloudera][Hardy] (35) ... MetaException(message:java.lang.NullPointerException)**: This is a known bug with the Simba ODBC driver when browsing partitioned tables or `TIMESTAMP`/`DATE` columns in the Power BI Navigator. To fix this:
  1. Instead of selecting the table from the Navigator, click **Advanced options** when setting up the ODBC connection.
  2. Enter a native SQL query: `SELECT * FROM egx_db.curated_ohlcv` (or whichever table/view you need).
  3. Alternatively, make sure you have run `MSCK REPAIR TABLE egx_db.curated_ohlcv;` to ensure no partition metadata is missing.
- **Slow queries**: Add `LIMIT` clauses to views used in large visuals.
