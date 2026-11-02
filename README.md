# Olist ELT Warehouse (Airflow + dbt + Postgres)

Batch ELT pipeline: Olist e-commerce CSVs → `raw` → `staging` → `marts` (star schema), orchestrated by Airflow, transformed and tested by dbt, explored in Metabase.

```
CSV → Python/COPY → raw (TEXT) → dbt staging (typed views) → dbt marts (star schema) → Metabase
                          ↑ Airflow: one run per simulated day, with backfill ↑
```

## Quick start

1. Download the [Olist dataset from Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and unzip the CSVs into `data/raw/olist/`.
2. (Linux only) `echo "AIRFLOW_UID=$(id -u)" > .env`
3. `docker compose up --build -d`
4. Airflow UI: http://localhost:8080 (admin / admin). Trigger **olist_bootstrap** once, then unpause **olist_elt_daily** to replay history day by day.
5. Metabase: http://localhost:3000. Add a PostgreSQL database: host `warehouse`, port `5432`, db `olist`, user/password `warehouse`.

## Fast dev loop (no Airflow)

```bash
docker compose up -d warehouse
pip install "dbt-core==1.8.8" "dbt-postgres==1.8.2" psycopg2-binary pandas
python ingestion/load_olist.py all          # loads everything into raw
cd dbt && dbt deps && dbt build             # profiles.yml defaults to localhost:5433
```

## Design decisions

- **Raw is a faithful copy.** All columns are TEXT; typing and cleaning happen in staging, so bad source data never breaks ingestion.
- **Simulated daily arrival.** Olist is a static export, so the loader releases one purchase date at a time. Re-running a date deletes and reloads that day's orders (idempotent), so backfills and retries are safe.
- **Incremental fact.** `fct_order_items` is incremental on `_loaded_at` (`delete+insert` on `order_item_key`). A full refresh must produce the same result as the incremental history: `dbt run --full-refresh` is a good consistency check.
- **Grain.** Fact grain is one row per order item. `customer_id` is per order in Olist; `customer_unique_id` identifies the person.
- **dbt in its own virtualenv** inside the Airflow image to avoid dependency clashes.

## Roadmap

- [ ] `fct_payments`, `fct_reviews`
- [ ] Custom tests (payment total ≈ items + freight; delivered date ≥ purchase date)
- [ ] SCD2 snapshot on sellers or products
- [ ] GitHub Actions: `dbt build` on pull requests
- [ ] Metabase dashboard + screenshots, `dbt docs` lineage screenshot
- [ ] Swap the adapter to BigQuery/Snowflake
