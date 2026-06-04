create or replace procedure operational_log.boost_performance()
language plpgsql
as $$
BEGIN
    -- Adjust PostgreSQL settings for better performance during ingestion
        ANALYZE silver.order_items_raw_p;
        ANALYZE silver.orders_raw_p;
        ANALYZE silver.payments_raw_p;
        ANALYZE gold.fact_sales;
        ANALYZE gold.fact_payments;
END;
$$;

call operational_log.boost_performance();