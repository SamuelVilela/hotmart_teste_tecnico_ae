-- Ordem de execução diária (D-1)
-- Staging
RUN sql/staging/01_purchase_state.sql;
RUN sql/staging/02_product_item_state.sql;
RUN sql/staging/03_purchase_extra_info_state.sql;
-- Snapshot
RUN sql/snapshot/01_snapshot_d1.sql;
-- Merge
RUN sql/merge/01_merge_fact_purchase_daily_history.sql;
-- Manutenção
RUN sql/maintenance/optimize.sql;