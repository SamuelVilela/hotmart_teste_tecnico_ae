# Arquitetura e Tech Stack
A solução foi desenhada pensando no ambiente da Hotmart, utilizei o Spark, Delta Lake e arquitetura medallion. Minha ideia foi utilizar os dados do exercicio para facilitar compreenção e validação do desenvolvimento.

# Core Stack
PySpark: Processamento distribuído para transformações complexas e agregações de alto volume (Batch e Incremental D-1).

Delta Lake: Camada de armazenamento que garante transações ACID, versionamento (Time Travel) e evolução de schema, trazendo confiabilidade ao Data Lake.

Formato Parquet: Otimização de I/O via compressão colunar e column pruning.

Organização de Dados (Medallion)
Dividi a estrutura em três camadas para garantir a linhagem e qualidade

Otimizações Técnicas
Particionamento: Dados estruturados por transaction_date para reduzir o data scan e acelerar consultas.

Idempotência: Pipeline seguro para reprocessamento sem duplicidade de dados.

Escalabilidade: Arquitetura modular preparada para rodar no EMR ou em outros ambientes, facilmente orquestrada via Airflow.


# Pipeline GMV (Delta + SCD2) - 

Arquitetura implementada:

- Bronze (eventos simulados)
- Silver (reconstrução de estado)
- Snapshot particionado por data
- SCD2 idempotente
- Gold (GMV mensal)
- Relatório analítico

## Executar ETL

D-1 (default)
python etl/main.py

Data específica
python etl/main.py 2023-02-26

Backfill
python etl/main.py 2023-01-20 2023-02-28

## Executar Relatório

python reports/report.py

Respostas para Exercicio 2 - 

# Create Table da Tabela Final essa tabela pode ser implementada de forma fácil ao redshit

CREATE TABLE fact_purchase (
    transaction_date DATE,
    purchase_id BIGINT,
    subsidiary STRING,
    item_quantity INT,
    unit_price DECIMAL(18,2),
    item_gmv DECIMAL(18,2)
)
USING DELTA
PARTITIONED BY (transaction_date);

# SELECT baseado na fact_purchase
SELECT
    transaction_date,
    subsidiary,
    SUM(item_gmv) AS daily_gmv
FROM fact_purchase
GROUP BY transaction_date, subsidiary
ORDER BY transaction_date, subsidiary;


# Retorno da implementação do pipeline

===== HISTÓRICO SCD2 =====
+----------------+-----------+--------+-----------+----------+-------------+-------------+----------+---------+----------------------------------------------------------------+----------+----------+----------+--------------------------+
|transaction_date|purchase_id|buyer_id|producer_id|product_id|subsidiary   |item_quantity|unit_price|item_gmv |hash_diff
     |valid_from|valid_to  |is_current|created_at                |
+----------------+-----------+--------+-----------+----------+-------------+-------------+----------+---------+----------------------------------------------------------------+----------+----------+----------+--------------------------+
|2023-03-12      |55         |160001  |852852     |696969    |internacional|10           |60.00     |600.00   |fbc08e976a9accf36d2b3c0b08e32263473e87eac602a4ce72d4a3ac8bef476b|2023-03-12|2023-01-20|false     |2026-02-13 11:00:55.21422 |
|2023-01-26      |55         |15947   |852852     |696969    |nacional     |10           |60.00     |600.00   |2b9b6745224440d85c517df3532970d5223a58435b3ba621d1b18d7c1e748812|2023-01-26|2023-07-12|false     |2026-02-13 10:54:46.684079|
|2026-02-12      |55         |160001  |852852     |696969    |internacional|10           |55.00     |550.00   |828a0b2127c5ac8df2810d8502c570a0a7dc38b6e310ac69aba2771f74a612df|2026-02-12|2023-01-23|false     |2026-02-13 10:48:15.167101|
|2026-02-12      |56         |36798   |963963     |808080    |internacional|120          |2400.00   |288000.00|7eaf0a4396bf1006a05a8cbea7333253731121bc9ffc26aa77ac4a8f2280ffd3|2026-02-12|NULL      |true      |2026-02-13 10:48:15.167101|
|2026-02-12      |69         |160001  |96967      |373737    |nacional     |2            |2000.00   |4000.00  |b08af138e52829c5b39a979c3c809b64c8ca7395c9a55fff5a4c679a370041a7|2026-02-12|NULL      |true      |2026-02-13 10:48:15.167101|
+----------------+-----------+--------+-----------+----------+-------------+-------------+----------+---------+----------------------------------------------------------------+----------+----------+----------+--------------------------+


===== GOLD GMV MENSAL =====
+---------------+-------------+-----------+----------+--------------------------+
|reference_month|subsidiary   |monthly_gmv|as_of_date|created_at                |
+---------------+-------------+-----------+----------+--------------------------+
|2023-07-01     |nacional     |4000.00    |2023-07-12|2026-02-13 10:57:11.750638|
|2023-07-01     |internacional|288550.00  |2023-07-12|2026-02-13 10:57:11.750638|
|2023-02-01     |nacional     |5200.00    |2023-07-12|2026-02-13 10:57:11.750638|
|2023-02-01     |internacional|576000.00  |2023-07-12|2026-02-13 10:57:11.750638|
|2026-02-01     |internacional|288550.00  |2023-07-12|2026-02-13 10:57:11.750638|
|2026-02-01     |nacional     |4000.00    |2023-07-12|2026-02-13 10:57:11.750638|
|2023-01-01     |internacional|288000.00  |2023-07-12|2026-02-13 10:57:11.750638|
|2023-01-01     |nacional     |1100.00    |2023-07-12|2026-02-13 10:57:11.750638|
|2023-02-01     |nacional     |5200.00    |2023-02-28|2026-02-13 10:56:09.945174|
|2023-02-01     |internacional|576000.00  |2023-02-28|2026-02-13 10:56:09.945174|
|2026-02-01     |internacional|288550.00  |2023-02-28|2026-02-13 10:56:09.945174|
|2026-02-01     |nacional     |4000.00    |2023-02-28|2026-02-13 10:56:09.945174|
|2023-01-01     |internacional|288000.00  |2023-02-28|2026-02-13 10:56:09.945174|
|2023-01-01     |nacional     |1100.00    |2023-02-28|2026-02-13 10:56:09.945174|
|2026-02-01     |internacional|288550.00  |2023-02-05|2026-02-13 10:55:34.096307|
|2026-02-01     |nacional     |4000.00    |2023-02-05|2026-02-13 10:55:34.096307|
|2023-02-01     |nacional     |600.00     |2023-02-05|2026-02-13 10:55:34.096307|
|2023-02-01     |internacional|288000.00  |2023-02-05|2026-02-13 10:55:34.096307|
|2023-01-01     |internacional|288000.00  |2023-02-05|2026-02-13 10:55:34.096307|
|2023-01-01     |nacional     |1100.00    |2023-02-05|2026-02-13 10:55:34.096307|
+---------------+-------------+-----------+----------+--------------------------+




