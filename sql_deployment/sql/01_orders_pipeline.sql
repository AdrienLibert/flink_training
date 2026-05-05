-- 01_orders_pipeline.sql — example SQL pipeline.
--
-- Submit via SQL Gateway:
--   curl -X POST http://sql-gateway:8083/sessions -H 'Content-Type: application/json' -d '{}'
--   curl -X POST http://sql-gateway:8083/sessions/<id>/statements -d @01_orders_pipeline.sql
--
-- Or via the SQL Client REPL:
--   ./bin/sql-client.sh embedded -i 01_orders_pipeline.sql

CREATE CATALOG hive WITH (
    'type' = 'hive',
    'hive-conf-dir' = '/etc/hive/conf'
);

USE CATALOG hive;
USE prod_streaming;

CREATE TEMPORARY TABLE orders_kafka (
    order_id STRING,
    user_id STRING,
    category STRING,
    amount DOUBLE,
    event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'prod.orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-orders-pipeline',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'avro-confluent',
    'avro-confluent.schema-registry.url' = 'http://schema-registry:8081'
);

CREATE TEMPORARY TABLE category_totals_jdbc (
    category STRING,
    total_amount DOUBLE,
    last_updated TIMESTAMP_LTZ(3),
    PRIMARY KEY (category) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/analytics',
    'table-name' = 'category_totals',
    'username' = '${env:PG_USERNAME}',
    'password' = '${env:PG_PASSWORD}'
);

INSERT INTO category_totals_jdbc
SELECT category, SUM(amount), CURRENT_TIMESTAMP
FROM orders_kafka
GROUP BY category;
