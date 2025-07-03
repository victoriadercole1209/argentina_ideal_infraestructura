from google.cloud import bigquery
from helpers.f_creadora import create_bigquery_dataset

PROJECT_ID   = "usm-infra-grupo5"
DATASET_DW   = "datawarehouse_argideal"
DATASET_RAW  = "Raw"
client       = bigquery.Client(project=PROJECT_ID)


def create_dimensions():
    print("\n→ Creando dimensiones…")

    # Dim Producto
    client.query(f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_DW}.dim_producto` AS
    SELECT DISTINCT
           SKU_codigo       AS id_producto,
           SKU_descripcion  AS nombre_producto,
           unidad
    FROM `{PROJECT_ID}.{DATASET_RAW}.stock`
    WHERE SKU_codigo IS NOT NULL;
    """).result()

    # Dim Cliente
    client.query(f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_DW}.dim_cliente` AS
    SELECT DISTINCT
           codigo_cliente AS id_cliente,
           nombre_cliente,
           codigo_sucursal AS id_sucursal,
           n_distribuidor
    FROM `{PROJECT_ID}.{DATASET_RAW}.maestro`
    WHERE codigo_cliente IS NOT NULL;
    """).result()

    # Dim Sucursal
    client.query(f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_DW}.dim_sucursal` AS
    SELECT DISTINCT
           codigo_sucursal  AS id_sucursal,
           ciudad,
           provincia
    FROM `{PROJECT_ID}.{DATASET_RAW}.maestro`
    WHERE codigo_sucursal IS NOT NULL;
    """).result()

    # Dim Fecha
    client.query(f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_DW}.dim_fecha` AS
    SELECT
      fecha,
      FORMAT_DATE('%Y%m%d', fecha)     AS id_fecha,
      EXTRACT(YEAR  FROM fecha)        AS anio,
      EXTRACT(MONTH FROM fecha)        AS mes,
      EXTRACT(DAY   FROM fecha)        AS dia,
      EXTRACT(DAYOFWEEK FROM fecha)    AS dia_semana,
      FORMAT_DATE('%Y-%m', fecha)      AS anio_mes,
      IF(EXTRACT(DAYOFWEEK FROM fecha) IN (1,7), TRUE, FALSE) AS es_fin_de_semana
    FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01','2025-12-31')) AS fecha;
    """).result()

    print("Dimensiones listas.")

def create_fact_table():
    print("\n→ Creando tabla de hechos…")

    client.query(f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_DW}.fact_ventas_stock_diaria`
    PARTITION BY fecha
    CLUSTER BY id_producto, id_cliente, id_sucursal
    AS
    SELECT
      v.SKU_codigo          AS id_producto,
      v.codigo_cliente      AS id_cliente,
      v.codigo_sucursal     AS id_sucursal,
      DATE(v.fecha_cierre_comercial) AS fecha,
      m.provincia,
      m.ciudad,
      SUM(v.venta_unidades)  AS total_unidades_vendidas,
      SUM(v.venta_importe)   AS total_importe,
      AVG(COALESCE(s.stock_unidades,0)) AS stock_promedio
    FROM `{PROJECT_ID}.{DATASET_RAW}.venta_clientes` v
    JOIN `{PROJECT_ID}.{DATASET_RAW}.maestro` m
      ON v.codigo_sucursal = m.codigo_sucursal
     AND v.codigo_cliente  = m.codigo_cliente         -- join compuesto
    LEFT JOIN `{PROJECT_ID}.{DATASET_RAW}.stock` s
      ON v.codigo_sucursal = s.codigo_sucursal
     AND v.SKU_codigo      = s.SKU_codigo
     AND v.fecha_cierre_comercial = s.fecha_cierre_comercial
    GROUP BY id_producto, id_cliente, id_sucursal,
             provincia, ciudad, fecha;
    """).result()

    rows = client.get_table(f"{PROJECT_ID}.{DATASET_DW}.fact_ventas_stock_diaria").num_rows
    print(f"Tabla de hechos creada con {rows:,} filas")

if __name__ == "__main__":
    print("Verificando existencia del dataset DW…")
    create_bigquery_dataset(DATASET_DW)
    create_dimensions()
    create_fact_table()
