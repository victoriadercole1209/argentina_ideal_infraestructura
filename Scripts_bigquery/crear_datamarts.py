from google.cloud import bigquery
from helpers.f_creadora import create_bigquery_dataset

PROJECT_ID  = "usm-infra-grupo5"
DATASET_DM  = "datamart_suministros"
DATASET_DW  = "datawarehouse_argideal"

client = bigquery.Client(project=PROJECT_ID)

DM_DEFS = {
    # Indicadores de asignación de stock y rotación por distribuidor
    "asignacion_stock_distribuidores": {
        "sql": f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_DM}.asignacion_stock_distribuidores`
        PARTITION BY fecha
        CLUSTER BY id_producto, id_sucursal
        AS
        SELECT
            f.fecha,
            f.id_producto,
            dp.nombre_producto,
            f.provincia,
            f.ciudad,
            f.id_sucursal,
            f.id_cliente,
            dc.n_distribuidor,
            dc.nombre_cliente,
            SUM(f.total_unidades_vendidas) AS total_unidades_vendidas,
            SUM(f.total_importe)           AS total_importe,
            AVG(f.stock_promedio)          AS stock_promedio,
            SAFE_DIVIDE(SUM(f.total_unidades_vendidas),
                        NULLIF(AVG(f.stock_promedio),0)) AS rotacion_producto,
            AVG(f.stock_promedio) IS NULL OR AVG(f.stock_promedio) = 0 AS rotacion_no_calculable
        FROM `{PROJECT_ID}.{DATASET_DW}.fact_ventas_stock_diaria` f
        JOIN `{PROJECT_ID}.{DATASET_DW}.dim_producto` dp
        ON f.id_producto = dp.id_producto
        JOIN `{PROJECT_ID}.{DATASET_DW}.dim_cliente` dc
        ON f.id_sucursal = dc.id_sucursal
        AND f.id_cliente = dc.id_cliente
        WHERE f.fecha BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                        AND CURRENT_DATE()
        GROUP BY f.fecha, f.id_producto, dp.nombre_producto,
                f.provincia, f.ciudad, f.id_sucursal, f.id_cliente,
                dc.n_distribuidor, dc.nombre_cliente
        """,
        "destination": f"{PROJECT_ID}.{DATASET_DM}.asignacion_stock_distribuidores",
    },
    # Indicadores de cobertura y eficiencia logística por producto y región
    "cobertura_stock_regional": {
        "sql": f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_DM}.cobertura_stock_regional`
        AS
        SELECT
          fecha                  AS fecha,
          id_producto,
          provincia,
          ciudad,
          SUM(total_unidades_vendidas) AS unidades,
          SUM(total_importe)           AS importe,
          AVG(stock_promedio)          AS stock_prom,
          SAFE_DIVIDE(SUM(total_unidades_vendidas),
                      NULLIF(AVG(stock_promedio),0))          AS rotacion,
          SAFE_DIVIDE(AVG(stock_promedio),
                      NULLIF(SUM(total_unidades_vendidas)/30,0)) AS cobertura_dias
        FROM `{PROJECT_ID}.{DATASET_DW}.fact_ventas_stock_diaria`
        WHERE fecha BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                      AND CURRENT_DATE()
        GROUP BY fecha, id_producto, provincia, ciudad
        """,
        "destination": f"{PROJECT_ID}.{DATASET_DM}.cobertura_stock_regional",
    },
}

if len(DM_DEFS) != len(set(DM_DEFS.keys())):
    raise ValueError("DM_DEFS contiene claves duplicadas")

def build_dm(dm_name: str):
    cfg = DM_DEFS[dm_name]
    job = client.query(
        cfg["sql"],
        job_config=bigquery.QueryJobConfig(priority="BATCH")
    )
    job.result()
    rows = client.get_table(cfg["destination"]).num_rows
    print(f"{dm_name}: {rows:,} filas")
    if rows == 0:
        raise RuntimeError(f"{dm_name} quedó vacío. Revisá filtros o joins.")

if __name__ == "__main__":
    print("Verificando existencia del dataset DM…")
    create_bigquery_dataset(DATASET_DM)
    for dm in DM_DEFS:
        build_dm(dm)
