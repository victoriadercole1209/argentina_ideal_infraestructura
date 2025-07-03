from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, NotFound
from helpers.f_creadora import create_bigquery_dataset

# Inicializar cliente
PROJECT_ID = "usm-infra-grupo5"
DATASET_ID = "Raw"
BUCKET = "argentina_ideal_grupo5_2025"
client = bigquery.Client(project=PROJECT_ID)

# Esquemas
esquema_tabla_stock = [
    bigquery.SchemaField("codigo_sucursal", "INTEGER"),
    bigquery.SchemaField("fecha_cierre_comercial", "DATETIME"),
    bigquery.SchemaField("SKU_codigo", "STRING"),
    bigquery.SchemaField("SKU_descripcion", "STRING"),
    bigquery.SchemaField("stock_unidades", "INTEGER"),
    bigquery.SchemaField("unidad", "STRING"),
    bigquery.SchemaField("n_distribuidor", "INTEGER"),
]

esquema_tabla_venta = [
    bigquery.SchemaField("codigo_sucursal", "INTEGER"),
    bigquery.SchemaField("codigo_cliente", "INTEGER"),
    bigquery.SchemaField("fecha_cierre_comercial", "DATETIME"),
    bigquery.SchemaField("SKU_codigo", "STRING"),
    bigquery.SchemaField("venta_unidades", "INTEGER"),
    bigquery.SchemaField("venta_importe", "FLOAT"),
    bigquery.SchemaField("condicion_venta", "STRING"),
    bigquery.SchemaField("n_distribuidor", "INTEGER"),
]

esquema_tabla_cliente = [
    bigquery.SchemaField("codigo_sucursal", "INTEGER"),
    bigquery.SchemaField("codigo_cliente", "INTEGER"),
    bigquery.SchemaField("ciudad", "STRING"),
    bigquery.SchemaField("provincia", "STRING"),
    bigquery.SchemaField("estado", "STRING"),
    bigquery.SchemaField("nombre_cliente", "STRING"),
    bigquery.SchemaField("cuit", "INTEGER"),
    bigquery.SchemaField("razon_social", "STRING"),
    bigquery.SchemaField("direccion", "STRING"),
    bigquery.SchemaField("dias_visita", "STRING"),
    bigquery.SchemaField("telefono", "STRING"),
    bigquery.SchemaField("fecha_alta", "DATETIME"),
    bigquery.SchemaField("fecha_baja", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("lat", "FLOAT"),
    bigquery.SchemaField("long", "FLOAT"),
    bigquery.SchemaField("condicion_venta", "STRING"),
    bigquery.SchemaField("deuda_vencida", "FLOAT"),
    bigquery.SchemaField("tipo_negocio", "STRING"),
    bigquery.SchemaField("n_distribuidor", "INTEGER"),
]

# Configuración para cada tabla
tablas = {
    "stock": {
        "uri": f"gs://{BUCKET}/Archivos_Stock/*.csv",
        "schema": esquema_tabla_stock
    },
    "venta_clientes": {
        "uri": f"gs://{BUCKET}/Archivos_VentaClientes/*.csv",
        "schema": esquema_tabla_venta
    },
    "maestro": {
        "uri": f"gs://{BUCKET}/Archivos_Maestro/*.csv",
        "schema": esquema_tabla_cliente
    }
}

def cargar_datos_de_gcs_a_bigquery(uri, table_id, schema):
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    table = client.get_table(table_id)
    print(f" {table_id} cargada con {table.num_rows} filas desde {uri}")

# Ejecutar cargas
if __name__== "__main__":
    create_bigquery_dataset(DATASET_ID)
    for nombre_tabla, info in tablas.items():
        try:
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{nombre_tabla}"
            cargar_datos_de_gcs_a_bigquery(info["uri"], table_id, info["schema"])
        except (BadRequest, NotFound) as e:
            print(f"Error al cargar '{nombre_tabla}' desde {info['uri']}: {e}")