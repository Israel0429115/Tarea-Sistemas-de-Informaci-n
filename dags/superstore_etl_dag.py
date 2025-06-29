# =============================================================================
# IMPORTS
# =============================================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
# Debajo de los imports de airflow
import pandas as pd
from google.cloud import storage, bigquery
from io import StringIO
# Debajo de las importaciones
# --- CONFIGURACIÓN DE GCP ---
GCP_PROJECT_ID = 'tarea-sistemas-proceso-etl'  # El ID de tu proyecto de GCP
GCS_BUCKET_NAME = 'at-bucket-practica-sistemas' # El nombre de tu bucket
BQ_DATASET_ID = 'dwh_superstore'                # El ID de tu dataset en BigQuery
GCS_FILE_NAME = 'Global_Superstore2.csv'        # El nombre de tu archivo CSV
# =============================================================================
# DEFINICIÓN DE FUNCIONES (PLACEHOLDERS)
# =============================================================================
# Estas funciones se implementarán en la siguiente fase. Por ahora, solo
# las definimos para que el DAG no dé error al intentar encontrarlas.

def etl_dim_producto():
    """
    Proceso ETL para la dimensión de productos.
    E: Extrae datos de GCS.
    T: Transforma los datos para crear la dimensión.
    L: Carga los datos a BigQuery.
    """
    print(f"Iniciando ETL para la dimensión de productos desde {GCS_FILE_NAME}...")

    # E (Extraer): Leer el archivo CSV desde Google Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_FILE_NAME)
    
    try:
        content = blob.download_as_string().decode('latin-1')
        df_source = pd.read_csv(StringIO(content))
        print(f"Se extrajeron {len(df_source)} filas del archivo de origen.")
    except Exception as e:
        print(f"Error al leer el archivo de GCS: {e}")
        raise

    # T (Transformar): Crear el DataFrame para la dimensión de productos
    print("Transformando datos para d_producto...")
    df_dim_producto = df_source[['Product ID', 'Product Name', 'Category', 'Sub-Category']].copy()

    # Renombrar columnas para que coincidan con el esquema de BigQuery
    df_dim_producto.rename(columns={
        'Product ID': 'producto_ID',
        'Product Name': 'nombre',
        'Category': 'categoria',
        'Sub-Category': 'subcategoria'
    }, inplace=True)
    
    # Eliminar duplicados para tener una fila única por producto
    df_dim_producto = df_dim_producto.drop_duplicates(subset=['producto_ID']).reset_index(drop=True)
    
    # Generar la clave subrogada (Surrogate Key)
    df_dim_producto.insert(0, 'id_producto', range(1, 1 + len(df_dim_producto)))
    print(f"Se crearon {len(df_dim_producto)} registros únicos para d_producto.")

    # L (Cargar): Cargar el DataFrame a BigQuery
    print("Cargando datos a BigQuery en la tabla d_producto...")
    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID)
    
    # Configuración del trabajo de carga
    job_config = bigquery.LoadJobConfig(
        # WRITE_TRUNCATE: Si la tabla ya tiene datos, los borra antes de cargar los nuevos.
        # Esto hace que nuestro ETL sea idempotente.
        write_disposition="WRITE_TRUNCATE", 
    )

    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.d_producto"

    try:
        job = bigquery_client.load_table_from_dataframe(
            df_dim_producto, table_id, job_config=job_config
        )
        job.result()  # Espera a que el trabajo de carga termine
        print(f"Carga completada. Se cargaron {job.output_rows} filas en la tabla {table_id}.")
    except Exception as e:
        print(f"Error al cargar datos a BigQuery: {e}")
        raise

def etl_dim_cliente():
    """
    Proceso ETL para la dimensión de clientes.
    """
    print(f"Iniciando ETL para la dimensión de clientes desde {GCS_FILE_NAME}...")

    # E (Extraer): Reutilizamos la misma lógica de lectura
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_FILE_NAME)
    
    try:
        content = blob.download_as_string().decode('latin-1')
        df_source = pd.read_csv(StringIO(content))
        print(f"Se extrajeron {len(df_source)} filas del archivo de origen.")
    except Exception as e:
        print(f"Error al leer el archivo de GCS: {e}")
        raise

    # T (Transformar): Crear el DataFrame para la dimensión de clientes
    print("Transformando datos para d_cliente...")
    # Seleccionamos las columnas relevantes para el cliente
    df_dim_cliente = df_source[['Customer ID', 'Customer Name', 'Segment']].copy()

    # Renombrar columnas
    df_dim_cliente.rename(columns={
        'Customer ID': 'cliente_ID',
        'Customer Name': 'nombre',
        'Segment': 'segmento'
    }, inplace=True)
    
    # Eliminar duplicados
    df_dim_cliente = df_dim_cliente.drop_duplicates(subset=['cliente_ID']).reset_index(drop=True)
    
    # Generar clave subrogada
    df_dim_cliente.insert(0, 'id_cliente', range(1, 1 + len(df_dim_cliente)))
    print(f"Se crearon {len(df_dim_cliente)} registros únicos para d_cliente.")

    # L (Cargar): Cargar a BigQuery
    print("Cargando datos a BigQuery en la tabla d_cliente...")
    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.d_cliente"

    try:
        job = bigquery_client.load_table_from_dataframe(
            df_dim_cliente, table_id, job_config=job_config
        )
        job.result()
        print(f"Carga completada. Se cargaron {job.output_rows} filas en la tabla {table_id}.")
    except Exception as e:
        print(f"Error al cargar datos a BigQuery: {e}")
        raise

def etl_dim_ubic_geo():
    """
    Proceso ETL para la dimensión de ubicación geográfica.
    """
    print(f"Iniciando ETL para la dimensión de ubicación desde {GCS_FILE_NAME}...")

    # E (Extraer)
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_FILE_NAME)
    
    try:
        content = blob.download_as_string().decode('latin-1')
        df_source = pd.read_csv(StringIO(content))
        print(f"Se extrajeron {len(df_source)} filas del archivo de origen.")
    except Exception as e:
        print(f"Error al leer el archivo de GCS: {e}")
        raise

    # T (Transformar)
    print("Transformando datos para d_ubic_geo...")
    # Seleccionamos las columnas geográficas
    geo_cols = ['Region', 'Country', 'State', 'City', 'Market', 'Postal Code']
    df_dim_geo = df_source[geo_cols].copy()

    # Renombrar columnas
    df_dim_geo.rename(columns={
        'Region': 'region',
        'Country': 'pais',
        'State': 'estado',
        'City': 'ciudad',
        'Market': 'mercado',
        'Postal Code': 'codigo_postal'
    }, inplace=True)

    # Convertir el código postal a String para evitar problemas de tipo de dato
    # y rellenar nulos con un valor placeholder
    df_dim_geo['codigo_postal'] = df_dim_geo['codigo_postal'].astype(str).fillna('N/A')
    
    # Eliminar duplicados basados en la combinación única de todas las columnas geográficas
    df_dim_geo = df_dim_geo.drop_duplicates().reset_index(drop=True)
    
    # Generar clave subrogada
    df_dim_geo.insert(0, 'id_ubicGeografica', range(1, 1 + len(df_dim_geo)))
    print(f"Se crearon {len(df_dim_geo)} registros únicos para d_ubic_geo.")

    # L (Cargar)
    print("Cargando datos a BigQuery en la tabla d_ubic_geo...")
    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.d_ubic_geo"

    try:
        job = bigquery_client.load_table_from_dataframe(
            df_dim_geo, table_id, job_config=job_config
        )
        job.result()
        print(f"Carga completada. Se cargaron {job.output_rows} filas en la tabla {table_id}.")
    except Exception as e:
        print(f"Error al cargar datos a BigQuery: {e}")
        raise

def etl_dim_detalle_orden():
    """
    Proceso ETL para la dimensión de detalles de la orden.
    """
    print(f"Iniciando ETL para la dimensión de detalles de la orden desde {GCS_FILE_NAME}...")

    # E (Extraer)
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_FILE_NAME)
    
    try:
        content = blob.download_as_string().decode('latin-1')
        df_source = pd.read_csv(StringIO(content))
        print(f"Se extrajeron {len(df_source)} filas del archivo de origen.")
    except Exception as e:
        print(f"Error al leer el archivo de GCS: {e}")
        raise

    # T (Transformar)
    print("Transformando datos para d_detalle_orden...")
    # Seleccionamos las columnas de detalle
    detalle_cols = ['Ship Mode', 'Order Priority']
    df_dim_detalle = df_source[detalle_cols].copy()

    # Renombrar columnas
    df_dim_detalle.rename(columns={
        'Ship Mode': 'modo_envio',
        'Order Priority': 'prioridad'
    }, inplace=True)
    
    # Eliminar duplicados
    df_dim_detalle = df_dim_detalle.drop_duplicates().reset_index(drop=True)
    
    # Generar clave subrogada
    df_dim_detalle.insert(0, 'id_detalle_orden', range(1, 1 + len(df_dim_detalle)))
    print(f"Se crearon {len(df_dim_detalle)} registros únicos para d_detalle_orden.")

    # L (Cargar)
    print("Cargando datos a BigQuery en la tabla d_detalle_orden...")
    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.d_detalle_orden"

    try:
        job = bigquery_client.load_table_from_dataframe(
            df_dim_detalle, table_id, job_config=job_config
        )
        job.result()
        print(f"Carga completada. Se cargaron {job.output_rows} filas en la tabla {table_id}.")
    except Exception as e:
        print(f"Error al cargar datos a BigQuery: {e}")
        raise

def etl_dim_tiempo():
    """
    Proceso ETL para la dimensión de tiempo, consolidando todas las fechas.
    """
    print(f"Iniciando ETL para la dimensión de tiempo desde {GCS_FILE_NAME}...")

    # E (Extraer)
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_FILE_NAME)
    
    try:
        content = blob.download_as_string().decode('latin-1')
        df_source = pd.read_csv(StringIO(content))
        print(f"Se extrajeron {len(df_source)} filas del archivo de origen.")
    except Exception as e:
        print(f"Error al leer el archivo de GCS: {e}")
        raise

    # T (Transformar)
    print("Transformando datos para d_tiempo...")
    
    # Extraer fechas de orden y envío, y convertirlas a formato de fecha
    fechas_orden = pd.to_datetime(df_source['Order Date'], dayfirst=False)
    fechas_envio = pd.to_datetime(df_source['Ship Date'], dayfirst=False)
    
    # Unir todas las fechas en una sola serie y obtener valores únicos
    fechas_unicas = pd.concat([fechas_orden, fechas_envio]).unique()
    
    # Crear el DataFrame de la dimensión a partir de las fechas únicas
    df_dim_tiempo = pd.DataFrame({'fecha_completa': fechas_unicas})
    df_dim_tiempo.dropna(inplace=True) # Eliminar posibles nulos
    df_dim_tiempo['fecha_completa'] = pd.to_datetime(df_dim_tiempo['fecha_completa']).dt.date

    # Ordenar por fecha para que los IDs sean secuenciales en el tiempo
    df_dim_tiempo = df_dim_tiempo.sort_values(by='fecha_completa').reset_index(drop=True)
    
    # Generar clave subrogada
    df_dim_tiempo.insert(0, 'id_tiempo', range(1, 1 + len(df_dim_tiempo)))
    
    # Derivar columnas adicionales (año, mes, día, etc.)
    df_dim_tiempo['anio'] = pd.to_datetime(df_dim_tiempo['fecha_completa']).dt.year
    df_dim_tiempo['mes'] = pd.to_datetime(df_dim_tiempo['fecha_completa']).dt.month
    df_dim_tiempo['dia'] = pd.to_datetime(df_dim_tiempo['fecha_completa']).dt.day
    df_dim_tiempo['mes_nombre'] = pd.to_datetime(df_dim_tiempo['fecha_completa']).dt.strftime('%B')
    print(f"Se crearon {len(df_dim_tiempo)} registros únicos para d_tiempo.")

    # L (Cargar)
    print("Cargando datos a BigQuery en la tabla d_tiempo...")
    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.d_tiempo"

    try:
        job = bigquery_client.load_table_from_dataframe(
            df_dim_tiempo, table_id, job_config=job_config
        )
        job.result()
        print(f"Carga completada. Se cargaron {job.output_rows} filas en la tabla {table_id}.")
    except Exception as e:
        print(f"Error al cargar datos a BigQuery: {e}")
        raise

def etl_fact_ventas():
    """
    Proceso ETL para la tabla de hechos de ventas.
    Esta tarea depende de que todas las dimensiones ya estén cargadas en BigQuery.
    """
    print("Iniciando ETL para la tabla de hechos h_ventas...")

    # --- E (Extraer) ---
    # 1. Extraer el archivo de ventas original de GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_FILE_NAME)
    try:
        content = blob.download_as_string().decode('latin-1')
        df_ventas = pd.read_csv(StringIO(content))
        print(f"Se extrajeron {len(df_ventas)} filas de ventas del archivo de origen.")
    except Exception as e:
        print(f"Error al leer el archivo de GCS: {e}")
        raise
    
    # 2. Extraer las tablas de dimensión desde BigQuery para el lookup
    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID)
    print("Extrayendo dimensiones desde BigQuery para lookup...")
    
    try:
        sql_producto = f"SELECT id_producto, producto_ID FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.d_producto`"
        df_dim_producto = bigquery_client.query(sql_producto).to_dataframe()

        sql_cliente = f"SELECT id_cliente, cliente_ID FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.d_cliente`"
        df_dim_cliente = bigquery_client.query(sql_cliente).to_dataframe()

        sql_ubic_geo = f"SELECT id_ubicGeografica, region, pais, estado, ciudad, mercado, codigo_postal FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.d_ubic_geo`"
        df_dim_ubic_geo = bigquery_client.query(sql_ubic_geo).to_dataframe()

        sql_detalle = f"SELECT id_detalle_orden, modo_envio, prioridad FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.d_detalle_orden`"
        df_dim_detalle = bigquery_client.query(sql_detalle).to_dataframe()

        sql_tiempo = f"SELECT id_tiempo, fecha_completa FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.d_tiempo`"
        df_dim_tiempo = bigquery_client.query(sql_tiempo).to_dataframe()
        print("Dimensiones extraídas correctamente desde BigQuery.")
    except Exception as e:
        print(f"Error al extraer dimensiones desde BigQuery: {e}")
        raise

    # --- T (Transformar) ---
    print("Iniciando transformación de la tabla de hechos...")

    # 1. Limpieza y preparación del DataFrame de ventas
    # Convertir las columnas de fecha del DataFrame de ventas a tipo datetime para el merge
    df_ventas['Order Date'] = pd.to_datetime(df_ventas['Order Date'], dayfirst=False).dt.date
    df_ventas['Ship Date'] = pd.to_datetime(df_ventas['Ship Date'], dayfirst=False).dt.date

    # 2. Realizar los merges (lookups) para obtener las claves subrogadas
    print("Realizando merges para enriquecer los hechos con las claves de dimensión...")
    
    # Merge con d_producto
    df_fact = pd.merge(df_ventas, df_dim_producto, left_on='Product ID', right_on='producto_ID', how='left')
    
    # Merge con d_cliente
    df_fact = pd.merge(df_fact, df_dim_cliente, left_on='Customer ID', right_on='cliente_ID', how='left')

    # Merge con d_ubic_geo
    # Para el merge de geo, necesitamos unir por todas las columnas que definen la unicidad
    geo_merge_cols = ['region', 'pais', 'estado', 'ciudad', 'mercado', 'codigo_postal']
    # Renombramos las columnas del df_fact para que coincidan con las de la dimensión
    df_fact.rename(columns={'Region': 'region', 'Country': 'pais', 'State': 'estado', 'City': 'ciudad', 'Market': 'mercado', 'Postal Code': 'codigo_postal'}, inplace=True)
    df_fact['codigo_postal'] = df_fact['codigo_postal'].astype(str).fillna('N/A')
    df_fact = pd.merge(df_fact, df_dim_ubic_geo, on=geo_merge_cols, how='left')

    # Merge con d_detalle_orden
    detalle_merge_cols = ['modo_envio', 'prioridad']
    df_fact.rename(columns={'Ship Mode': 'modo_envio', 'Order Priority': 'prioridad'}, inplace=True)
    df_fact = pd.merge(df_fact, df_dim_detalle, on=detalle_merge_cols, how='left')
    
    # Merge con d_tiempo (dos veces, una para cada rol de fecha)
    # Primero para la fecha de la orden
    df_fact = pd.merge(df_fact, df_dim_tiempo, left_on='Order Date', right_on='fecha_completa', how='left')
    df_fact.rename(columns={'id_tiempo': 'id_fecha_orden'}, inplace=True) # Renombrar la FK
    df_fact.drop(columns=['fecha_completa'], inplace=True) # Limpiar columna de merge

    # Segundo para la fecha de envío
    df_fact = pd.merge(df_fact, df_dim_tiempo, left_on='Ship Date', right_on='fecha_completa', how='left')
    df_fact.rename(columns={'id_tiempo': 'id_fecha_envio'}, inplace=True) # Renombrar la FK
    df_fact.drop(columns=['fecha_completa'], inplace=True) # Limpiar columna de merge

    print("Merges completados.")

    # 3. Seleccionar y renombrar las columnas finales para la tabla de hechos
    fact_cols = [
        'id_fecha_orden',
        'id_fecha_envio',
        'id_producto',
        'id_cliente',
        'id_detalle_orden',
        'id_ubicGeografica',
        'Order ID',
        'Sales',
        'Quantity',
        'Discount',
        'Profit',
        'Shipping Cost'
    ]
    df_final_fact = df_fact[fact_cols].copy()
    
    df_final_fact.rename(columns={
        'Order ID': 'orden_ID',
        'Sales': 'monto_ventas',
        'Quantity': 'cantidad',
        'Discount': 'desceunto',
        'Profit': 'ganancia',
        'Shipping Cost': 'costo_envio'
    }, inplace=True)

    print(f"Transformación finalizada. Se prepararon {len(df_final_fact)} filas para la tabla de hechos.")

    # --- L (Cargar) ---
    print("Cargando datos a BigQuery en la tabla h_ventas...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.h_ventas"
    
    try:
        job = bigquery_client.load_table_from_dataframe(
            df_final_fact, table_id, job_config=job_config
        )
        job.result()
        print(f"Carga completada. Se cargaron {job.output_rows} filas en la tabla {table_id}.")
    except Exception as e:
        print(f"Error al cargar datos a BigQuery: {e}")
        raise

# =============================================================================
# ARGUMENTOS POR DEFECTO DEL DAG
# =============================================================================
default_args = {
    'owner': 'tu_nombre', # Cambia esto por tu nombre o iniciales
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}
# =============================================================================
# DEFINICIÓN DEL DAG
# =============================================================================
with DAG(
    dag_id='superstore_etl_workflow',
    default_args=default_args,
    description='Proceso ETL completo para el Data Warehouse de Superstore',
    schedule_interval=None,  # Lo ejecutaremos manualmente
    catchup=False,
    tags=['superstore', 'gcp', 'bigquery', 'etl'],
) as dag:
    # =============================================================================
    # DEFINICIÓN DE TAREAS (OPERADORES)
    # =============================================================================

    # Tarea de inicio (placeholder)
    start_op = DummyOperator(task_id='inicio_del_proceso')
    # Tareas de carga de Dimensiones (se ejecutarán en paralelo)
    load_dim_producto = PythonOperator(
        task_id='cargar_dim_producto',
        python_callable=etl_dim_producto
    )
    load_dim_cliente = PythonOperator(
        task_id='cargar_dim_cliente',
        python_callable=etl_dim_cliente
    )
    load_dim_ubic_geo = PythonOperator(
        task_id='cargar_dim_ubic_geo',
        python_callable=etl_dim_ubic_geo
    )
    load_dim_detalle_orden = PythonOperator(
        task_id='cargar_dim_detalle_orden',
        python_callable=etl_dim_detalle_orden
    )

    load_dim_tiempo = PythonOperator(
        task_id='cargar_dim_tiempo',
        python_callable=etl_dim_tiempo
    )
    # Tarea de carga de la Tabla de Hechos (se ejecuta después de las dimensiones)
    load_fact_ventas = PythonOperator(
        task_id='cargar_fact_ventas',
        python_callable=etl_fact_ventas
    )
    # Tarea de fin (placeholder)
    end_op = DummyOperator(task_id='fin_del_proceso')

    # =============================================================================
    # DEFINICIÓN DEL FLUJO DE TRABAJO (DEPENDENCIAS)
    # =============================================================================
    dimensiones_tasks = [
        load_dim_producto,
        load_dim_cliente,
        load_dim_ubic_geo,
        load_dim_detalle_orden,
        load_dim_tiempo
    ]
    start_op >> dimensiones_tasks >> load_fact_ventas >> end_op