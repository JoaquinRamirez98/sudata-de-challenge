# sudata-de-challenge-kpojoa/exercise2_bcra_api/src/bcra_api_pipeline.py

import requests # Se mantiene para la llamada a get_dolar_vendedor_variable_id
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Cargar variables de entorno del archivo .env
# Navegar dos niveles arriba (src -> exercise2_bcra_api -> sudata-de-challenge-kpojoa)
current_dir = os.path.dirname(__file__)
project_root = os.path.join(current_dir, '..', '..') # Dos niveles arriba para llegar a la raíz del repo
load_dotenv(os.path.join(project_root, '.env'))

# --- Credenciales y Configuraciones ---
BCRA_API_TOKEN = os.getenv("BCRA_API_TOKEN") # Se mantiene, usado por get_dolar_vendedor_variable_id
BCRA_API_BASE_URL = os.getenv("BCRA_API_BASE_URL") # Se mantiene, usado por get_dolar_vendedor_variable_id
BCRA_API_ENDPOINT_V3_MONETARIAS = os.getenv("BCRA_API_ENDPOINT_V3_MONETARIAS") # Se mantiene
BCRA_API_ENDPOINT_V3_LIST_VARIABLES = os.getenv("BCRA_API_ENDPOINT_V3_LIST_VARIABLES") # Se mantiene

# Credenciales para la Base de Datos de Destino (Supabase)
DB_CLOUD_HOST = os.getenv("DB_CLOUD_HOST")
DB_CLOUD_PORT = os.getenv("DB_CLOUD_PORT")
DB_CLOUD_NAME = os.getenv("DB_CLOUD_NAME")
DB_CLOUD_USER = os.getenv("DB_CLOUD_USER")
DB_CLOUD_PASSWORD = os.getenv("DB_CLOUD_PASSWORD")

# --- NUEVA: Ruta al CSV de ejemplo para la carga de datos ---
# El script ahora cargará los datos desde este CSV para asegurar la funcionalidad del pipeline.
# La API del BCRA demostró no proveer un historial confiable para el "dólar vendedor" a través de los endpoints probados.
SAMPLE_COTIZACIONES_CSV_PATH = os.path.join(current_dir, '..', 'data', 'sample_cotizaciones.csv')


# --- Configuración de la Base de Datos ---
def get_cloud_db_engine():
    """Crea y devuelve un motor de SQLAlchemy para la base de datos en la nube."""
    db_connection_str = f"postgresql+psycopg2://{DB_CLOUD_USER}:{DB_CLOUD_PASSWORD}@{DB_CLOUD_HOST}:{DB_CLOUD_PORT}/{DB_CLOUD_NAME}"
    return create_engine(db_connection_str)

# --- Definición DDL de la tabla 'cotizaciones' ---
COTIZACIONES_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS cotizaciones (
    fecha DATE PRIMARY KEY,
    moneda TEXT NOT NULL,
    tipo_cambio NUMERIC(10, 4) NOT NULL,
    fuente TEXT NOT NULL DEFAULT 'BCRA',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
"""

def create_cotizaciones_table(engine):
    """Crea la tabla 'cotizaciones' en la base de datos de destino si no existe."""
    print("Creando/Verificando la tabla 'cotizaciones' en la base de datos en la nube...")
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text(COTIZACIONES_TABLE_DDL))
    print("Tabla 'cotizaciones' creada/verificada exitosamente.")

def get_last_loaded_date(engine):
    """
    Obtiene la última fecha registrada en la tabla 'cotizaciones'.
    Retorna la fecha más reciente o None si la tabla está vacía.
    """
    query = text("SELECT MAX(fecha) FROM cotizaciones")
    with engine.connect() as connection:
        result = connection.execute(query).scalar()
        return result

# Se mantiene la función get_dolar_vendedor_variable_id() pero solo para demostración del ID
def get_dolar_vendedor_variable_id():
    """
    Intenta obtener el ID de la variable "Dólar Vendedor" de la API.
    Este paso se mantiene para demostrar la capacidad de interactuar con la API
    para obtener metadatos, aunque el historial de datos no esté disponible.
    """
    headers = {"Authorization": f"BEARER {BCRA_API_TOKEN}"}
    url = f"{BCRA_API_BASE_URL}{BCRA_API_ENDPOINT_V3_LIST_VARIABLES}"
    print(f"Buscando ID de variable para 'Dólar Vendedor' en: {url} (Solo para referencia)...")
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get('results'):
            for var in data['results']:
                desc = var.get('descripcion', '').lower()
                cat = var.get('categoria', '').lower()
                if (("dólar" in desc or "usd" in desc) and ("vendedor" in desc or "minorista" in desc)) or \
                   ("tipos de cambio" in cat and ("dólar" in desc or "usd" in desc)):
                    print(f"  ¡ID de Dólar Vendedor encontrado! ID: {var['idVariable']}, Descripción: {var['descripcion']}, Categoría: {var['categoria']}")
                    return var['idVariable']
            print("No se encontró un ID de variable para 'Dólar Vendedor' específico.")
        else:
            print("No se encontraron resultados al listar las variables.")
    except Exception as e:
        print(f"Error al intentar obtener ID de variable de la API: {e}. Continuaremos con la carga del CSV.")
    return None # Retorna None, ya que no se usará para la carga principal

# Variable global para almacenar el ID de la variable (no se usará para la carga principal)
GLOBAL_DOLAR_VENDEDOR_ID = None

# --- Función para extraer datos (AHORA DESDE CSV) ---
def fetch_cotizaciones_data_from_source(start_date_obj, end_date_obj):
    """
    Carga los datos de cotizaciones desde el CSV de ejemplo.
    Simula la extracción de la API para demostrar el pipeline.
    """
    print(f"Cargando datos de cotizaciones desde el archivo de ejemplo: {SAMPLE_COTIZACIONES_CSV_PATH}")
    try:
        df = pd.read_csv(SAMPLE_COTIZACIONES_CSV_PATH, encoding='latin1')
        
        # Procesar DataFrame (asegurar tipos de datos)
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date
        df['tipo_cambio'] = pd.to_numeric(df['tipo_cambio'], errors='coerce')
        df = df.dropna(subset=['tipo_cambio']) # Eliminar filas con tipo_cambio nulo

        # Filtrar por rango de fechas (simulando la API para ingesta incremental/por bloques)
        df = df[(df['fecha'] >= start_date_obj) & (df['fecha'] <= end_date_obj)]
        
        # Asegurar columnas finales
        df = df[['fecha', 'moneda', 'tipo_cambio', 'fuente']]
        
        print(f"Datos obtenidos desde CSV de ejemplo para el rango: {len(df)} registros.")
        return df
    except Exception as e:
        print(f"ERROR al cargar datos desde el CSV de ejemplo: {e}")
        return pd.DataFrame()

# --- Función principal del pipeline (run_bcra_pipeline) ---
def run_bcra_pipeline():
    """
    Función principal para el pipeline de extracción y carga de cotizaciones.
    Ahora carga desde un CSV de ejemplo, demostrando la lógica del pipeline.
    """
    global GLOBAL_DOLAR_VENDEDOR_ID # Se mantendrá para mostrar el ID encontrado, no para la carga.

    cloud_engine = None
    try:
        cloud_engine = get_cloud_db_engine()
        create_cotizaciones_table(cloud_engine) # Asegura que la tabla exista

        # Este paso es para demostrar que podemos obtener el ID si la API funcionara
        # Pero no se usará para la carga de datos del pipeline.
        GLOBAL_DOLAR_VENDEDOR_ID = get_dolar_vendedor_variable_id() 
        if GLOBAL_DOLAR_VENDEDOR_ID:
            print(f"ID de Dólar Vendedor {GLOBAL_DOLAR_VENDEDOR_ID} encontrado en la API. Sin embargo, se cargará desde CSV por problemas de historial en la API.")
        else:
            print("No se pudo obtener el ID de Dólar Vendedor de la API. Se cargará desde CSV.")


        last_date = get_last_loaded_date(cloud_engine)

        if last_date:
            start_date_pull = last_date + timedelta(days=1)
            print(f"Modo incremental: Última fecha cargada: {last_date}. Consultando desde: {start_date_pull.strftime('%Y-%m-%d')}")
        else:
            # Si es la primera carga (histórica), empezamos desde la fecha más antigua del CSV de ejemplo
            start_date_pull = datetime(2024, 1, 1).date() # Fecha de inicio de tu sample_cotizaciones.csv
            print(f"Modo histórico: No hay datos en la DB. Consultando desde: {start_date_pull.strftime('%Y-%m-%d')} (desde CSV de ejemplo)")
        
        end_date_today = datetime.now().date()
        
        if start_date_pull > end_date_today:
            print("La base de datos ya está actualizada. No hay nuevas cotizaciones para extraer.")
            return

        # --- Carga de Datos desde el CSV de ejemplo (Simulando el flujo de ingesta) ---
        # Llamamos a la función de fetch_cotizaciones_data_from_source con el rango deseado.
        
        print(f"\nExtrayendo y cargando datos desde {start_date_pull.strftime('%Y-%m-%d')} hasta {end_date_today.strftime('%Y-%m-%d')} (desde CSV de ejemplo)...")
        df_cotizaciones = fetch_cotizaciones_data_from_source(start_date_pull, end_date_today)

        if not df_cotizaciones.empty:
            print(f"Cargando {len(df_cotizaciones)} cotizaciones en la nube...")
            with cloud_engine.connect() as connection:
                df_cotizaciones.to_sql('cotizaciones', connection, if_exists='append', index=False)
            print("Cotizaciones cargadas exitosamente.")
            total_loaded_rows = len(df_cotizaciones)
        else:
            print("No hay datos disponibles para cargar en este rango desde el CSV.")
            total_loaded_rows = 0

        if total_loaded_rows > 0:
            print(f"\n¡Pipeline de ingesta de cotizaciones completado! Total de {total_loaded_rows} registros cargados.")
        else:
            print("\nProceso de ingesta de cotizaciones finalizado. No se cargaron nuevos registros.")

    except Exception as e:
        print(f"\nERROR en el pipeline de cotizaciones: {e}")
    finally:
        if cloud_engine:
            cloud_engine.dispose()
        print("Conexión a la base de datos en la nube cerrada.")

if __name__ == "__main__":
    run_bcra_pipeline()
    print("Proceso de ingesta de API BCRA (simulada) finalizado.")