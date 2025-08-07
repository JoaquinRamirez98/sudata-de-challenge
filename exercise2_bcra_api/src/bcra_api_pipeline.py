# sudata-de-challenge-kpojoa/exercise2_bcra_api/src/bcra_api_pipeline.py

import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Cargar variables de entorno del archivo .env
current_dir = os.path.dirname(__file__)
project_root = os.path.join(current_dir, '..', '..') # Dos niveles arriba para llegar a la raíz del repo
load_dotenv(os.path.join(project_root, '.env'))

# --- Credenciales y Configuraciones de la API del BCRA ---
# NOTA: La API de Estadísticas Cambiarias NO REQUIERE TOKEN de autenticación según la documentación.
# Sin embargo, lo mantendremos para consistencia con el .env si se necesita para otras APIs.
BCRA_API_TOKEN = os.getenv("BCRA_API_TOKEN") # Puede ser None o vacío, la API no lo usa.
BCRA_API_BASE_URL = os.getenv("BCRA_API_BASE_URL")
BCRA_API_ENDPOINT_EVOLUCION_MONEDA = os.getenv("BCRA_API_ENDPOINT_EVOLUCION_MONEDA") # Endpoint para historial por moneda
BCRA_API_COD_MONEDA = os.getenv("BCRA_API_COD_MONEDA") # Código ISO de la moneda (ej. 'USD')

# --- Credenciales para la Base de Datos de Destino (Supabase) ---
DB_CLOUD_HOST = os.getenv("DB_CLOUD_HOST")
DB_CLOUD_PORT = os.getenv("DB_CLOUD_PORT")
DB_CLOUD_NAME = os.getenv("DB_CLOUD_NAME")
DB_CLOUD_USER = os.getenv("DB_CLOUD_USER")
DB_CLOUD_PASSWORD = os.getenv("DB_CLOUD_PASSWORD")

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

# --- Función para extraer datos de la API de Evolución de Moneda ---
def fetch_bcra_dolar_data_evolution(start_date_str, end_date_str):
    """
    Consume el endpoint de evolución de moneda del BCRA
    (/estadisticascambiarias/v1.0/Cotizaciones/{moneda})
    para obtener datos de cotización del dólar vendedor en un rango de fechas,
    manejando la paginación.
    Retorna un DataFrame de Pandas.
    """
    headers = {
        # Esta API no requiere token, pero podemos enviar un User-Agent básico
        "User-Agent": "Mozilla/5.0 (compatible; BCRA_Data_Engineer_Challenge/1.0)",
    }
    
    all_df_data = []
    current_offset = 0
    limit_per_request = 1000 # Max limit from documentation is 1000

    # Construir la URL del endpoint
    # api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones/{moneda}
    url = f"{BCRA_API_BASE_URL}{BCRA_API_ENDPOINT_EVOLUCION_MONEDA}/{BCRA_API_COD_MONEDA}"
    
    print(f"Consultando API BCRA: {url} para fechas desde {start_date_str} hasta {end_date_str}...")

    while True: # Bucle para paginación
        params = {
            "fechadesde": start_date_str,
            "fechahasta": end_date_str,
            "limit": limit_per_request,
            "offset": current_offset
        }

        print(f"  Petición con offset={current_offset}, limit={limit_per_request} para rango {start_date_str} a {end_date_str}...")
        try:
            response = requests.get(url, headers=headers, params=params, verify=False, timeout=60) # Aumentar timeout
            response.raise_for_status() # Lanza una excepción para errores HTTP (4xx o 5xx)
            api_response_json = response.json()

            if not api_response_json.get('results'):
                print("  No se encontraron más datos para este rango o la respuesta es vacía.")
                break # Salir del bucle de paginación
            
            # Procesar los resultados: 'results' es una lista de objetos 'CotizacionesFecha'
            # Cada uno tiene 'fecha' y 'detalle'. 'detalle' es una lista.
            processed_records = []
            for item in api_response_json['results']:
                cotizacion_fecha = item.get('fecha')
                detalles = item.get('detalle', [])
                
                # Buscar el tipo de cambio vendedor en el detalle
                # La documentación del ejemplo para EUR muestra tipoPase como el valor principal.
                # Para USD vendedor, asumiremos que tipoPase es el valor a extraer.
                # Si hubiera múltiples detalles, se podría filtrar por descripcion="DOLAR VENDEDOR"
                
                # Suponiendo que el primer detalle de USD/ARS contiene el tipoPase deseado
                if detalles:
                    for detalle in detalles:
                        if detalle.get('codigoMoneda') == BCRA_API_COD_MONEDA: # Asegurarse de que sea la moneda correcta si hay varias
                            tipo_cambio_valor = detalle.get('tipoCotizacion')
                            processed_records.append({
                                'fecha': cotizacion_fecha,
                                'moneda': BCRA_API_COD_MONEDA, # Guardar como 'USD'
                                'tipo_cambio': tipo_cambio_valor,
                                'fuente': 'BCRA'
                            })
                            break

            if not processed_records:
                print(f"  No se encontraron registros relevantes de '{BCRA_API_COD_MONEDA}' para este bloque.")
                break # Si no hay registros procesados, salir de paginación
            
            df_chunk = pd.DataFrame(processed_records)
            
            # Convertir 'fecha' a tipo DATE
            df_chunk['fecha'] = pd.to_datetime(df_chunk['fecha']).dt.date
            df_chunk['tipo_cambio'] = pd.to_numeric(df_chunk['tipo_cambio'], errors='coerce')
            df_chunk = df_chunk.dropna(subset=['tipo_cambio'])

            if df_chunk.empty:
                print("  Chunk procesado vacío, terminando paginación.")
                break

            all_df_data.append(df_chunk)
            
            # Update offset for the next request
            if len(df_chunk) < limit_per_request: # If number of results is less than the limit, it's the last page
                break
            current_offset += limit_per_request
            
        except requests.exceptions.HTTPError as e:
            print(f"  Error HTTP al consultar la API (offset {current_offset}): {e.response.status_code} - {e.response.text}")
            return pd.DataFrame()
        except requests.exceptions.RequestException as e:
            print(f"  Error de conexión al consultar la API (offset {current_offset}): {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"  Error inesperado al procesar datos de la API (offset {current_offset}): {e}")
            return pd.DataFrame()
    
    if not all_df_data:
        print("No se obtuvieron datos de cotizaciones después de procesar todas las fechas y paginaciones.")
        return pd.DataFrame()

    df = pd.concat(all_df_data, ignore_index=True)
    
    df = df[['fecha', 'moneda', 'tipo_cambio', 'fuente']] # Reorder final columns
    
    print(f"Datos obtenidos de la API (total): {len(df)} registros.")
    return df

# --- Main pipeline function (run_bcra_pipeline) ---
def run_bcra_pipeline():
    """
    Main function for the BCRA exchange rate extraction and loading pipeline.
    It fetches historical data using the /estadisticascambiarias/v1.0/Cotizaciones/{moneda} endpoint.
    """
    cloud_engine = None
    try:
        cloud_engine = get_cloud_db_engine()
        create_cotizaciones_table(cloud_engine)

        last_date = get_last_loaded_date(cloud_engine)

        if last_date:
            start_date_pull = last_date + timedelta(days=1)
            print(f"Modo incremental: Última fecha cargada: {last_date}. Consultando desde: {start_date_pull.strftime('%Y-%m-%d')}")
        else:
            # Según la documentación, esta API puede tener historial hasta 2024-06-12 en ejemplos.
            # Para la carga histórica, vamos a intentar desde el inicio de la serie.
            start_date_pull = datetime(2002, 1, 1).date() # Fecha más antigua para la mayoría de series del BCRA
            print(f"Modo histórico: No hay datos en la DB. Consultando desde: {start_date_pull.strftime('%Y-%m-%d')}")
        
        end_date_today = datetime.now().date()
        
        if start_date_pull > end_date_today:
            print("La base de datos ya está actualizada. No hay nuevas cotizaciones para extraer.")
            return

        # Strategy to load data in annual blocks to manage API range limits and pagination
        current_block_start_date = start_date_pull
        total_loaded_rows = 0

        while current_block_start_date <= end_date_today:
            # Define the end of the block (e.g., end of the current year or end of the general range)
            next_year_start = (current_block_start_date.replace(year=current_block_start_date.year + 1, month=1, day=1))
            block_end_date = min(next_year_start - timedelta(days=1), end_date_today)

            print(f"\nProcesando bloque de fechas: {current_block_start_date.strftime('%Y-%m-%d')} a {block_end_date.strftime('%Y-%m-%d')}")
            df_cotizaciones_block = fetch_bcra_dolar_data_evolution(current_block_start_date.strftime('%Y-%m-%d'), block_end_date.strftime('%Y-%m-%d'))

            if not df_cotizaciones_block.empty:
                print(f"Cargando {len(df_cotizaciones_block)} cotizaciones del bloque en la nube...")
                with cloud_engine.connect() as connection:
                    df_cotizaciones_block.to_sql('cotizaciones', connection, if_exists='append', index=False)
                print("Cotizaciones del bloque cargadas exitosamente.")
                total_loaded_rows += len(df_cotizaciones_block)
            else:
                print("No hay datos para este bloque de fechas.")

            current_block_start_date = block_end_date + timedelta(days=1)

        if total_loaded_rows > 0:
            print(f"\n¡Pipeline de ingesta de API BCRA completado! Total de {total_loaded_rows} registros cargados.")
        else:
            print("\nProceso de ingesta de API BCRA finalizado. No se cargaron nuevos registros.")

    except Exception as e:
        print(f"\nERROR en el pipeline de BCRA API: {e}")
    finally:
        if cloud_engine:
            cloud_engine.dispose()
        print("Conexión a la base de datos en la nube cerrada.")

if __name__ == "__main__":
    run_bcra_pipeline()
    print("Proceso de ingesta de API BCRA finalizado.")