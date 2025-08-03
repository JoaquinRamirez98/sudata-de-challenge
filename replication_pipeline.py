# sudata_de_challenge/replication_pipeline.py

import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Cargar variables de entorno del archivo .env
load_dotenv()

# --- Credenciales para la Base de Datos de Origen (Local) ---
DB_ORIGIN_HOST = os.getenv("DB_ORIGIN_HOST")
DB_ORIGIN_PORT = os.getenv("DB_ORIGIN_PORT")
DB_ORIGIN_NAME = os.getenv("DB_ORIGIN_NAME")
DB_ORIGIN_USER = os.getenv("DB_ORIGIN_USER")
DB_ORIGIN_PASSWORD = os.getenv("DB_ORIGIN_PASSWORD")

# --- Credenciales para la Base de Datos de Destino (Supabase) ---
DB_CLOUD_HOST = os.getenv("DB_CLOUD_HOST")
DB_CLOUD_PORT = os.getenv("DB_CLOUD_PORT")
DB_CLOUD_NAME = os.getenv("DB_CLOUD_NAME")
DB_CLOUD_USER = os.getenv("DB_CLOUD_USER")
DB_CLOUD_PASSWORD = os.getenv("DB_CLOUD_PASSWORD")

# Definición de las tablas a replicar y su orden (IMPORTANTE para FKs)
# Usamos los nombres de tabla y columna exactos (case-sensitive) como están en la DB
tables_to_replicate = [
    {"name": "dim_date", "pk": "dateid", "file_name": "DimDate.csv"},
    {"name": "dim_customer_segment", "pk": "Segmentid", "file_name": "DimCustomerSegment.csv"},
    {"name": "dim_product", "pk": "Productid", "file_name": "DimProduct.csv"},
    {"name": "fact_sales", "pk": "Salesid", "file_name": "FactSales.csv"}
]

def get_db_engine(db_type="origin"):
    """
    Crea y devuelve un motor de SQLAlchemy para la base de datos especificada.
    db_type: 'origin' para la DB local, 'cloud' para la DB en la nube.
    """
    if db_type == "origin":
        host = DB_ORIGIN_HOST
        port = DB_ORIGIN_PORT
        name = DB_ORIGIN_NAME
        user = DB_ORIGIN_USER
        password = DB_ORIGIN_PASSWORD
    elif db_type == "cloud":
        host = DB_CLOUD_HOST
        port = DB_CLOUD_PORT
        name = DB_CLOUD_NAME
        user = DB_CLOUD_USER
        password = DB_CLOUD_PASSWORD
    else:
        raise ValueError("db_type debe ser 'origin' o 'cloud'")

    db_connection_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return create_engine(db_connection_str)

def replicate_data():
    """
    Función principal para extraer datos de la DB origen, transformar y cargar en la DB destino.
    """
    origin_engine = None
    cloud_engine = None
    try:
        print("Iniciando pipeline de replicación...")

        origin_engine = get_db_engine("origin")
        cloud_engine = get_db_engine("cloud")

        # PASO 1: BORRAR TABLAS EXISTENTES EN LA NUBE EN ORDEN DE DEPENDENCIA INVERSA
        # Esto es crucial para la estrategia truncate-and-load con FKs
        print("\nBorrando tablas existentes en la base de datos de destino (en orden inverso de dependencia)...")
        tables_to_drop_order = [
            "fact_sales",
            "dim_product",
            "dim_customer_segment",
            "dim_date"
        ]
        
        with cloud_engine.connect() as connection:
            with connection.begin():
                for table_name in tables_to_drop_order:
                    print(f"Borrando tabla: {table_name} (si existe)...")
                    # Usamos CASCADE para asegurar que las FKs se manejen si hay dependencias no esperadas
                    connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;'))
        print("Tablas borradas/verificadas en el destino.")


        # PASO 2: CREAR ESQUEMAS EN LA NUBE (Despues de borrar, recreamos limpiamente)
        print("\nCreando/recreando esquemas en la nube...")
        tables_ddl = [
            """
            CREATE TABLE IF NOT EXISTS "dim_date" (
                "dateid" INT PRIMARY KEY,
                "date" DATE NOT NULL UNIQUE,
                "Year" INT,
                "Quarter" INT,
                "QuarterName" VARCHAR(20),
                "Month" INT,
                "Monthname" VARCHAR(20),
                "Day" INT,
                "Weekday" INT,
                "WeekdayName" VARCHAR(15)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "dim_customer_segment" (
                "Segmentid" INT PRIMARY KEY,
                "City" VARCHAR(100) NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "dim_product" (
                "Productid" INT PRIMARY KEY,
                "Producttype" VARCHAR(255) NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "fact_sales" (
                "Salesid" VARCHAR(50) PRIMARY KEY,
                "Dateid" INT NOT NULL,
                "Productid" INT NOT NULL,
                "Segmentid" INT NOT NULL,
                "Price_PerUnit" NUMERIC(10, 2) NOT NULL,
                "QuantitySold" INT NOT NULL,
                "MontoTotal" NUMERIC(10, 2),
                FOREIGN KEY ("Productid") REFERENCES dim_product ("Productid"),
                FOREIGN KEY ("Segmentid") REFERENCES dim_customer_segment ("Segmentid"),
                FOREIGN KEY ("Dateid") REFERENCES dim_date ("dateid")
            );
            """
        ]
        
        with cloud_engine.connect() as connection:
            with connection.begin():
                for table_sql in tables_ddl:
                    print(f"Creando/verificando tabla en la nube: {table_sql.splitlines()[1].strip()}")
                    connection.execute(text(table_sql))
        print("Esquemas en la nube creados/verificados.")


        # PASO 3: EXTRAER, TRANSFORMAR Y CARGAR (ETL) - CAMBIAR if_exists a 'append'
        for table_info in tables_to_replicate:
            table_name_origin = table_info["name"]
            table_name_cloud = table_info["name"]

            print(f"\nProcesando tabla: {table_name_origin}")

            print(f"Extrayendo datos de '{table_name_origin}' (origen)...")
            df = pd.read_sql_table(table_name_origin, origin_engine, schema='public')
            print(f"Extraídas {len(df)} filas de '{table_name_origin}'.")

            # --- Transformaciones (sin cambios) ---
            if table_name_origin == "fact_sales":
                df["MontoTotal"] = df["Price_PerUnit"] * df["QuantitySold"]
                print(f"Columna 'MontoTotal' calculada para {table_name_cloud}.")
            
            # Ajustes de tipo de datos (sin cambios)
            if table_name_origin == "dim_date":
                df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
                for col in ['dateid', 'Year', 'Quarter', 'Month', 'Day', 'Weekday']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif table_name_origin == "dim_customer_segment":
                if 'Segmentid' in df.columns:
                    df['Segmentid'] = pd.to_numeric(df['Segmentid'], errors='coerce').astype('Int64')
            elif table_name_origin == "dim_product":
                if 'Productid' in df.columns:
                    df['Productid'] = pd.to_numeric(df['Productid'], errors='coerce').astype('Int64')
            elif table_name_origin == "fact_sales":
                if 'Dateid' in df.columns:
                    df['Dateid'] = pd.to_numeric(df['Dateid'], errors='coerce').astype('Int64')
                if 'Productid' in df.columns:
                    df['Productid'] = pd.to_numeric(df['Productid'], errors='coerce').astype('Int64')
                if 'Segmentid' in df.columns:
                    df['Segmentid'] = pd.to_numeric(df['Segmentid'], errors='coerce').astype('Int64')


            # Cargar datos en la base de datos destino (Nube)
            print(f"Cargando {len(df)} filas en '{table_name_cloud}' (destino en la nube)...")
            # CAMBIO CLAVE AQUÍ: if_exists='append' porque ya hicimos el DROP TABLE antes
            df.to_sql(table_name_cloud, cloud_engine, if_exists='append', index=False, schema='public')
            print(f"Datos de '{table_name_cloud}' cargados exitosamente en la nube.")

        print("\n¡Pipeline de replicación completado exitosamente!")

    except Exception as e:
        print(f"\nERROR en el pipeline de replicación: {e}")
        # Considerar un logging más robusto aquí en un entorno real.
    finally:
        if origin_engine:
            origin_engine.dispose()
        if cloud_engine:
            cloud_engine.dispose()
        print("Conexiones a bases de datos cerradas.")

if __name__ == "__main__":
    replicate_data()