import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_ORIGIN_HOST")
DB_PORT = os.getenv("DB_ORIGIN_PORT")
DB_USER = os.getenv("DB_ORIGIN_USER")
DB_PASSWORD = os.getenv("DB_ORIGIN_PASSWORD")
DB_NAME = os.getenv("DB_ORIGIN_NAME")

DATA_DIR = "data"

# Nombres de los archivos CSV y sus correspondientes tablas
# NOTA: EL ORDEN ES IMPORTANTE PARA LAS CLAVES FORÁNEAS
csv_tables_mapping = [
    ("DimDate.csv", "dim_date"),
    ("DimCustomerSegment.csv", "dim_customer_segment"),
    ("DimProduct.csv", "dim_product"),
    ("FactSales.csv", "fact_sales") # Esta se procesará con una transformación extra
]

def load_data_to_origin_db():
    print(f"Conectando a la base de datos '{DB_NAME}' para cargar datos...")
    db_connection_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = None
    try:
        engine = create_engine(db_connection_str)

        for csv_file, table_name in csv_tables_mapping:
            file_path = os.path.join(DATA_DIR, csv_file)
            print(f"Cargando {csv_file} en la tabla {table_name}...")
            
            df = pd.read_csv(file_path)

            # --- Transformaciones específicas para cada tabla si es necesario ---
            if table_name == "fact_sales":
                # Calcular "MontoTotal" a partir de "Price_PerUnit" y "QuantitySold"
                df["MontoTotal"] = df["Price_PerUnit"] * df["QuantitySold"]
                print(f"Columna 'MontoTotal' calculada para {table_name}.")

                df['Dateid'] = pd.to_numeric(df['Dateid'], errors='coerce').astype(int)
                df['Productid'] = pd.to_numeric(df['Productid'], errors='coerce').astype(int)
                df['Segmentid'] = pd.to_numeric(df['Segmentid'], errors='coerce').astype(int)
            
            elif table_name == "dim_date":

                df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date

                for col in ['dateid', 'Year', 'Quarter', 'Month', 'Day', 'Weekday']:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype(int)

            elif table_name == "dim_customer_segment":
                df['Segmentid'] = pd.to_numeric(df['Segmentid'], errors='coerce').astype(int)
            
            elif table_name == "dim_product":
                df['Productid'] = pd.to_numeric(df['Productid'], errors='coerce').astype(int)


            # Cargar el DataFrame en la tabla de PostgreSQL
            # Usamos 'append' para añadir datos, o 'replace' si queremos borrar y recrear en cada corrida.
            # Para la carga inicial, 'append' está bien si no hay IDs duplicados en el CSV.
            # Para reintentos, si ya tienes datos cargados, deberías limpiar la tabla antes o usar 'replace'.
            df.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Datos de {csv_file} cargados exitosamente en {table_name}.")

        print("Todos los datos han sido cargados a la base de datos de origen.")

    except Exception as e:
        print(f"Error al cargar datos: {e}")

    finally:
        if engine:
            engine.dispose()

if __name__ == "__main__":
    load_data_to_origin_db()
    print("Proceso de carga de datos origen finalizado.")