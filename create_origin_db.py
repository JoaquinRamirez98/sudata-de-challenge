import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_ORIGIN_HOST")
DB_PORT = os.getenv("DB_ORIGIN_PORT")
DB_USER = os.getenv("DB_ORIGIN_USER")
DB_PASSWORD = os.getenv("DB_ORIGIN_PASSWORD")
DB_NAME = os.getenv("DB_ORIGIN_NAME")

def create_database():
    print(f"Intentando conectar para crear la base de datos '{DB_NAME}'...")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database="postgres"
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [DB_NAME])
        if not cur.fetchone():
            print(f"La base de datos '{DB_NAME}' no existe. Creándola...")
            cur.execute(sql.SQL(f"CREATE DATABASE {DB_NAME}"))
            print(f"Base de datos '{DB_NAME}' creada exitosamente.")
        else:
            print(f"La base de datos '{DB_NAME}' ya existe.")
    except Exception as e:
        print(f"Error al crear la base de datos: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

def create_tables():
    print(f"Intentando conectar a '{DB_NAME}' para crear tablas...")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Definiciones de esquema de tablas (SQL DDL)
        tables_sql = [
            """
            CREATE TABLE IF NOT EXISTS dim_date (
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
            CREATE TABLE IF NOT EXISTS dim_customer_segment (
                "Segmentid" INT PRIMARY KEY,
                "City" VARCHAR(100) NOT NULL
                -- No hay columna 'descripcion' en el CSV, por lo tanto, no se incluye aquí.
                -- Si se necesita para BI, podría ser una columna NULLable en el destino.
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_product (
                "Productid" INT PRIMARY KEY,
                "Producttype" VARCHAR(255) NOT NULL
                -- No hay 'nombre_producto', 'categoria', 'precio' directos en CSV.
                -- 'Producttype' será el nombre del producto.
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS fact_sales (
                "Salesid" VARCHAR(50) PRIMARY KEY, -- Salesid es String (ej. S1001)
                "Dateid" INT NOT NULL,
                "Productid" INT NOT NULL,
                "Segmentid" INT NOT NULL,
                "Price_PerUnit" NUMERIC(10, 2) NOT NULL,
                "QuantitySold" INT NOT NULL,
                "MontoTotal" NUMERIC(10, 2), -- Esta columna la calcularemos en Python
                FOREIGN KEY ("Productid") REFERENCES dim_product ("Productid"),
                FOREIGN KEY ("Segmentid") REFERENCES dim_customer_segment ("Segmentid"),
                FOREIGN KEY ("Dateid") REFERENCES dim_date ("dateid")
            );
            """
        ]

        for table_sql in tables_sql:
            cur.execute(table_sql)
            print(f"Tabla creada/verificada: {table_sql.splitlines()[1].strip()}")
        
        print("Todas las tablas han sido creadas o ya existían.")

    except Exception as e:
        print(f"Error al crear tablas: {e}")
    finally:
        if cur: cur.close()
        if conn: conn.close()

if __name__ == "__main__":
    create_database()
    create_tables()
    print("Proceso de configuración de la base de datos origen finalizado.")