# Desafío Técnico Data Engineer - Sudata

Este repositorio contiene la solución al Ejercicio 1: **Replicación de Base de Datos**, parte del desafío técnico de Data Engineer para Sudata.

---

## Ejercicio 1: Replicación de Base de Datos

### Objetivo General

Implementar un pipeline robusto para la replicación diaria de datos de ventas y productos desde una base de datos PostgreSQL de origen (simulada localmente) hacia una base de datos espejo en la nube (PostgreSQL en Supabase), con énfasis en la automatización y el modelado básico para Business Intelligence.

### Tecnologías Clave Utilizadas

*   **Python 3.x:** Lenguaje principal de desarrollo.
*   **PostgreSQL:** Bases de datos de origen (local) y destino (Supabase).
*   **Pandas & SQLAlchemy:** Librerías Python para extracción, transformación y carga de datos (ETL).
*   **GitHub Actions:** Orquestación y automatización del pipeline.
*   **`python-dotenv`:** Gestión segura de credenciales en entorno local.

### Diseño y Ejecución del Pipeline

El pipeline opera bajo una estrategia de **"truncate-and-load" (vaciar y cargar)** para asegurar una réplica completa y consistente en cada ejecución.

1.  **Configuración de Origen:**
    *   Una base de datos PostgreSQL (`sudata_origin_db`) se crea y popula localmente a partir de archivos CSV (`DimDate.csv`, `DimCustomerSegment.csv`, `DimProduct.csv`, `FactSales.csv`).
    *   El script `create_origin_db.py` define los esquemas de tablas (`dim_date`, `dim_customer_segment`, `dim_product`, `fact_sales`), mapeando directamente a los nombres de columnas de los CSV, incluyendo la capitalización original.
    *   El script `load_origin_data.py` carga los datos, incluyendo la **transformación** de `MontoTotal` en `fact_sales` (calculado como `Price_PerUnit * QuantitySold`).

2.  **Configuración de Destino en la Nube:**
    *   Una instancia de PostgreSQL en la nube se provisiona usando **Supabase** (plan gratuito), conectándose a través de su **Transaction Pooler** para asegurar compatibilidad con IPv4 y escalabilidad básica.

3.  **Script de Replicación (`replication_pipeline.py`):**
    *   Se conecta a ambas bases de datos utilizando credenciales gestionadas por variables de entorno (cargadas vía `.env` localmente o GitHub Secrets en producción).
    *   **Proceso de Carga:** Primero, se borran las tablas existentes en el destino en orden inverso de dependencia (hechos, luego dimensiones) para evitar conflictos de claves foráneas. Luego, se recrean las tablas con el mismo esquema del origen y se cargan los datos extraídos y transformados.

4.  **Automatización Diaria:**
    *   El pipeline se automatiza con **GitHub Actions**. El workflow (`.github/workflows/replicate_db.yml`) está configurado para ejecutarse **una vez al día a medianoche UTC** (`cron: '0 0 * * *'`) y puede ser disparado manualmente (`workflow_dispatch`).
    *   Las credenciales de acceso a ambas bases de datos se gestionan de forma segura mediante **GitHub Secrets**.

### Esquema del Modelo de Datos para BI (Destino)

El esquema de la base de datos destino (`sudata_origin_db` en Supabase) es el siguiente, replicando la estructura de las tablas de origen y ajustado para BI:

*   `"dim_date"`: Contiene atributos de tiempo.
*   `"dim_customer_segment"`: Contiene información sobre segmentos de clientes.
*   `"dim_product"`: Contiene detalles de productos.
*   `"fact_sales"`: Tabla de hechos de ventas, con claves foráneas a las dimensiones y la métrica calculada `"MontoTotal"`.

### Acceso y Verificación

Para verificar la replicación, puedes acceder a la base de datos en la nube:

*   **Consola de Supabase:** [https://app.supabase.com/](https://app.supabase.com/)
*   **Credenciales de Conexión (Transaction Pooler):**
    *   Host: `aws-0-us-east-2.pooler.supabase.com` (o el que te corresponda)
    *   Port: `6543`
    *   Usuario: `postgres.rqbnmzoehqwozmxuajyb`
    *   Nombre de la BD: `postgres`

### Consideraciones Profesionales

*   **Reusabilidad y Modularidad:** Código organizado para facilitar el mantenimiento y futuras extensiones.
*   **Gestión de Credenciales:** Implementación de prácticas seguras para el manejo de credenciales (`.env` y GitHub Secrets).
*   **Fiabilidad:** La estrategia de truncate-and-load asegura la idempotencia del pipeline.
*   **Observabilidad:** El workflow de GitHub Actions proporciona un registro detallado de cada ejecución.
*   **Preparación para BI:** El modelado de datos y la transformación básica de `MontoTotal` facilitan el análisis posterior.

---

**Autor:** [Joaquin Ramirez] ([JoaquinRamirez98])
**Fecha:** [02 de Agosto de 2025]