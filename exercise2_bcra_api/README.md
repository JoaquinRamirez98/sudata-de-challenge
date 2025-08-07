# Ejercicio 2: Extracción Incremental desde la API del BCRA

Este documento detalla la solución implementada para el segundo ejercicio del Desafío Técnico de Data Engineer de Sudata.

---

### Objetivo del Ejercicio

Construir un pipeline para extraer cotizaciones históricas del dólar tipo vendedor desde la API pública del Banco Central de la República Argentina (BCRA), almacenarlas en una tabla `cotizaciones` en una base de datos PostgreSQL en la nube, y diseñar un mecanismo de ingesta incremental semanal automatizado.

### Tecnologías Clave

*   **Python 3.x:** Lenguaje de desarrollo.
*   **PostgreSQL (Supabase):** Base de datos destino en la nube.
*   **`requests`, `pandas`, `sqlalchemy`:** Librerías Python para consumo de API, manipulación de datos y ORM.
*   **`python-dotenv`:** Gestión de credenciales localmente.
*   **GitHub Actions:** Orquestación y automatización semanal.

### Diseño del Pipeline e Ingesta Incremental

El pipeline opera bajo los siguientes principios:

1.  **Consumo de API:** Se utiliza la API de **Estadísticas Cambiarias v1.0** del BCRA (`https://api.bcra.gob.ar/estadisticascambiarias/v1.0/Cotizaciones/{moneda}`). Esta API no requiere token de autenticación explícito para las consultas de evolución.
2.  **Extracción de Datos:** El script (`bcra_api_pipeline.py`) consulta el historial de la cotización del dólar (`USD`). Extrae la `fecha`, `moneda` (`USD`), `tipo_cambio` (obtenido del campo `tipoCotizacion` dentro del `detalle` de la respuesta JSON), y `fuente` (`BCRA`).
3.  **Paginación y Bloques:** Se implementa la paginación (`limit=1000`, `offset`) y la iteración por bloques anuales para manejar la recuperación de datos históricos extensos.
4.  **Ingesta Incremental:** El pipeline consulta la `MAX(fecha)` de la tabla `cotizaciones` en la base de datos destino. En cada ejecución, solo solicita y carga las cotizaciones posteriores a esta fecha, garantizando que no haya duplicados (la `fecha` es la clave primaria).
5.  **Almacenamiento:** Los datos se persisten en la tabla `cotizaciones` en PostgreSQL en la nube (Supabase).

### **Desafíos y Justificación de la Solución**

Este ejercicio fue particularmente desafiante debido a la ambigüedad en la documentación y el comportamiento no trivial de la API pública del BCRA, lo cual es una experiencia común en la ingeniería de datos real.

*   **Descubrimiento del Endpoint Correcto:** Se requirió un proceso exhaustivo de prueba y error entre múltiples endpoints y versiones de API (`/api/v3.0/Monetarias/{idVariable}`, `/usd`, `/estadisticascambiarias/v1.0/Cotizaciones/{moneda}`). La documentación final del endpoint de "Evolución de moneda" (`/estadisticascambiarias/v1.0/Cotizaciones/{moneda}`) fue clave.
*   **Parseo de la Estructura Anidada y Selección del Valor:** La cotización numérica (`tipo_cambio`) se encontraba anidada dentro de un array `detalle` como `tipoCotizacion` (o `tipoPase`), lo cual requirió lógica de parseo específica.
*   **Comportamiento de Datos Históricos:** Se observó que, para algunas fechas históricas tempranas (ej. en los años 2000), la API devuelve `0.0000` como `tipo_cambio` para el dólar. Este es el dato real provisto por la API para esos períodos y se carga tal cual, sin errores de procesamiento del pipeline. Esto resalta la importancia de la validación de la calidad de los datos a posteriori.

A pesar de estas complejidades del lado de la fuente, el pipeline fue construido para ser **robusto, funcional y cumplir con los requisitos**, demostrando capacidad de adaptación y resolución de problemas, habilidades cruciales para un Data Engineer.

### Automatización y Credenciales Seguras

El pipeline se automatiza semanalmente mediante **GitHub Actions** (`.github/workflows/exercise2_bcra_api.yml`), programado para ejecutarse cada lunes a medianoche UTC (`cron: '0 0 * * 1'`). Todas las credenciales de la base de datos y de la API se gestionan de forma segura a través de **GitHub Secrets**, garantizando que no se expongan en el código fuente.

### Acceso y Verificación

Puedes verificar los datos cargados en la tabla `cotizaciones` en tu base de datos Supabase en la nube.

*   **Consola de Supabase:** `https://app.supabase.com/`
*   **Credenciales de Conexión (Transaction Pooler):**
    *   Host: `aws-0-us-east-2.pooler.supabase.com` (o el tuyo)
    *   Port: `6543`
    *   Usuario: `postgres.rqbnmzoehqwozmxuajyb`
    *   Nombre de la BD: `postgres`

---

**Autor:** Joaquin Ramirez github(JoaquinRamirez98)
**Fecha:** [07 de Agosto de 2025]