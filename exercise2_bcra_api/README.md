# Ejercicio 2: Extracción Incremental desde la API del BCRA

Este documento detalla la solución implementada para el segundo ejercicio del Desafío Técnico de Data Engineer de Sudata.

---

### Objetivo del Ejercicio

El objetivo principal fue construir un pipeline para extraer cotizaciones históricas del dólar tipo vendedor desde la API pública del Banco Central de la República Argentina (BCRA), almacenarlas en una tabla PostgreSQL en la nube, e implementar un mecanismo de ingesta incremental semanal automatizado.

### Tecnologías Clave

*   **Python 3.x:** Lenguaje de desarrollo.
*   **PostgreSQL (Supabase):** Base de datos destino en la nube.
*   **`pandas`, `sqlalchemy`, `requests`:** Librerías Python para ETL y consumo de API.
*   **GitHub Actions:** Orquestación y automatización semanal.

### Diseño del Pipeline e Ingesta Incremental

El pipeline está diseñado para realizar una ingesta incremental, consultando la última fecha cargada en la base de datos de destino y extrayendo solo los datos posteriores a esta. La tabla `cotizaciones` en PostgreSQL (`fecha` PK, `moneda`, `tipo_cambio`, `fuente`) almacena las cotizaciones.

### **Desafío Central: Consumo de la API del BCRA y Justificación de la Solución**

Este ejercicio presentó un desafío significativo debido a la inconsistencia y ambigüedad en el comportamiento de la API pública del BCRA para la extracción de datos históricos.

1.  **Problemas Detectados:**
    *   A pesar de identificar el `idVariable` correcto (`ID: 4, "Tipo de Cambio Minorista"`) a través del endpoint de listado de variables (`/estadisticas/v3.0/Monetarias`), las consultas históricas a `/api/v3.0/Monetarias/{idVariable}` consistentemente devolvieron `404 Not Found` para cualquier rango de fechas probado, incluso para rangos recientes o variables con historial conocido (como la UVA).
    *   La documentación no especificaba límites de rango o comportamientos especiales que explicaran estos `404`s al acceder a series históricas.
    *   La opción de consultar la API día a día para el historial completo no es viable debido al límite de 100 peticiones diarias del token de la API, lo que tomaría meses para cargar la historia completa.

2.  **Solución Implementada y Justificación Profesional:**
    Ante la imposibilidad de obtener datos históricos fiables y eficientes de la API oficial para la demostración del pipeline, se tomó la decisión pragmática de **simular la fuente de datos utilizando un archivo CSV de ejemplo (`sample_cotizaciones.csv`).**

    Esta elección permite:
    *   **Demostrar la funcionalidad completa del pipeline:** Incluyendo la lógica de ingesta incremental (basada en la última fecha cargada), el procesamiento de datos, la carga a la base de datos en la nube y la automatización.
    *   **Priorizar la entrega de una solución funcional y reproducible** para el desafío, en lugar de quedar bloqueado por las limitaciones de una fuente externa.
    *   La estructura del código es modular y permite una fácil re-integración con la API real si sus problemas se resuelven en el futuro.

### Automatización y Credenciales Seguras

El pipeline se automatiza semanalmente mediante **GitHub Actions** (`.github/workflows/exercise2_bcra_api.yml`), programado para ejecutarse cada lunes. Todas las credenciales de la base de datos y de la API se gestionan de forma segura a través de **GitHub Secrets**, garantizando que no se expongan en el código fuente.

### Acceso y Verificación

Los datos cargados se pueden verificar en la tabla `cotizaciones` en tu base de datos Supabase en la nube.

*   **Consola de Supabase:** `https://app.supabase.com/`
*   **Credenciales de Conexión (Transaction Pooler):**
    *   Host: `aws-0-us-east-2.pooler.supabase.com` (o el tuyo)
    *   Port: `6543`
    *   Usuario: `postgres.tu_id_de_proyecto_supabase`
    *   Nombre de la BD: `postgres`

---

**Autor:** Joaquin Ramirez (JoaquinRamirez98)
**Fecha:** [03 de Agosto de 2025]