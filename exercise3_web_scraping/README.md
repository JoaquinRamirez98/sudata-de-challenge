# Ejercicio 3: Web Scraping de Propiedades en Venta

Este documento describe la solución implementada para el tercer ejercicio del Desafío Técnico de Data Engineer de Sudata.

---

### Objetivo del Ejercicio

El objetivo principal fue construir un pipeline para realizar scraping de anuncios de venta de terrenos en Posadas, Misiones, desde el marketplace Zonaprop.com.ar. La información extraída debía almacenarse en una base de datos PostgreSQL en la nube.

### Tecnologías Clave

*   **Python 3.x:** Lenguaje de desarrollo.
*   **PostgreSQL (Supabase):** Base de datos destino en la nube.
*   **Selenium & `webdriver_manager`:** Para automatizar la navegación del navegador (Chrome) y superar las medidas anti-scraping del sitio.
*   **BeautifulSoup4:** Para parsear el contenido HTML y extraer los datos.
*   **Pandas & SQLAlchemy:** Para manipulación de datos y carga a la base de datos.

### Diseño y Ejecución del Pipeline

El pipeline navega por las páginas de listado de Zonaprop para terrenos en Posadas, Misiones, y extrae la información visible de al menos 20 anuncios.

1.  **Herramienta de Scraping:** Se optó por **Selenium** debido a que `requests` directo fue bloqueado por el sitio (`403 Forbidden`). Selenium simula un navegador completo (Google Chrome), permitiendo la carga de contenido dinámico y evitando detecciones de bot basadas en headers o JavaScript.
2.  **Extracción de Datos:** Por cada anuncio en la página de listado, se extraen los siguientes campos disponibles:
    *   `id_anuncio` (ID único del anuncio, extraído de la URL)
    *   `titulo` (Título del anuncio)
    *   `ubicacion` (Dirección/ubicación aproximada)
    *   `precio_moneda` (Moneda del precio)
    *   `precio_valor` (Valor numérico del precio)
    *   `metros_cuadrados_terreno` (m² totales)
    *   `url_anuncio` (URL directa al anuncio)
    *   `descripcion` (Snippet de descripción)
    *   `fecha_scrapeo` (Fecha de la extracción)
3.  **Almacenamiento:** Los datos se almacenan en la tabla `propiedades_posadas` en la base de datos PostgreSQL en la nube (Supabase). La tabla incluye claves primarias y campos de auditoría (`created_at`, `updated_at`).

### **Consideraciones y Justificación de Limitaciones**

Durante el desarrollo, se identificaron las siguientes consideraciones y limitaciones con respecto a la fuente:

*   **Bloqueo por `requests`:** Zonaprop implementa medidas anti-scraping robustas, lo que hizo indispensable el uso de Selenium.
*   **Ausencia de Datos Detallados en Listado:** Campos como **coordenadas geográficas (`latitud`, `longitud`), frente del terreno (`frente_terreno_mts`) y largo del terreno (`largo_terreno_mts`)** generalmente no están disponibles en la vista de lista de anuncios. Extraer esta información requeriría navegar a la página de detalle individual de cada anuncio. Si bien es posible con Selenium, se decidió no implementar la visita a páginas de detalle para **optimizar el tiempo de ejecución y reducir el riesgo de bloqueo durante la prueba**, priorizando la obtención rápida de al menos 20 resultados de los campos principales desde la página de listado, tal como indica la consigna.
*   **Dinamicidad de Selectores:** Los selectores CSS pueden cambiar con el tiempo, requiriendo actualizaciones periódicas del código de scraping.

### Automatización (No Requerida, Pero Viable)

La consigna no solicitó la automatización semanal para este ejercicio. Sin embargo, el pipeline está diseñado de manera que podría ser fácilmente automatizado con GitHub Actions (similar a los ejercicios anteriores) para una ejecución periódica, si los términos de servicio del sitio lo permitieran y se gestionaran las protecciones anti-bot.

### Acceso y Verificación

Puedes verificar los datos scrapeados en la tabla `propiedades_posadas` en tu base de datos Supabase en la nube.

*   **Consola de Supabase:** `https://app.supabase.com/`
*   **Credenciales de Conexión (Transaction Pooler):**
    *   Host: `aws-0-us-east-2.pooler.supabase.com` (o el tuyo)
    *   Port: `6543`
    *   Usuario: `postgres.tu_id_de_proyecto_supabase`
    *   Nombre de la BD: `postgres`

---

**Autor:** Joaquin Ramirez (JoaquinRamirez98)
**Fecha:** [06 de Agosto de 2025]