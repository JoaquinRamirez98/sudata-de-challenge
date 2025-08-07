# Desafío Técnico Data Engineer - Sudata

¡Bienvenido al repositorio de mi solución para el Desafío Técnico de Data Engineer propuesto por Sudata!

Este proyecto abarca la construcción integral de **pipelines de datos para Business Intelligence**, demostrando habilidades clave en la replicación, extracción (API y Web Scraping) y actualización de datos desde diversas fuentes hacia una base de datos en la nube.

---

## Estructura del Desafío y Habilidades Demostradas

El desafío está dividido en tres ejercicios clave, cada uno abordado con un enfoque profesional y las mejores prácticas de ingeniería de datos:

1.  **Ejercicio 1: Replicación de Base de Datos**
    *   **Habilidades:** Diseño de esquema ETL, replicación de datos de PostgreSQL local a PostgreSQL en la nube (Supabase), transformación de datos (`monto_total`), automatización diaria con GitHub Actions.
    *   **Ubicación:** `exercise1_replication/`

2.  **Ejercicio 2: Extracción Incremental desde API Externa (BCRA)**
    *   **Habilidades:** Consumo de APIs REST complejas (BCRA), manejo de autenticación (JWT), extracción de datos históricos y parseo de JSON anidado, implementación de ingesta incremental (basada en fecha), paginación de API, automatización semanal con GitHub Actions.
    *   **Consideración clave:** Diagnóstico y justificación de las particularidades de la API del BCRA.
    *   **Ubicación:** `exercise2_bcra_api/`

3.  **Ejercicio 3: Web Scraping de Propiedades en Venta**
    *   **Habilidades:** Web scraping de sitios dinámicos (Zonaprop), superación de medidas anti-scraping (uso de Selenium con Chrome), extracción de datos estructurados (título, precio, m², etc.), cálculo de métricas derivadas (`metros_cuadrados_terreno` a partir de frente/largo), almacenamiento en PostgreSQL en la nube.
    *   **Ubicación:** `exercise3_web_scraping/`

---

## Arquitectura General del Repositorio

El repositorio está organizado de forma modular, con una estructura clara para cada ejercicio y la centralización de configuraciones comunes.
sudata-de-challenge/
├── .github/ # Workflows de GitHub Actions para automatización de pipelines
│ └── workflows/
│ ├── exercise1_replication.yml
│ ├── exercise2_bcra_api.yml
│ └── exercise3_web_scraping.yml (si aplica automatización)
├── .env # Variables de entorno locales (credenciales sensibles, IGNORADO por Git)
├── .gitignore # Reglas para ignorar archivos y directorios por Git
├── README.md # Este archivo: Visión general y guía del desafío
├── exercise1_replication/ # Directorio para la solución del Ejercicio 1
│ ├── data/ # Archivos CSV de origen
│ ├── src/ # Scripts Python (.py) del pipeline
│ ├── requirements.txt # Dependencias Python específicas del ejercicio
│ └── README.md # Documentación detallada del Ejercicio 1
├── exercise2_bcra_api/ # Directorio para la solución del Ejercicio 2
│ ├── src/ # Scripts Python (.py) del pipeline
│ ├── requirements.txt # Dependencias Python específicas del ejercicio
│ └── README.md # Documentación detallada del Ejercicio 2
└── exercise3_web_scraping/ # Directorio para la solución del Ejercicio 3
├── src/ # Scripts Python (.py) del pipeline
├── requirements.txt # Dependencias Python específicas del ejercicio
└── README.md # Documentación detallada del Ejercicio 3
---

## Configuración y Ejecución

Para explorar o ejecutar cualquiera de los ejercicios, siga estos pasos:

### 1. Configuración General (Una sola vez por el repositorio)

1.  **Clonar el Repositorio:**
    git clone https://github.com/JoaquinRamirez98/sudata-de-challenge.git
    cd sudata-de-challenge

2.  **Crear Archivo `.env`:**
    En la raíz de este repositorio (`sudata-de-challenge/`), cree un archivo llamado `.env`. Este archivo debe contener **todas las credenciales y configuraciones** necesarias para las bases de datos (local y Supabase) y las APIs (BCRA).
    **¡ADVERTENCIA!** Este archivo está incluido en `.gitignore` y **NO debe subirse a GitHub** para proteger sus credenciales.
    *   Un ejemplo de la estructura esperada para el `.env` se encuentra en el `README.md` detallado de cada ejercicio.

3.  **Configurar GitHub Secrets:**
    Para la automatización con GitHub Actions, asegúrese de añadir todas las variables definidas en su `.env` como "Secrets" en la configuración del repositorio de GitHub (`Settings > Secrets and variables > Actions`).

### 2. Ejecución de Ejercicios Específicos

Cada ejercicio tiene su propio entorno virtual y `requirements.txt` para gestionar las dependencias de forma aislada.

1.  **Navegar al Directorio del Ejercicio:**
    cd exerciseX_nombre_ejercicio/ # Ej: cd exercise1_replication/

2.  **Crear y Activar Entorno Virtual (si no existe):**
   
    python -m venv venv
    # En Windows:
    .\venv\Scripts\activate
    # En macOS/Linux:
    source venv/bin/activate
 
    Su prompt de terminal debería mostrar `(venv)` al inicio.

3.  **Instalar Dependencias del Ejercicio:**
    (venv) pip install -r requirements.txt

4.  **Ejecutar el Pipeline del Ejercicio:**
    Siga las instrucciones específicas en el `README.md` detallado de cada subdirectorio (`exerciseX_nombre_ejercicio/README.md`). Típicamente, esto implicará ejecutar scripts Python como `python src/nombre_pipeline.py`.

---

**Autor:** Joaquin Ramirez
**Contacto:** [www.linkedin.com/in/joaquin-ramirez-systems-engineer]
**Fecha de Finalización:** 07 de Agosto de 2025