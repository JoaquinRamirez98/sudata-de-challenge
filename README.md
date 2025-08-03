# Desafío Técnico Data Engineer - Sudata

Este repositorio contiene mi solución al Desafío Técnico de Data Engineer propuesto por Sudata, que abarca la construcción de pipelines de replicación, scraping y actualización de datos desde diversas fuentes hacia una base de datos en la nube.

El desafío está dividido en tres ejercicios clave:

1.  **Replicación de una Base de Datos Existente**
2.  **Extracción Incremental desde la API del BCRA**
3.  **Web Scraping de Propiedades en Venta**

---

## Estructura del Repositorio

├── .github/ # Workflows de GitHub Actions para automatización
├── .env # Variables de entorno locales (credenciales, no versionado)
├── README.md # Este archivo: Visión general del desafío
├── exercise1_replication/ # Solución para el Ejercicio 1 (Replicación DB)
│ ├── data/ # Archivos CSV de origen
│ ├── src/ # Scripts Python para el pipeline
│ ├── requirements.txt # Dependencias específicas de este ejercicio
│ └── README.md # Documentación detallada del Ejercicio 1
├── exercise2_bcra_api/ # Solución para el Ejercicio 2 (API BCRA)
│ ├── src/ # Scripts Python para el pipeline
│ ├── requirements.txt # Dependencias específicas de este ejercicio
│ └── README.md # Documentación detallada del Ejercicio 2
└── exercise3_web_scraping/ # Solución para el Ejercicio 3 (Web Scraping)
├── src/ # Scripts Python para el pipeline
├── requirements.txt # Dependencias específicas de este ejercicio
└── README.md # Documentación detallada del Ejercicio 3



---

## Instrucciones de Uso

Cada ejercicio contiene un `README.md` específico con instrucciones detalladas para su configuración, ejecución y verificación.

### Configuración General (Una Sola Vez)

1.  **Clonar el Repositorio:**
    git clone https://github.com/JoaquinRamirez98/sudata-de-challenge.git
    cd sudata-de-challenge

2.  **Crear Archivo `.env`:**
    Crea un archivo `.env` en la raíz de este repositorio y añade todas las credenciales necesarias para los ejercicios. **Este archivo NO se sube a Git.**

### Ejecutar Ejercicios Específicos

Para cada ejercicio, navega a su directorio correspondiente (`exercise1_replication/`, `exercise2_bcra_api/`, etc.) y sigue las instrucciones en su `README.md` particular. Asegúrate de activar el entorno virtual específico del ejercicio antes de ejecutar sus scripts.

---

**Autor:** [Joaquin Ramirez]
**Contacto:** [www.linkedin.com/in/joaquin-ramirez-systems-engineer]
**Fecha:** [02 de Agosto de 2025]