# sudata-de-challenge-kpojoa/exercise3_web_scraping/src/web_scraping_pipeline.py

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager

from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re

from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load environment variables from .env file
current_dir = os.path.dirname(__file__)
project_root = os.path.join(current_dir, '..', '..')
load_dotenv(os.path.join(project_root, '.env'))

# --- Database Credentials ---
DB_CLOUD_HOST = os.getenv("DB_CLOUD_HOST")
DB_CLOUD_PORT = os.getenv("DB_CLOUD_PORT")
DB_CLOUD_NAME = os.getenv("DB_CLOUD_NAME")
DB_CLOUD_USER = os.getenv("DB_CLOUD_USER")
DB_CLOUD_PASSWORD = os.getenv("DB_CLOUD_PASSWORD")

# --- Scraping Configuration ---
BASE_URL = "https://www.zonaprop.com.ar/terrenos-venta-posadas.html"
DELAY_SECONDS = 3  # Delay between page requests to be respectful to the website
MAX_PAGES_TO_SCRAPE = 2  # Number of listing pages to scrape
MIN_RESULTS_REQUIRED = 20 # Minimum number of results to collect

# --- Database Connection ---
def get_cloud_db_engine():
    """Creates and returns an SQLAlchemy engine for the cloud database."""
    db_connection_str = f"postgresql+psycopg2://{DB_CLOUD_USER}:{DB_CLOUD_PASSWORD}@{DB_CLOUD_HOST}:{DB_CLOUD_PORT}/{DB_CLOUD_NAME}"
    return create_engine(db_connection_str)

# --- DDL Definition for 'propiedades_posadas' table ---
PROPIEDADES_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS propiedades_posadas (
    id_anuncio VARCHAR(255) PRIMARY KEY,
    titulo TEXT,
    ubicacion TEXT,
    precio_moneda VARCHAR(10),
    precio_valor NUMERIC(15, 2),
    metros_cuadrados_terreno NUMERIC(10, 2),
    frente_terreno_mts NUMERIC(10, 2),
    largo_terreno_mts NUMERIC(10, 2),
    url_anuncio TEXT UNIQUE,
    latitud NUMERIC(10, 7),
    longitud NUMERIC(10, 7),
    descripcion TEXT,
    fecha_scrapeo DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
"""

def create_propiedades_table(engine):
    """Creates the 'propiedades_posadas' table in the destination database if it doesn't exist."""
    print("Creando/Verificando la tabla 'propiedades_posadas' en la base de datos en la nube...")
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text(PROPIEDADES_TABLE_DDL))
    print("Tabla 'propiedades_posadas' creada/verificada exitosamente.")

# --- parse_ad_data: Extracts data from the listing page HTML ---
def parse_ad_data(ad_element):
    """
    Extracts data fields from an individual ad element on the listing page.
    Assumes all required data is available within the listing card.
    """
    data = {
        'id_anuncio': None, 'titulo': None, 'ubicacion': None,
        'precio_moneda': None, 'precio_valor': None, 'metros_cuadrados_terreno': None,
        'frente_terreno_mts': None, 'largo_terreno_mts': None,   
        'url_anuncio': None, 'latitud': None, 'longitud': None,
        'descripcion': None, 'fecha_scrapeo': datetime.now().date()
    }

    desc_link_h3 = ad_element.find('h3', class_='postingCard-module__posting-description')
    link_element = desc_link_h3.find('a', href=True) if desc_link_h3 else None

    raw_description_text = ""
    if link_element:
        data['url_anuncio'] = "https://www.zonaprop.com.ar" + link_element['href']
        raw_description_text = link_element.get_text(strip=True)
    elif desc_link_h3:
        raw_description_text = desc_link_h3.get_text(strip=True)

    data['descripcion'] = raw_description_text
    
    if data['url_anuncio']:
        id_match = re.search(r'-id-(\d+)\.html', data['url_anuncio'])
        data['id_anuncio'] = id_match.group(1) if id_match else str(hash(data['url_anuncio']))
    else:
        data['id_anuncio'] = str(hash(raw_description_text)) if raw_description_text else str(hash(time.time()))

    if raw_description_text:
        first_sentence_match = re.match(r'(.+?)[\.\-–]', raw_description_text)
        data['titulo'] = first_sentence_match.group(1).strip() if first_sentence_match else raw_description_text[:100].strip()
    else:
        data['titulo'] = "Título no disponible"

    address_element = ad_element.find('div', class_='postingLocations-module__location-address')
    city_element = ad_element.find('h2', class_='postingLocations-module__location-text')
    
    full_location = []
    if address_element:
        full_location.append(address_element.get_text(strip=True))
    if city_element:
        full_location.append(city_element.get_text(strip=True))
    data['ubicacion'] = ', '.join(full_location) if full_location else None

    price_match_in_desc = re.search(r'precio[:\s]*([a-zA-Z$]+)?\s*([\d\.,]+)', raw_description_text, re.IGNORECASE)
    if price_match_in_desc:
        currency_raw = price_match_in_desc.group(1)
        value_str = price_match_in_desc.group(2)
        value_str = value_str.replace('.', '').replace(',', '.')
        data['precio_moneda'] = (currency_raw.upper().replace('$', 'USD') if currency_raw else "USD")
        try: data['precio_valor'] = float(value_str)
        except ValueError: data['precio_valor'] = None
    else:
        price_div = ad_element.find('div', class_='postingPrices-module__price')
        if price_div:
            price_text_div = price_div.get_text(strip=True).replace('.', '').replace(',', '.')
            price_match_div = re.match(r'([A-Z$]+)?\s*([\d.]+)', price_text_div)
            if price_match_div:
                currency_div = price_match_div.group(1)
                value_str_div = price_match_div.group(2)
                data['precio_moneda'] = (currency_div.upper().replace('$', 'USD') if currency_div else "USD")
                try: data['precio_valor'] = float(value_str_div)
                except ValueError: data['precio_valor'] = None
            elif "consultar precio" in price_text_div.lower():
                data['precio_moneda'] = "Consultar"
                data['precio_valor'] = None
        
    # Metros Cuadrados de Terreno, Frente, y Largo from listing card
    m2_element = ad_element.find('span', class_='postingMainFeatures-module__posting-main-features-span', string=re.compile(r'\d+\s*m[2²]\s*tot\.'))
    if m2_element:
        m2_match = re.search(r'(\d+)', m2_element.get_text(strip=True))
        data['metros_cuadrados_terreno'] = float(m2_match.group(1)) if m2_match else None

    general_features_spans = ad_element.find_all('span', class_=re.compile(r'generalFeaturesProperty-module__description-text'))
    
    for span in general_features_spans:
        text_content = span.get_text(strip=True)
        frente_match = re.search(r'Frente del terreno \(mts\) : ([\d\.,]+)', text_content)
        largo_match = re.search(r'Largo del terreno \(mts\) : ([\d\.,]+)', text_content)

        if frente_match:
            try: data['frente_terreno_mts'] = float(frente_match.group(1).replace(',', '.'))
            except ValueError: pass
        if largo_match:
            try: data['largo_terreno_mts'] = float(largo_match.group(1).replace(',', '.'))
            except ValueError: pass
    
    if data['metros_cuadrados_terreno'] is None and \
       data['frente_terreno_mts'] is not None and \
       data['largo_terreno_mts'] is not None:
        data['metros_cuadrados_terreno'] = data['frente_terreno_mts'] * data['largo_terreno_mts']
    
    if data['metros_cuadrados_terreno'] is None:
        m2_desc_match = re.search(r'superficie:\s*([\d\.,]+)\s*m[2²]', raw_description_text, re.IGNORECASE)
        if m2_desc_match:
            m2_value_str = m2_desc_match.group(1).replace('.', '').replace(',', '.')
            try: data['metros_cuadrados_terreno'] = float(m2_value_str)
            except ValueError: data['metros_cuadrados_terreno'] = None
    
    return data


# --- scrape_zonaprop: Orchestrates the scraping process with Selenium ---
def scrape_zonaprop():
    """
    Performs web scraping of property listings from Zonaprop.com.ar using Selenium with Google Chrome.
    It navigates through listing pages and extracts data.
    """
    all_ads_data = []
    total_scraped_count = 0
    page_num = 1
    driver = None

    print(f"Iniciando scraping de Zonaprop para terrenos en Posadas desde: {BASE_URL} (usando Selenium con Chrome)")

    try:
        # Configure Chrome options (e.g., headless mode for server environments)
        chrome_options = ChromeOptions()
        # chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Initialize the WebDriver for Chrome
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30) # Set page load timeout

        while total_scraped_count < MIN_RESULTS_REQUIRED and page_num <= MAX_PAGES_TO_SCRAPE:
            url = f"{BASE_URL.replace('.html', '')}-pagina-{page_num}.html" if page_num > 1 else BASE_URL
            print(f"  Navegando a página {page_num}: {url}")

            try:
                driver.get(url)
                time.sleep(DELAY_SECONDS) # Give time for dynamic content to load

                soup = BeautifulSoup(driver.page_source, 'html.parser')

                # Identify individual ad elements
                ad_elements = soup.find_all('div', class_='postingCard-module__posting-container')
                
                if not ad_elements:
                    print(f"  No se encontraron anuncios en la página {page_num}. Terminando scraping.")
                    break

                ads_on_page_count = 0
                for ad_element in ad_elements:
                    try:
                        parsed_data = parse_ad_data(ad_element)
                        if parsed_data.get('id_anuncio') and parsed_data.get('url_anuncio'):
                            all_ads_data.append(parsed_data)
                            ads_on_page_count += 1
                            total_scraped_count += 1
                            if total_scraped_count >= MIN_RESULTS_REQUIRED:
                                print(f"  Alcanzado el mínimo de {MIN_RESULTS_REQUIRED} resultados.")
                                break
                    except Exception as e:
                        print(f"    Error al parsear un anuncio en página {page_num}: {e}")
                        continue

                print(f"  Scrapeados {ads_on_page_count} anuncios en página {page_num}. Total: {total_scraped_count}")

                page_num += 1

            except Exception as e:
                print(f"  Error durante la navegación o carga de página {url}: {e}")
                break

    except Exception as e:
        print(f"\nERROR al iniciar o durante el proceso de Selenium: {e}")
    finally:
        if driver:
            driver.quit()
            print("Navegador Selenium cerrado.")
    
    print(f"\nScraping finalizado. Total de anuncios recolectados: {total_scraped_count}")
    return pd.DataFrame(all_ads_data)

# --- run_web_scraping_pipeline: Main function of the scraping pipeline ---
def run_web_scraping_pipeline():
    """
    Main function for the web scraping pipeline.
    """
    cloud_engine = None
    try:
        cloud_engine = get_cloud_db_engine()
        create_propiedades_table(cloud_engine)

        df_propiedades = scrape_zonaprop()

        if not df_propiedades.empty:
            print(f"Cargando {len(df_propiedades)} propiedades en la base de datos en la nube...")
            df_propiedades.drop_duplicates(subset=['id_anuncio'], inplace=True)
            print(f"Cargando {len(df_propiedades)} propiedades únicas...")
            
            with cloud_engine.connect() as connection:
                df_propiedades.to_sql('propiedades_posadas', connection, if_exists='append', index=False, schema='public')
            
            print("Propiedades cargadas exitosamente.")
        else:
            print("No se encontraron propiedades para cargar.")

    except Exception as e:
        print(f"\nERROR en el pipeline de web scraping: {e}")
    finally:
        if cloud_engine:
            cloud_engine.dispose()
        print("Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    run_web_scraping_pipeline()
    print("Proceso de Web Scraping finalizado.")