# scraper/main.py
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import redis
from config import Config
from database.connectors import SQLServerConnector
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Configuración de Redis
r = redis.Redis.from_url(Config.REDIS_URL)

class RegistradoresScraper:
    def __init__(self):
        self.base_url = "https://opendata.registradores.org/directorio"
        self.driver = self._init_driver()
        self.connector = SQLServerConnector()
        
    def _init_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  # Ejecución en background
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        # Especificar ruta explícita si es necesario
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)
    
    def _change_page_size(self):
        try:
            select = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.NAME, "_org_registradores_opendata_portlet_BuscadorSociedadesPortlet_dataTable_length"))
            )
            select.send_keys("100")
            time.sleep(2)  # Esperar recarga
        except TimeoutException:
            print("No se pudo cambiar el tamaño de página")
            
    def _extract_page_data(self):
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        table = soup.find('table', {'id': '_org_registradores_opendata_portlet_BuscadorSociedadesPortlet_dataTable'})
    
        if not table or not table.tbody:
            print("Estructura de tabla inválida")
            return []
        
        # Mejorar la espera para la tabla
        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, '_org_registradores_opendata_portlet_BuscadorSociedadesPortlet_dataTable'))
            )
        except TimeoutException:
            print("Tabla no encontrada")
            return []
        
        table = soup.find('table', {'id': '_org_registradores_opendata_portlet_BuscadorSociedadesPortlet_dataTable'})
        
        if not table or not table.tbody:
            return []
    
        table = soup.find('table', {'id': '_org_registradores_opendata_portlet_BuscadorSociedadesPortlet_dataTable'})

        empresas = []
        for row in table.tbody.find_all('tr'):
            cols = row.find_all('td')
            if len(cols) >= 3:
                nombre = cols[0].text.strip()
                provincia = cols[1].text.strip()
                detalle_url = cols[0].find('a')['href']
                
                empresas.append({
                    'nombre': nombre,
                    'provincia': provincia,
                    'detalle_url': detalle_url,
                    'processed': False
                })
        return empresas
    
    def _handle_pagination(self):
        while True:
            # Extraer datos de la página actual
            empresas = self._extract_page_data()
            self._store_data(empresas)
            
            # Intentar pasar a siguiente página
            try:
                next_btn = self.driver.find_element(By.ID, '_org_registradores_opendata_portlet_BuscadorSociedadesPortlet_dataTable_next')
                if 'disabled' in next_btn.get_attribute('class'):
                    break
                    
                next_btn.click()
                WebDriverWait(self.driver, 10).until(
                    EC.staleness_of(self.driver.find_element(By.TAG_NAME, 'table'))
                )
                time.sleep(1)
            except Exception as e:
                print(f"Error paginación: {str(e)}")
                break
                
    def _store_data(self, empresas):
        # Almacenar en Redis y SQL
        with self.connector.get_connection() as conn:
            for empresa in empresas:
                # Guardar en SQL
                conn.execute(
                    "INSERT INTO empresas (razon_social, provincia) VALUES (?, ?)",
                    (empresa['nombre'], empresa['provincia'])
                )
                # Añadir detalles a Redis para procesamiento posterior
                r.hset(f"empresa:{empresa['nombre']}", mapping=empresa)
                r.lpush('scraping:pending', empresa['detalle_url'])
                
    def run(self):
        try:
            self.driver.get(self.base_url)
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, 'body'))
            )
            
            # Cambiar a 100 resultados por página
            self._change_page_size()
            
            # Procesar paginación
            self._handle_pagination()
            
        except Exception as e:
            print(f"Error crítico: {str(e)}")
            self.driver.save_screenshot('error_screenshot.png')  # Debug visual
            raise e
        finally:
            self.driver.quit()

# Uso desde Streamlit
def iniciar_scraping():
    scraper = RegistradoresScraper()
    scraper.run()