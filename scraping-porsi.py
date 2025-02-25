"""
Scraper Empresite - Versión Optimizada
Autor: [Jhon Limones]
Fecha: 24/02/2025
Descripción: Script para extraer información de empresas desde empresite.eleconomista.es
"""

# Importación de librerías estándar
import csv
import time
import logging
import random
import re
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from pathlib import Path
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, urljoin

# Importación de librerías de terceros
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException,
    ElementClickInterceptedException
)
from bs4 import BeautifulSoup
from tqdm import tqdm


# Configuración de logging
def configurar_logging():
    """Configura el sistema de logging"""
    Path('logs').mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = configurar_logging()

def guardar_resultados_ordenados(datos, ruta_salida="debug_resultado.csv", csv_original="csv_1.csv"):
    """
    Guarda los resultados del scraping en CSV con el orden correcto de columnas,
    evitando duplicar RAZON_SOCIAL y manteniendo el orden original del CSV.
    
    Args:
        datos (dict): Datos completos de la empresa (originales + scrapeados)
        ruta_salida (str): Ruta del archivo donde guardar los resultados
        csv_original (str): Ruta al archivo CSV original
    """
    import pandas as pd
    
    # Definir el orden de columnas originales del CSV
    columnas_originales = [
        'COD_INFOTEL', 'NIF', 'RAZON_SOCIAL', 'DOMICILIO', 
        'COD_POSTAL', 'NOM_POBLACION', 'NOM_PROVINCIA', 'URL'
    ]
    
    # Definir columnas escrapeadas (sin RAZON_SOCIAL para evitar duplicados)
    columnas_scrapeadas = [
        'FORMA_JURIDICA', 'SECTOR', 'FECHA_CONSTITUCION', 'FECHA_ULTIMO_CAMBIO',
        'OBJETO_SOCIAL', 'ACTIVIDAD', 'ACTIVIDAD_CNAE', 'ESTADO_EMPRESA',
        'COORDENADAS', 'TELEFONO_1', 'TELEFONO_2', 'TELEFONO_3', 'EMAIL',
        'EVOLUCION_VENTAS', 'OPERACIONES_INTERNACIONALES', 'GRUPO_SECTOR',
        'CARGOS', 'NUMERO_EMPLEADOS', 'COTIZA_BOLSA'
    ]
    
    # Intentar recuperar los datos originales del CSV
    try:
        df_original = pd.read_csv(csv_original)
        # Buscar la empresa por RAZON_SOCIAL
        empresa_original = df_original[df_original['RAZON_SOCIAL'] == datos['RAZON_SOCIAL']]
        
        if not empresa_original.empty:
            # Tomar la primera coincidencia
            datos_originales = empresa_original.iloc[0].to_dict()
            
            # Combinar datos originales con datos escrapeados
            datos_combinados = datos_originales.copy()
            for clave, valor in datos.items():
                if clave != 'RAZON_SOCIAL' or clave not in datos_combinados:
                    datos_combinados[clave] = valor
                    
            # Usar datos combinados
            datos_finales = datos_combinados
        else:
            datos_finales = datos
    except Exception as e:
        logger.warning(f"No se pudo recuperar datos del CSV original: {str(e)}")
        datos_finales = datos
    
    # Combinar todas las columnas en el orden deseado
    todas_columnas = columnas_originales.copy()
    for col in columnas_scrapeadas:
        if col not in todas_columnas:
            todas_columnas.append(col)
    
    # Crear DataFrame y reordenar columnas
    df = pd.DataFrame([datos_finales])
    
    # Filtrar solo las columnas que existen en los datos
    columnas_disponibles = [col for col in todas_columnas if col in df.columns]
    df = df.reindex(columns=columnas_disponibles, fill_value="")
    
    # Guardar en CSV
    df.to_csv(ruta_salida, index=False)
    logger.info(f"Resultados guardados en: {ruta_salida}")
    
    return df

# Constantes
COLUMNAS_SALIDA = [
    'COD_INFOTEL', 'NIF', 'RAZON_SOCIAL', 'DOMICILIO', 'COD_POSTAL',
    'NOM_POBLACION', 'NOM_PROVINCIA', 'URL', 'FORMA_JURIDICA', 'SECTOR',
    'FECHA_CONSTITUCION', 'FECHA_ULTIMO_CAMBIO', 'OBJETO_SOCIAL', 'ACTIVIDAD',
    'ACTIVIDAD_CNAE', 'ESTADO_EMPRESA', 'COORDENADAS', 'TELEFONO_1', 'TELEFONO_2',
    'TELEFONO_3', 'EMAIL', 'FACEBOOK', 'TWITTER', 'LINKEDIN', 'INSTAGRAM',
    'EVOLUCION_VENTAS', 'OPERACIONES_INTERNACIONALES', 'GRUPO_SECTOR', 'CARGOS',
    'NUMERO_EMPLEADOS', 'COTIZA_BOLSA'
]

@dataclass
class ConfiguracionScraper:
    """Configuración del scraper"""
    url_base: str = "https://empresite.eleconomista.es/"
    tiempo_espera: int = 20  # Aumentado para mayor estabilidad
    reintentos_maximos: int = 5  # Aumentado número de reintentos
    delay_entre_peticiones: Tuple[float, float] = (2.0, 4.0)  # Más tiempo entre peticiones
    procesos: int = min(8, mp.cpu_count())  # Limitado a 8 procesos
    timeout_peticion: int = 45  # Aumentado timeout
    archivo_entrada: str = "csv_1.csv"
    archivo_salida: str = "csv_1_actualizado.csv"
    modo_headless: bool = False  # Modo visible para debugging
    chunk_size: int = 50  # Reducido para mejor control
    max_retries: int = 5  # Aumentado número de reintentos

class GestorNavegador:
    """Gestiona las instancias del navegador Chrome"""
    
    # Lista predefinida de User-Agents modernos
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/121.0.0.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15'
    ]
    
    def __init__(self, config: ConfiguracionScraper):
        self.config = config

    def crear_opciones(self) -> webdriver.ChromeOptions:
        """Configura las opciones del navegador"""
        user_agent = random.choice(self.USER_AGENTS)
        opciones = webdriver.ChromeOptions()
        
        # Configuración básica
        if self.config.modo_headless:
            opciones.add_argument("--headless=new")
        
        # Configuraciones esenciales
        opciones.add_argument("--no-sandbox")
        opciones.add_argument("--disable-dev-shm-usage")
        opciones.add_argument(f"user-agent={user_agent}")
        opciones.add_argument("--disable-blink-features=AutomationControlled")
        opciones.add_experimental_option("excludeSwitches", ["enable-automation"])
        opciones.add_experimental_option('useAutomationExtension', False)
        
        # Optimizaciones adicionales
        opciones.add_argument("--disable-extensions")
        opciones.add_argument("--disable-notifications")
        opciones.add_argument("--disable-infobars")
        opciones.add_argument("--disable-gpu")
        opciones.add_argument("--disable-popup-blocking")
        opciones.add_argument("--start-maximized")
        
        # Configuraciones de rendimiento
        opciones.add_argument("--disable-cache")
        opciones.add_argument("--disable-application-cache")
        opciones.add_argument("--disable-offline-load-stale-cache")
        
        return opciones

    def inicializar_navegador(self) -> webdriver.Chrome:
        """Crea y retorna una nueva instancia del navegador"""
        service = webdriver.ChromeService()
        navegador = webdriver.Chrome(service=service, options=self.crear_opciones())
        navegador.set_page_load_timeout(self.config.timeout_peticion)
        return navegador
    

class ControladorPopups:
    """Maneja los popups y elementos interstitiales"""
    
    @staticmethod
    def cerrar_popup_cookies(navegador: webdriver.Chrome) -> bool:
        """Intenta cerrar el popup de cookies de Didomi"""
        try:
            # Esperar el popup
            WebDriverWait(navegador, 5).until(
                EC.presence_of_element_located((By.ID, "didomi-notice-agree-button"))
            )
            
            # Intentar cerrar usando JavaScript
            script = """
                var boton = document.querySelector("#didomi-notice-agree-button");
                if (boton) boton.click();
            """
            navegador.execute_script(script)
            time.sleep(1)
            return True
        except Exception as e:
            logger.debug(f"No se encontró popup de cookies o no se pudo cerrar: {str(e)}")
            return False

    @staticmethod
    def esperar_y_cerrar_modales(navegador: webdriver.Chrome):
        """Espera y cierra cualquier modal que pueda aparecer"""
        try:
            # Lista de selectores comunes de modales
            selectores_modales = [
                "button.modal-close",
                ".modal-cerrar",
                ".close-modal",
                ".cerrar-popup"
            ]
            
            for selector in selectores_modales:
                try:
                    elemento = WebDriverWait(navegador, 2).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    elemento.click()
                    time.sleep(0.5)
                except:
                    continue
        except Exception as e:
            logger.debug(f"Error manejando modales: {str(e)}")

class ExtractorDatos:
    """Clase para extraer datos específicos de la página"""
    
    @staticmethod
    def extraer_texto_seguro(elemento: Any, selector: str, clase: str = "") -> str:
        """Extrae texto de forma segura de un elemento BeautifulSoup"""
        try:
            if clase:
                resultado = elemento.find(selector, class_=clase)
            else:
                resultado = elemento.find(selector)
            return resultado.get_text(strip=True) if resultado else ""
        except Exception:
            return ""

    @staticmethod
    def extraer_coordenadas(soup: BeautifulSoup) -> str:
        """Extrae las coordenadas geográficas"""
        try:
            geo_div = soup.find('div', class_='geo')
            if geo_div:
                lat = geo_div.find('span', class_='latitude').find('span')['title']
                lon = geo_div.find('span', class_='longitude').find('span')['title']
                return f"{lat},{lon}"
        except Exception as e:
            logger.warning(f"Error al extraer coordenadas: {str(e)}")
        return ""

    @staticmethod
    def extraer_telefonos(soup: BeautifulSoup, navegador: webdriver.Chrome = None) -> List[str]:
        """
        Extrae los números de teléfono de la página y los limpia correctamente.
        Si se proporciona un navegador, intenta expandir las secciones con "Ver más" primero.
    
        Args:
            soup: Objeto BeautifulSoup con el HTML
            navegador: Instancia activa del navegador para interactuar con "Ver más" (opcional)
    
        Returns:
            Lista de hasta 3 números de teléfono únicos (con 'N/A' si no hay suficientes)
        """
        telefonos = []
        try:
            # Si tenemos navegador, expandir sección "Ver más" de teléfonos
            if navegador:
                try:
                    # Encontrar botón "Ver más" en la sección de teléfonos
                    xpath_boton = "//div[contains(@class, 'seeMoreDir')]//p[contains(@class, 'cursor-pointer')]"
                    botones = navegador.find_elements(By.XPATH, xpath_boton)
                
                    if botones:
                        print("[DEBUG] Encontrado botón 'Ver más' para teléfonos")
                        for boton in botones:
                            try:
                                # Hacer scroll y clic
                                navegador.execute_script("arguments[0].scrollIntoView(true);", boton)
                                time.sleep(0.5)
                                navegador.execute_script("arguments[0].click();", boton)
                                print("[DEBUG] Clic en 'Ver más' exitoso")
                                time.sleep(1)  # Esperar a que se expanda
                            except Exception as e:
                                print(f"[DEBUG] Error haciendo clic en 'Ver más': {str(e)}")
                
                    # Actualizar el soup con el nuevo HTML expandido
                    soup = BeautifulSoup(navegador.page_source, 'html.parser')
                except Exception as e:
                    print(f"[DEBUG] Error expandiendo sección: {str(e)}")
        
            # Extracción con JavaScript si tenemos navegador
            if navegador:
                try:
                    script = """
                        // Recoger todos los enlaces y spans que pueden contener teléfonos
                        let telefonos = [];
                    
                        // Buscar enlaces tel: o rel:
                        document.querySelectorAll('a[href^="tel:"], a[href^="rel:"]').forEach(link => {
                            let num = link.href.replace('tel:', '').replace('rel:', '');
                            if (/^\\d{9}$/.test(num)) telefonos.push(num);
                        });
                    
                        // Buscar en spans de valor oculto
                        document.querySelectorAll('span.text-bodytext-m').forEach(span => {
                            let texto = span.textContent.trim();
                            if (/^\\d{9}$/.test(texto)) telefonos.push(texto);
                        });
                    
                        // Buscar en el div de teléfonos completo
                        const divTel = document.querySelector('div.seeMoreDir');
                        if (divTel) {
                            const texto = divTel.textContent;
                            const regex = /\\b\\d{9}\\b/g;
                            let match;
                            while (match = regex.exec(texto)) {
                                telefonos.push(match[0]);
                            }
                        }
                    
                        // Buscar en la descripción
                        const divDesc = document.querySelector('#myModuleContext');
                        if (divDesc) {
                            const texto = divDesc.textContent;
                            const regex = /\\b\\d{9}\\b/g;
                            let match;
                            while (match = regex.exec(texto)) {
                                telefonos.push(match[0]);
                            }
                        }
                    
                        // Eliminar duplicados
                        return [...new Set(telefonos)];
                    """
                
                    numeros_js = navegador.execute_script(script)
                    if numeros_js and len(numeros_js) > 0:
                        telefonos = numeros_js
                        print(f"[DEBUG] Teléfonos encontrados con JavaScript: {telefonos}")
                except Exception as e:
                    print(f"[DEBUG] Error extrayendo teléfonos con JavaScript: {str(e)}")
        
            # Si no se encontraron teléfonos con JavaScript o no hay navegador, usar BeautifulSoup
            if not telefonos:
                # 1. Buscar en enlaces de teléfono
                links = soup.find_all('a', href=lambda h: h and (h.startswith('tel:') or h.startswith('rel:')))
                for link in links:
                    href = link['href'].replace('tel:', '').replace('rel:', '')
                    if re.match(r'^\d{9}$', href):
                        telefonos.append(href)
            
                # 2. Buscar en spans de texto oculto
                spans = soup.find_all('span', class_=lambda c: c and 'text-bodytext-m' in c)
                for span in spans:
                    texto = span.get_text(strip=True)
                    if re.match(r'^\d{9}$', texto):
                        telefonos.append(texto)
            
                # 3. Buscar en el div de teléfonos completo
                div_tel = soup.find('div', class_='seeMoreDir')
                if div_tel:
                    texto = div_tel.get_text()
                    numeros = re.findall(r'\b\d{9}\b', texto)
                    telefonos.extend(numeros)
        
            # Eliminar duplicados y limitarse a 3
            telefonos = list(dict.fromkeys(telefonos))[:3]
            print(f"[DEBUG] Teléfonos únicos encontrados: {telefonos}")
        
            # Rellenar con 'N/A' si hay menos de 3
            while len(telefonos) < 3:
                telefonos.append('N/A')
        
            return telefonos
        except Exception as e:
            print(f"[ERROR] Error al extraer teléfonos: {str(e)}")
            return ['N/A', 'N/A', 'N/A']

    @staticmethod
    def extraer_redes_sociales(url: str) -> Dict[str, str]:
        """Extrae enlaces de redes sociales de la web oficial de la empresa."""
        redes_sociales = {
            'FACEBOOK': 'N/A',
            'TWITTER': 'N/A',
            'LINKEDIN': 'N/A',
            'INSTAGRAM': 'N/A'
        }

        try:
            headers = {'User-Agent': random.choice(GestorNavegador.USER_AGENTS)}
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                logger.warning(f"Error al acceder a la web oficial: {url}")
                return redes_sociales

            soup = BeautifulSoup(response.text, 'html.parser')

            # Patrones de URLs de redes sociales
            patrones = {
                'FACEBOOK': [r'facebook\.com/\w+', r'fb\.com/\w+'],
                'TWITTER': [r'twitter\.com/\w+', r'x\.com/\w+'],
                'LINKEDIN': [r'linkedin\.com/company/\w+', r'linkedin\.com/in/\w+'],
                'INSTAGRAM': [r'instagram\.com/\w+']
            }

            for enlace in soup.find_all('a', href=True):
                href = enlace['href'].lower()
                for red, pattern in patrones.items():
                    if re.search(pattern, href):
                        redes_sociales[red] = href
                        break

        except Exception as e:
            logger.warning(f"Error al extraer redes sociales: {str(e)}")

        return redes_sociales


class ProcesadorEmpresas:
    """Clase principal para procesar empresas"""
    
    def __init__(self, config: ConfiguracionScraper):
        self.config = config
        self.gestor_navegador = GestorNavegador(config)
        self.extractor = ExtractorDatos()
        self.controlador_popups = ControladorPopups()

    import re  # Importamos la librería para limpieza de texto

    def normalizar_nombre(self, nombre: str) -> str:
        """
        Convierte un nombre de empresa a minúsculas y elimina caracteres especiales,
        espacios extra y signos de puntuación para asegurar una comparación uniforme.

        Parámetros:
            nombre (str): Nombre de la empresa a normalizar.

        Retorna:
            str: Nombre normalizado en minúsculas y sin caracteres especiales.
        """
        if not nombre:
            return ""

        # Convertir todo a minúsculas
        nombre = nombre.lower()

        # Reemplazar caracteres especiales comunes
        nombre = nombre.replace("á", "a").replace("é", "e").replace("í", "i") \
                    .replace("ó", "o").replace("ú", "u").replace("ñ", "n")

        # Eliminar cualquier carácter que no sea letra, número o espacio
        nombre = re.sub(r'[^a-z0-9 ]', '', nombre)

        # Eliminar espacios en blanco extra
        nombre = " ".join(nombre.split())

        return nombre


    def procesar_empresa(self, empresa: Dict) -> Dict:
        """Procesa una empresa individual en Empresite."""

        navegador = None
        datos_actualizados = empresa.copy()
        service = webdriver.ChromeService()

        for intento in range(self.config.max_retries):
            try:
                # Inicializar navegador con servicio
                navegador = webdriver.Chrome(service=service, options=self.gestor_navegador.crear_opciones())
                navegador.set_page_load_timeout(self.config.timeout_peticion)

                print(f"\n[DEBUG] Intento {intento + 1}: Procesando empresa {empresa['RAZON_SOCIAL']}")

                # Cargar página inicial
                navegador.get(self.config.url_base)
                print("[DEBUG] Página inicial cargada.")
                time.sleep(2)

                # Manejo de cookies y popups
                self.controlador_popups.cerrar_popup_cookies(navegador)
                self.controlador_popups.esperar_y_cerrar_modales(navegador)

                # Realizar búsqueda usando JavaScript
                print(f"[DEBUG] Buscando empresa: {empresa['RAZON_SOCIAL']}")
                script_busqueda = f"""
                    document.getElementById('default-search').value = '{empresa["RAZON_SOCIAL"]}';
                    document.getElementById('boton-buscador').click();
                """
                navegador.execute_script(script_busqueda)
                time.sleep(3)  # Esperar a que se carguen los resultados

                # Esperar a que los resultados de búsqueda se carguen
                try:
                    WebDriverWait(navegador, self.config.tiempo_espera).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "cardCompanyBox"))
                    )
                    print("[DEBUG] Resultados de búsqueda cargados.")
                except TimeoutException:
                    print("[ERROR] No se cargaron los resultados de búsqueda a tiempo.")
                    continue  # Reintentar en el siguiente intento

                # Buscar todas las empresas en los resultados
                empresas_encontradas = navegador.find_elements(By.CLASS_NAME, "cardCompanyBox")

                print(f"[DEBUG] Empresas encontradas: {len(empresas_encontradas)}")

                
                empresa_encontrada = None
                for index, empresa_div in enumerate(empresas_encontradas):
                    try:
                        nombre_empresa = empresa_div.find_element(By.TAG_NAME, "h3").text.strip()
                        print(f"[DEBUG] Empresa {index + 1}: {nombre_empresa}")

                        # Normalizar nombres antes de comparar
                        nombre_empresa_normalizado = self.normalizar_nombre(nombre_empresa)
                        nombre_buscado_normalizado = self.normalizar_nombre(empresa["RAZON_SOCIAL"])

                        if nombre_buscado_normalizado in nombre_empresa_normalizado:
                            empresa_encontrada = empresa_div
                            print(f"[DEBUG] ✅ Coincidencia encontrada en la empresa {index + 1}: {nombre_empresa}")
                            break  # Se encontró la empresa, salir del bucle

                    except Exception as e:
                        print(f"[WARNING] No se pudo leer el nombre de la empresa {index + 1}: {str(e)}")

                # Si no se encontró la empresa, reintentar
                if not empresa_encontrada:
                    print("[ERROR] No se encontró la empresa en los resultados de búsqueda. Reintentando...")
                    continue


                # Hacer clic en el enlace "Ver Ficha"
                try:
                    enlace_ficha = empresa_encontrada.find_element(By.XPATH, ".//a[@title='Ver Ficha']")
                    navegador.execute_script("arguments[0].click();", enlace_ficha)
                    print("[DEBUG] Se hizo clic en 'Ver Ficha'. Cargando página de detalles...")
                except Exception as e:
                    print(f"[ERROR] No se pudo hacer clic en 'Ver Ficha': {str(e)}")
                    continue  # Reintentar en el siguiente intento

                # Esperar a que cargue la página de detalles
                try:
                    WebDriverWait(navegador, self.config.tiempo_espera).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "text-neutrals-700"))
                    )
                    print("[DEBUG] Página de detalles cargada completamente.")
                except TimeoutException:
                    print("[ERROR] La página de detalles no cargó a tiempo. Reintentando...")
                    continue  # Reintentar en el siguiente intento

                # Procesar el contenido de la página con BeautifulSoup
                soup = BeautifulSoup(navegador.page_source, 'html.parser')

                # Extraer datos básicos
                try:
                    self._extraer_datos_basicos(soup, datos_actualizados)
                    print("[DEBUG] Datos básicos extraídos con éxito.")
                except Exception as e:
                    print(f"[ERROR] Fallo al extraer datos básicos: {str(e)}")

                # Extraer datos de contacto
                try:
                    self._extraer_datos_contacto(soup, datos_actualizados, navegador)
                    print("[DEBUG] Datos de contacto extraídos con éxito.")
                except Exception as e:
                    print(f"[ERROR] Fallo al extraer datos de contacto: {str(e)}")

                # Extraer datos comerciales
                try:
                    self._extraer_datos_comerciales(soup, datos_actualizados)
                    print("[DEBUG] Datos comerciales extraídos con éxito.")
                except Exception as e:
                    print(f"[ERROR] Fallo al extraer datos comerciales: {str(e)}")

                print("\n✅ [DEBUG] Scraping finalizado exitosamente.")
                break  # Si todo ha funcionado, salir del loop de reintentos

            except ElementClickInterceptedException:
                print(f"[WARNING] Click interceptado en intento {intento + 1}, reintentando...")
                time.sleep(random.uniform(2, 4))

            except Exception as e:
                print(f"[ERROR] Intento {intento + 1} fallido para {empresa['RAZON_SOCIAL']}: {str(e)}")
                if intento == self.config.max_retries - 1:
                    print(f"[ERROR] Todos los intentos fallaron para {empresa['RAZON_SOCIAL']}")
                time.sleep(random.uniform(3, 6))

            finally:
                if navegador:
                    try:
                        print("[DEBUG] Cerrando navegador después de completar el scraping.")
                        navegador.quit()
                    except Exception as e:
                        print(f"[ERROR] No se pudo cerrar el navegador: {str(e)}")

            # Asegurar que todas las columnas del CSV original + columnas del scraping estén en datos_actualizados
            for columna in COLUMNAS_SALIDA:
                if columna not in datos_actualizados:
                    datos_actualizados[columna] = ""  # Rellenar con vacío si no se encontró el dato

            # Imprimir para depuración si faltan columnas
            print("[DEBUG] Columnas incluidas en datos_actualizados:", list(datos_actualizados.keys()))


        return datos_actualizados


    def _extraer_datos_basicos(self, soup: BeautifulSoup, datos: Dict):
        """Extrae los datos básicos de la empresa"""
        try:
            for seccion in soup.find_all('div', class_='flex flex-col gap-2'):
                titulo = seccion.find('h3')
                if not titulo:
                    continue
                    
                titulo_texto = titulo.get_text(strip=True)
                valor = seccion.find('span', class_='text-neutrals-700')
                
                if valor:
                    valor_texto = valor.get_text(strip=True)
                    
                    # Mapeo de campos mejorado
                    mapeo_campos = {
                        'Forma jurídica': 'FORMA_JURIDICA',
                        'Sector': 'SECTOR',
                        'Fecha de constitución': 'FECHA_CONSTITUCION',
                        'Fecha último cambio': 'FECHA_ULTIMO_CAMBIO',
                        'Objeto social': 'OBJETO_SOCIAL',
                        'Actividad': 'ACTIVIDAD',
                        'Actividad CNAE': 'ACTIVIDAD_CNAE',
                        'Estado de la empresa': 'ESTADO_EMPRESA'
                    }
                    
                    if titulo_texto in mapeo_campos:
                        datos[mapeo_campos[titulo_texto]] = valor_texto
                        
        except Exception as e:
            logger.error(f"Error al extraer datos básicos: {str(e)}")

    def _extraer_datos_contacto(self, soup: BeautifulSoup, datos: Dict, navegador: webdriver.Chrome = None):
        """Extrae los datos de contacto"""
        try:
            # Coordenadas
            datos['COORDENADAS'] = self.extractor.extraer_coordenadas(soup)

            # Dirección extraída del scraping
            direccion_scraping = soup.find('span', {'itemprop': 'address'}).get_text(strip=True) if soup.find('span', {'itemprop': 'address'}) else ""

            # Comparar dirección con la de la empresa
            if 'DOMICILIO' in datos:
                if datos['DOMICILIO'] and datos['DOMICILIO'].lower() in direccion_scraping.lower():
                    print("[DEBUG] ✅ La dirección coincide con la del scraping.")
                else:
                    print("[DEBUG] ❗ La dirección en la web es más extensa, actualizando...")
                    if direccion_scraping:  # Solo actualizar si encontramos algo
                        datos['DOMICILIO'] = direccion_scraping
            elif direccion_scraping:  # Si no hay dirección pero la encontramos
                datos['DOMICILIO'] = direccion_scraping
            
            # Teléfonos con verificación
            telefonos = self.extractor.extraer_telefonos(soup, navegador)
            for i in range(1, 4):  # TELEFONO_1, TELEFONO_2, TELEFONO_3
                datos[f'TELEFONO_{i}'] = telefonos[i-1]
                
            # Email con validación
            email = soup.find('a', class_='email')
            if email and '@' in email['href']:
                email_limpio = email['href'].replace('mailto:', '').split('?')[0].strip()
                if re.match(r'[^@]+@[^@]+\.[^@]+', email_limpio):
                    datos['EMAIL'] = email_limpio
            
            # Web y redes sociales con verificación
            # Extraer y actualizar la URL de la empresa
            web = soup.find('a', class_='url')
            if web and web['href'].startswith(('http://', 'https://')):
                datos['URL'] = web['href']
                print(f"[DEBUG] URL encontrada: {datos['URL']}")

                # Extraer redes sociales solo si la empresa tiene una web oficial
                if datos['URL']:
                    try:
                        redes = self.extractor.extraer_redes_sociales(datos['URL'])
                        datos.update(redes)
                        print("[DEBUG] Redes sociales extraídas con éxito.")
                    except Exception as e:
                        logger.warning(f"Error al extraer redes sociales: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error al extraer datos de contacto: {str(e)}")

    def _extraer_datos_comerciales(self, soup: BeautifulSoup, datos: Dict):
        """Extrae los datos comerciales"""
        try:
            datos_comerciales = soup.find('div', id='datoscomerciales')
            if datos_comerciales:
                # Extraer evolución de ventas
                evolucion = datos_comerciales.find('span', class_='text-bodytext-m text-neutrals-700')
                if evolucion:
                    datos['EVOLUCION_VENTAS'] = evolucion.get_text(strip=True)
                
                # Extraer otros datos comerciales con manejo mejorado
                for seccion in datos_comerciales.find_all('div', class_='flex flex-col gap-2'):
                    titulo = seccion.find('h3')
                    if not titulo:
                        continue
                        
                    titulo_texto = titulo.get_text(strip=True)
                    
                    # Buscar valor en diferentes formatos
                    valor = (
                        seccion.find('span', class_='text-neutrals-700') or
                        seccion.find('a', class_='text-bodytext-m') or
                        seccion.find('span', class_='value')
                    )
                    
                    mapeo_comercial = {
                        'Operaciones Internacionales': 'OPERACIONES_INTERNACIONALES',
                        'Grupo Sector': 'GRUPO_SECTOR',
                        'Cargos': 'CARGOS',
                        'Número de empleados': 'NUMERO_EMPLEADOS',
                        'Cotiza en Bolsa': 'COTIZA_BOLSA'
                    }
                    
                    if titulo_texto in mapeo_comercial:
                        datos[mapeo_comercial[titulo_texto]] = valor.get_text(strip=True) if valor else ""
                        
        except Exception as e:
            logger.error(f"Error al extraer datos comerciales: {str(e)}")

class GestorProcesamiento:
    """Gestiona el procesamiento completo del scraping"""
    
    def __init__(self, config: ConfiguracionScraper):
        self.config = config
        self.procesador = ProcesadorEmpresas(config)

    def verificar_archivo_entrada(self) -> bool:
        """Verifica que el archivo de entrada existe y tiene el formato correcto"""
        try:
            if not Path(self.config.archivo_entrada).exists():
                logger.error(f"El archivo {self.config.archivo_entrada} no existe")
                return False
            
            df = pd.read_csv(self.config.archivo_entrada)
            columnas_requeridas = ['RAZON_SOCIAL']
            for columna in columnas_requeridas:
                if columna not in df.columns:
                    logger.error(f"Falta la columna requerida: {columna}")
                    return False
            return True
        except Exception as e:
            logger.error(f"Error al verificar el archivo de entrada: {str(e)}")
            return False

    def procesar_lote(self, empresas: List[Dict]) -> List[Dict]:
        """Procesa un lote de empresas en paralelo"""
        resultados = []
        with ThreadPoolExecutor(max_workers=self.config.procesos) as executor:
            futuros = {
                executor.submit(self.procesador.procesar_empresa, empresa): empresa
                for empresa in empresas
            }
            
            for futuro in tqdm(futuros, desc="Procesando empresas", unit="empresa"):
                try:
                    resultado = futuro.result(timeout=300)  # 5 minutos timeout
                    resultados.append(resultado)
                except Exception as e:
                    empresa = futuros[futuro]
                    logger.error(f"Error procesando empresa {empresa['RAZON_SOCIAL']}: {str(e)}")
                    resultados.append(empresa)  # Mantener datos originales en caso de error
                    
        return resultados

    def ejecutar(self):
        """Ejecuta el proceso completo de scraping"""
        try:
            # Verificar archivo de entrada
            if not self.verificar_archivo_entrada():
                return
            
            # Leer archivo de entrada
            logger.info(f"Leyendo archivo de entrada: {self.config.archivo_entrada}")
            # Leer el archivo de entrada y capturar todas las columnas originales
            df = pd.read_csv(self.config.archivo_entrada, dtype=str).fillna("")  # Cargar como texto y evitar valores NaN
            columnas_originales = list(df.columns)

            # Asegurar que todas las columnas del CSV original estén en COLUMNAS_SALIDA
            for columna in columnas_originales:
                if columna not in COLUMNAS_SALIDA:
                    COLUMNAS_SALIDA.append(columna)

            empresas = df.to_dict('records')
            
            # Dividir en lotes
            total_empresas = len(empresas)
            tamaño_lote = min(self.config.chunk_size, total_empresas)
            lotes = [
                empresas[i:i + tamaño_lote]
                for i in range(0, total_empresas, tamaño_lote)
            ]
            
            logger.info(f"Procesando {total_empresas} empresas en {len(lotes)} lotes")
            
            # Procesar lotes con control de errores mejorado
            resultados_totales = []
            for i, lote in enumerate(lotes, 1):
                logger.info(f"Procesando lote {i} de {len(lotes)}")
                try:
                    resultados_lote = self.procesar_lote(lote)
                    resultados_totales.extend(resultados_lote)
                    
                    # Guardar resultados parciales
                    if i % 2 == 0:  # Cada 2 lotes
                        self._guardar_resultados_parciales(resultados_totales, i)
                        
                except Exception as e:
                    logger.error(f"Error procesando lote {i}: {str(e)}")
                
                # Pausa entre lotes
                time.sleep(random.uniform(5, 10))
            
            # Guardar resultados finales
            self._guardar_resultados_finales(resultados_totales)
            
            # Generar estadísticas
            self._generar_estadisticas(pd.DataFrame(resultados_totales))
            
        except Exception as e:
            logger.error(f"Error en la ejecución principal: {str(e)}")
            raise

    def _guardar_resultados_parciales(self, resultados: List[Dict], num_lote: int):
        """Guarda resultados parciales"""
        try:
            df_parcial = pd.DataFrame(resultados)
            df_parcial = df_parcial.reindex(columns=COLUMNAS_SALIDA)
            archivo_parcial = f"resultados_parciales/resultados_parciales_lote_{num_lote}.csv"
            df_parcial.to_csv(archivo_parcial, index=False)
            logger.info(f"Resultados parciales guardados en {archivo_parcial}")
        except Exception as e:
            logger.error(f"Error guardando resultados parciales: {str(e)}")

    def _guardar_resultados_finales(self, resultados: List[Dict]):
        """Guarda los resultados finales con el orden correcto de columnas"""
        try:
            # Definir el orden de columnas originales del CSV
            columnas_originales = [
                'COD_INFOTEL', 'NIF', 'RAZON_SOCIAL', 'DOMICILIO', 
                'COD_POSTAL', 'NOM_POBLACION', 'NOM_PROVINCIA', 'URL'
            ]
        
            # Definir columnas escrapeadas (sin incluir RAZON_SOCIAL que ya está en las originales)
            columnas_scrapeadas = [
                'FORMA_JURIDICA', 'SECTOR', 'FECHA_CONSTITUCION', 'FECHA_ULTIMO_CAMBIO',
                'OBJETO_SOCIAL', 'ACTIVIDAD', 'ACTIVIDAD_CNAE', 'ESTADO_EMPRESA',
                'COORDENADAS', 'TELEFONO_1', 'TELEFONO_2', 'TELEFONO_3', 'EMAIL',
                'EVOLUCION_VENTAS', 'OPERACIONES_INTERNACIONALES', 'GRUPO_SECTOR',
                'CARGOS', 'NUMERO_EMPLEADOS', 'COTIZA_BOLSA'
            ]
        
            # Intentar recuperar datos originales del CSV
            try:
                df_original = pd.read_csv(self.config.archivo_entrada)
            
                # Crear un mapa de las razones sociales a sus datos originales
                mapa_original = {row['RAZON_SOCIAL']: row.to_dict() for _, row in df_original.iterrows()}
            
                # Combinar datos originales con datos escrapeados
                resultados_combinados = []
                for empresa in resultados:
                    if empresa['RAZON_SOCIAL'] in mapa_original:
                        # Tomar datos originales
                        datos_combinados = mapa_original[empresa['RAZON_SOCIAL']].copy()
                        # Actualizar con datos escrapeados
                        for clave, valor in empresa.items():
                            if clave != 'RAZON_SOCIAL' or clave not in datos_combinados:
                                datos_combinados[clave] = valor
                        resultados_combinados.append(datos_combinados)
                    else:
                        resultados_combinados.append(empresa)
                    
                # Usar resultados combinados
                resultados_finales = resultados_combinados
            except Exception as e:
                logger.warning(f"No se pudo recuperar datos del CSV original: {str(e)}")
                resultados_finales = resultados
        
            # Combinar todas las columnas en el orden deseado
            todas_columnas = columnas_originales.copy()
            for col in columnas_scrapeadas:
                if col not in todas_columnas:
                    todas_columnas.append(col)
                
            # Crear dataframe con todos los resultados
            df_final = pd.DataFrame(resultados_finales)
        
            # Filtrar para incluir solo columnas que existen en los datos
            columnas_existentes = [col for col in todas_columnas if col in df_final.columns]
            df_final = df_final.reindex(columns=columnas_existentes, fill_value="")
        
            # Guardar CSV con el orden correcto
            df_final.to_csv(self.config.archivo_salida, index=False)
            logger.info(f"Resultados finales guardados en {self.config.archivo_salida}")
        
        except Exception as e:
            logger.error(f"Error guardando resultados finales: {str(e)}")

    def _generar_estadisticas(self, df: pd.DataFrame):
        """Genera estadísticas del proceso de scraping"""
        try:
            stats = {
                'total_empresas': len(df),
                'empresas_con_web': df['URL'].notna().sum(),
                'empresas_con_email': df['EMAIL'].notna().sum(),
                'empresas_con_telefono': df['TELEFONO_1'].notna().sum(),
                'empresas_con_coordenadas': df['COORDENADAS'].notna().sum(),
                'fecha_ejecucion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Guardar estadísticas
            archivo_stats = f'logs/estadisticas_scraping_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            with open(archivo_stats, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=4, ensure_ascii=False)
                
            logger.info(f"Estadísticas generadas y guardadas en {archivo_stats}")
            
        except Exception as e:
            logger.error(f"Error generando estadísticas: {str(e)}")

def verificar_dependencias() -> bool:
    """Verifica que todas las dependencias necesarias estén instaladas y configuradas"""
    try:
        # Verificar Chrome y ChromeDriver
        options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(options=options)
        driver.quit()
        
        # Verificar otras dependencias
        import pandas as pd
        import requests
        from bs4 import BeautifulSoup
        
        return True
    except Exception as e:
        logger.error(f"Error verificando dependencias: {str(e)}")
        return False

def crear_estructura_directorios():
    """Crea la estructura de directorios necesaria"""
    directorios = ['logs', 'resultados_parciales', 'backup']
    for directorio in directorios:
        Path(directorio).mkdir(exist_ok=True)

def backup_archivo_entrada(config: ConfiguracionScraper):
    """Crea una copia de seguridad del archivo de entrada"""
    try:
        archivo_entrada = Path(config.archivo_entrada)
        if archivo_entrada.exists():
            backup_path = Path('backup') / f"{archivo_entrada.stem}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            import shutil
            shutil.copy2(archivo_entrada, backup_path)
            logger.info(f"Backup creado en {backup_path}")
    except Exception as e:
        logger.error(f"Error creando backup: {str(e)}")

def main():
    """Función principal del scraper"""
    try:
        # Mostrar banner de inicio
        print("""
╔══════════════════════════════════════════╗
║     Scraper Empresite - Versión 2.0      ║
║      Fecha: 24/02/2025                   ║
╚══════════════════════════════════════════╝
        """)
        
        # Verificar dependencias
        logger.info("Verificando dependencias...")
        if not verificar_dependencias():
            logger.error("Error en las dependencias. Abortando ejecución.")
            return
        
        # Crear estructura de directorios
        crear_estructura_directorios()
        
        # Configuración inicial
        config = ConfiguracionScraper()
        
        # Crear backup del archivo de entrada
        backup_archivo_entrada(config)
        
        # Iniciar procesamiento con manejo de interrupciones
        try:
            gestor = GestorProcesamiento(config)
            gestor.ejecutar()
        except KeyboardInterrupt:
            logger.warning("Proceso interrumpido por el usuario")
            # Aquí se podría implementar guardado de emergencia
        except Exception as e:
            logger.critical(f"Error crítico en la ejecución: {str(e)}")
            raise
        finally:
            # Limpiar recursos y archivos temporales si es necesario
            logger.info("Limpiando recursos...")
            for archivo in Path('resultados_parciales').glob('*.tmp'):
                archivo.unlink()
        
        logger.info("Proceso completado exitosamente")
        
    except Exception as e:
        logger.critical(f"Error fatal en la ejecución: {str(e)}")
        raise
    finally:
        # Mostrar estadísticas de uso de recursos
        import psutil
        proceso = psutil.Process()
        memoria_usada = proceso.memory_info().rss / 1024 / 1024  # MB
        tiempo_cpu = proceso.cpu_times()
        logger.info(f"Uso de recursos - Memoria: {memoria_usada:.2f}MB, CPU User: {tiempo_cpu.user:.2f}s, CPU System: {tiempo_cpu.system:.2f}s")

if __name__ == "__main__":
    main()