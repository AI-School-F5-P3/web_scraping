import requests
import logging
from urllib.parse import urljoin

#Configuración del logging
name='hola'
logging.basicConfig(level=logging.DEBUG,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name)

def get_company_by_irus(irus):
    try:
        # URL base correcta según el portal de datos abiertos del Registro Mercantil
        base_url = "https://opendata.registradores.org/"
        endpoint = f"/datos/sociedades/{irus}"  # Cambiamos el endpoint
        url = urljoin(base_url, endpoint)

        logger.debug(f"URL generada: {url}")

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, /',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/json',
            'Origin': 'https://opendata.registradores.org/',
            'Referer': 'https://opendata.registradores.org/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'
        }

        logger.debug(f"Headers utilizados: {headers}")

        session = requests.Session()
        session.get(base_url, headers=headers)

        logger.info(f"Iniciando petición GET a {url}")
        response = session.get(
            url,
            headers=headers,
            timeout=15,
            verify=True
        )

        logger.debug(f"Status code recibido: {response.status_code}")
        logger.debug(f"Headers de respuesta: {dict(response.headers)}")
        logger.debug(f"Contenido de la respuesta: {response.text}")

        if response.status_code == 200:
            data = response.json()
            logger.info("Datos JSON parseados correctamente")

            return {
                'denominacion': data.get('denominacionSocial'),
                'nif': data.get('cif'),
                'forma_juridica': data.get('formaSocial')
            }
        else:
            error_msg = f"Status {response.status_code}"
            try:
                error_data = response.json()
                error_msg = f"{error_msg} - {error_data.get('message', 'No message available')}"
            except:
                error_msg = f"{error_msg} - {response.text[:200]}"
            logger.error(f"Error en la respuesta: {error_msg}")
            return f"Error: {error_msg}"

    except requests.exceptions.Timeout:
        logger.error("La petición excedió el tiempo de espera")
        return "Error: Timeout en la petición"
    except requests.exceptions.ConnectionError:
        logger.error("Error de conexión con el servidor")
        return "Error: No se pudo conectar con el servidor"
    except requests.exceptions.RequestException as e:
        logger.error(f"Error en la petición HTTP: {str(e)}")
        return f"Error en la petición HTTP: {str(e)}"
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return f"Error inesperado: {str(e)}"


irus = "1000003295252"
logger.info(f"Iniciando consulta para IRUS: {irus}")
resultado = get_company_by_irus(irus)
logger.info(f"Resultado final: {resultado}")
print(resultado)