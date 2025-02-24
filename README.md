# Aplicación de Scraping de Datos de Empresas

Esta aplicación permite cargar datos de empresas desde un archivo Excel, procesarlos para extraer información adicional como teléfonos, redes sociales y detectar si cuentan con comercio electrónico.

## Características

- Carga de datos desde Excel
- Verificación de URLs existentes
- Generación y verificación de URLs alternativas cuando la original no funciona
- Extracción de teléfonos (hasta 3 por empresa)
- Extracción de redes sociales (Twitter, Instagram, LinkedIn, Facebook, YouTube)
- Detección de comercio electrónico
- Almacenamiento de datos en PostgreSQL
- Interfaz gráfica con Streamlit

## Requisitos previos

- Python 3.8+
- PostgreSQL

## Instalación

1. Clonar el repositorio:
   ```
   git clone https://github.com/AI-School-F5-P3/web_scraping
   cd scraping-empresas
   ```

2. Crear un entorno virtual y activarlo:
   ```
   python -m venv venv
   source venv/bin/activate  # En Windows: venv\Scripts\activate
   ```

3. Instalar dependencias:
   ```
   pip install -r requirements.txt
   ```

4. Configurar la base de datos PostgreSQL:
   - Crear una base de datos llamada `empresas_db` (o el nombre que prefieras)
   - Editar el archivo `config.py` con tus datos de conexión

## Estructura de archivos

- `main.py`: Aplicación principal Streamlit
- `scraping_flow.py`: Lógica del scraping
- `database.py`: Gestión de la base de datos
- `db_validator.py`: Validación de datos
- `config.py`: Configuración de la aplicación

## Uso

1. Iniciar la aplicación:
   ```
   streamlit run main.py
   ```

2. Acceder a la interfaz web:
   - Abrir navegador en http://localhost:8501

3. Flujo de trabajo:
   - Cargar datos desde Excel
   - Procesar empresas para extraer información
   - Visualizar resultados y estadísticas

## Formato del Excel

El archivo Excel debe contener al menos las siguientes columnas:
- `cod_infotel`: Identificador único de la empresa
- `razon_social`: Nombre de la empresa
- `url`: URL de la empresa (opcional)
- `nif`: NIF de la empresa (opcional)
- `domicilio`: Dirección (opcional)
- `cod_postal`: Código postal (opcional)
- `nom_poblacion`: Población (opcional)
- `nom_provincia`: Provincia (opcional)

## Flujo de procesamiento

1. Se carga el Excel y se almacena en la base de datos
2. Para cada empresa:
   - Si tiene URL, se verifica que funcione
   - Si la URL no funciona o no existe, se generan URLs alternativas
   - Se elige la mejor URL alternativa mediante un sistema de puntuación
   - Se extrae información (teléfonos, redes sociales, e-commerce) de la URL seleccionada
   - Se guardan los resultados en la base de datos

## Solución de problemas

### Base de datos
- Asegúrate de que PostgreSQL esté en ejecución
- Verifica los datos de conexión en `config.py`
- Usa la opción "Reiniciar Base de Datos" en caso de problemas

### Problemas de scraping
- Algunos sitios pueden bloquear el scraping
- La aplicación implementa rate limiting para evitar bloqueos
- Verifica los logs para más información en caso de fallos

## Licencia

Este proyecto está bajo la Licencia MIT.
