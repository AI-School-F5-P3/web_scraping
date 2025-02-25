# Sistema de Scraping Distribuido con Supabase y Redis

Este proyecto implementa un sistema de scraping distribuido para procesar grandes volúmenes de datos de empresas (100,000+) utilizando múltiples workers que pueden ejecutarse en diferentes máquinas. La solución utiliza **Supabase** como base de datos centralizada y **Redis Cloud** para coordinar el trabajo entre los diferentes nodos.

## Arquitectura del Sistema

![Arquitectura Distribuida](https://i.imgur.com/example_diagram.png)

El sistema está compuesto por los siguientes componentes:

1. **Base de Datos Centralizada (Supabase)**: Almacena todos los datos de empresas y resultados de scraping.
2. **Sistema de Colas (Redis)**: Maneja la distribución y seguimiento de tareas.
3. **Worker Nodes**: Nodos de procesamiento que ejecutan el scraping en paralelo.
4. **Dashboard de Monitoreo**: Interfaz para seguir el progreso y rendimiento.

## Requisitos Previos

- Python 3.8+
- Cuenta en [Supabase](https://supabase.com/) (Plan gratuito o de pago)
- Cuenta en [Redis Cloud](https://redis.com/try-free/) (Plan gratuito o de pago)
- Paquetes Python (ver `requirements.txt`)

## Configuración Inicial

### 1. Crear Cuentas en Servicios Cloud

#### Supabase
1. Regístrate en [Supabase](https://supabase.com/)
2. Crea un nuevo proyecto
3. Anota la URL, la clave API y los detalles de conexión PostgreSQL

#### Redis Cloud
1. Regístrate en [Redis Cloud](https://redis.com/try-free/)
2. Crea una base de datos (el plan gratuito es suficiente para empezar)
3. Anota los detalles de conexión (host, puerto, contraseña)

### 2. Configurar Variables de Entorno

Crea un archivo `.env` en la raíz del proyecto:

```
# Supabase
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_KEY=your-anon-key
SUPABASE_SERVICE_KEY=your-service-key
SUPABASE_DB_HOST=your-project-id.supabase.co
SUPABASE_DB_PORT=5432
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD=your-db-password
SUPABASE_DB_NAME=postgres

# Redis Cloud
REDIS_HOST=your-redis-host.redislabs.com
REDIS_PORT=15678
REDIS_PASSWORD=your-redis-password
REDIS_USERNAME=default

# Configuración general
MAX_WORKERS_PER_NODE=4
SCRAPING_RATE_LIMIT=60
```

### 3. Migrar el Esquema de Base de Datos

Ejecuta el siguiente SQL en la consola SQL de Supabase:

```sql
CREATE TABLE sociedades (
    id SERIAL PRIMARY KEY,
    cod_infotel INTEGER NOT NULL,
    nif VARCHAR(11),
    razon_social VARCHAR(255),
    domicilio VARCHAR(255),
    cod_postal VARCHAR(5),
    nom_poblacion VARCHAR(100),
    nom_provincia VARCHAR(100),
    url VARCHAR(255),
    url_valida VARCHAR(255),
    url_exists BOOLEAN DEFAULT FALSE NOT NULL,
    url_limpia VARCHAR(255),
    url_status INTEGER,
    url_status_mensaje VARCHAR(255),
    telefono_1 VARCHAR(16),
    telefono_2 VARCHAR(16),
    telefono_3 VARCHAR(16),
    facebook VARCHAR(255),
    twitter VARCHAR(255),
    linkedin VARCHAR(255),
    instagram VARCHAR(255),
    youtube VARCHAR(255),
    e_commerce BOOLEAN DEFAULT FALSE NOT NULL,
    processed BOOLEAN DEFAULT FALSE NOT NULL,
    worker_id VARCHAR(50),
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted BOOLEAN DEFAULT FALSE
);

-- Crear índice único
CREATE UNIQUE INDEX idx_sociedades_cod_infotel 
ON sociedades(cod_infotel);
```

### 4. Instalar Dependencias

```bash
pip install -r requirements.txt
```

## Uso del Sistema

### 1. Cargar Datos y Encolar Tareas

Para cargar un archivo Excel o CSV y encolar las empresas para procesamiento:

```bash
python load_and_enqueue.py datos_empresas.xlsx --batch-size 1000
```

Opciones:
- `--batch-size`: Tamaño de lote para encolar (por defecto: 1000)
- `--reset`: Reiniciar todas las colas antes de cargar

### 2. Ejecutar Workers

Cada colaborador puede ejecutar uno o más workers en su máquina:

```bash
python worker.py --max-tasks 5000
```

Opciones:
- `--max-tasks`: Número máximo de tareas a procesar por worker (por defecto: sin límite)
- `--idle-timeout`: Tiempo máximo de espera cuando no hay tareas (segundos, por defecto: 60)

Para ejecutar múltiples workers en una misma máquina:

```bash
# En terminales/consolas separadas
python worker.py
python worker.py
python worker.py
python worker.py
```

### 3. Monitorear el Progreso

**Modo consola (rico):**
```bash
python monitor.py --refresh-rate 3
```

**Dashboard web con Streamlit:**
```bash
streamlit run dashboard.py
```

## Comandos Principales

| Comando | Descripción |
|---------|-------------|
| `python load_and_enqueue.py <archivo>` | Carga datos desde un archivo y encola tareas |
| `python worker.py` | Ejecuta un worker para procesar tareas |
| `python monitor.py` | Monitorea el progreso en consola |
| `streamlit run dashboard.py` | Lanza el dashboard web |

## Recomendaciones de Despliegue

### Para procesar 100,000 empresas:

1. **Distribución Recomendada:**
   - 4-8 workers por máquina (dependiendo de los recursos)
   - Cada worker puede procesar aproximadamente 2-5 empresas por minuto
   - Con 4 personas ejecutando 4 workers cada una: ~16 workers = ~32-80 empresas/minuto
   - Tiempo estimado: 21-52 horas para 100,000 empresas

2. **Optimización de Rate Limiting:**
   - Ajustar el parámetro `SCRAPING_RATE_LIMIT` en `.env` según la capacidad de tu red
   - Monitorizarlo para evitar bloqueos de IP

3. **Tolerancia a Fallos:**
   - El sistema está diseñado para manejar caídas de workers
   - Las tareas se recuperarán automáticamente si un worker falla

## Estructura de Archivos del Proyecto

```
├── config/
│   ├── redis_config.py          # Configuración de Redis
│   ├── supabase_config.py       # Configuración de Supabase
│   └── timeout_config.py        # Configuración de timeouts
├── database_supabase.py         # Gestor de base de datos Supabase
├── db_validator.py              # Validación de datos
├── dashboard.py                 # Dashboard web con Streamlit
├── load_and_enqueue.py          # Script para cargar datos y encolar
├── monitor.py                   # Monitor en consola
├── scraping_flow.py             # Lógica de scraping
├── task.py                      # Definición de tareas
├── task_manager.py              # Gestor de colas de tareas
├── worker.py                    # Worker para procesamiento distribuido
├── requirements.txt             # Dependencias del proyecto
└── .env                         # Variables de entorno (no incluir en git)
```

## Gestión del Código con Git

### Crear nueva rama para desarrollo distribuido

```bash
# Clonar el repositorio si aún no lo has hecho
git clone <url-del-repositorio>
cd <directorio-del-repositorio>

# Crear y cambiar a nueva rama
git checkout -b feature/distributed-architecture

# Añadir archivos nuevos
git add .

# Commit de cambios
git commit -m "Implementación de arquitectura distribuida con Supabase y Redis"

# Subir la rama al repositorio remoto
git push -u origin feature/distributed-architecture
```

## Solución de Problemas Comunes

### Conexión a Redis falla
- Verifica que las credenciales en `.env` sean correctas
- Asegúrate de que tu IP esté permitida en la configuración de Redis Cloud

### Conexión a Supabase falla
- Verifica que las credenciales en `.env` sean correctas
- Asegúrate de que la política de PostgreSQL permita conexiones externas

### Los workers no procesan tareas
- Ejecuta `python monitor.py` para verificar el estado de las colas
- Asegúrate de que haya tareas en la cola `scraper:pending`

### Tasas de procesamiento lentas
- Verifica la configuración de rate limiting
- Monitoriza el uso de recursos (CPU, memoria, red) en los nodos de worker

## Próximos Pasos

- Implementar autenticación en el dashboard
- Añadir funcionalidad de re-intentar tareas fallidas
- Desarrollar sistema de notificaciones (email, Slack, etc.)
- Implementar mecanismos avanzados de detección de proxy para evitar bloqueos
