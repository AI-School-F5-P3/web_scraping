# Sistema de Scraping Distribuido con Supabase y Redis

Este proyecto implementa un sistema de scraping distribuido para procesar grandes volúmenes de datos de empresas (100,000+) utilizando múltiples workers que pueden ejecutarse en diferentes máquinas. La solución utiliza **Supabase** como base de datos centralizada y **Redis Cloud** para coordinar el trabajo entre los diferentes nodos.

## Arquitectura del Sistema

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 500">
  <!-- Fondo y título -->
  <rect width="800" height="500" fill="#f8f9fa" rx="5" ry="5"/>
  <text x="400" y="30" font-family="Arial, sans-serif" font-size="22" font-weight="bold" text-anchor="middle" fill="#333">Arquitectura Distribuida del Sistema de Scraping</text>
  
  <!-- Supabase (Base de datos) -->
  <rect x="40" y="220" width="160" height="120" rx="5" ry="5" fill="#3ecf8e" fill-opacity="0.2" stroke="#3ecf8e" stroke-width="2"/>
  <text x="120" y="200" font-family="Arial, sans-serif" font-size="16" font-weight="bold" text-anchor="middle" fill="#333">Supabase</text>
  <text x="120" y="250" font-family="Arial, sans-serif" font-size="14" text-anchor="middle" fill="#333">Base de Datos</text>
  <text x="120" y="270" font-family="Arial, sans-serif" font-size="14" text-anchor="middle" fill="#333">Centralizada</text>
  <text x="120" y="310" font-family="Arial, sans-serif" font-size="12" text-anchor="middle" fill="#555">Almacena datos de</text>
  <text x="120" y="330" font-family="Arial, sans-serif" font-size="12" text-anchor="middle" fill="#555">empresas y resultados</text>
  
  <!-- Redis (Sistema de colas) -->
  <rect x="320" y="90" width="160" height="120" rx="5" ry="5" fill="#dc382c" fill-opacity="0.2" stroke="#dc382c" stroke-width="2"/>
  <text x="400" y="70" font-family="Arial, sans-serif" font-size="16" font-weight="bold" text-anchor="middle" fill="#333">Redis Cloud</text>
  <text x="400" y="120" font-family="Arial, sans-serif" font-size="14" text-anchor="middle" fill="#333">Sistema de Colas</text>
  <text x="400" y="160" font-family="Arial, sans-serif" font-size="12" text-anchor="middle" fill="#555">Coordina tareas entre</text>
  <text x="400" y="180" font-family="Arial, sans-serif" font-size="12" text-anchor="middle" fill="#555">workers distribuidos</text>
  
  <!-- Workers - Máquina 1 -->
  <rect x="600" y="100" width="160" height="80" rx="5" ry="5" fill="#4285f4" fill-opacity="0.2" stroke="#4285f4" stroke-width="2"/>
  <text x="680" y="80" font-family="Arial, sans-serif" font-size="14" font-weight="bold" text-anchor="middle" fill="#333">Máquina 1</text>
  <rect x="620" y="115" width="120" height="25" rx="3" ry="3" fill="#4285f4" fill-opacity="0.3" stroke="#4285f4" stroke-width="1"/>
  <text x="680" y="132" font-family="Arial, sans-serif" font-size="12" text-anchor="middle" fill="#333">Worker 1</text>
  <rect x="620" y="145" width="120" height="25" rx="3" ry="3" fill="#4285f4" fill-opacity="0.3" stroke="#4285f4" stroke-width="1"/>
  <text x="680" y="162" font-family="Arial, sans-serif" font-size="12" text-anchor="middle" fill="#333">Worker 2</text>
  
  <!-- Workers - Máquina 2 -->
  <rect x="600" y="220" width="160" height="80" rx="5" ry="5" fill="#4285f4" fill-opacity="0.2" stroke="#4285f4" stroke-width="2"/>
  <text x="680" y="200" font-family="Arial, sans-serif" font-size="14" font-weight="bold" text-anchor="middle" fill="#333">Máquina 2</text>
  <rect x="620" y="235" width="120" height="25" rx="3" ry="3" fill="#4285f4" fill-opacity="0.3" stroke="#4285f4" stroke-width="1"/>
  <text x="680" y="252" font-family="Arial, sans-serif" font-size="12" text-anchor="middle" fill="#333">Worker 3</text>
  <rect x="620" y="265" width="120" height="25" rx="3" ry="3" fill="#4285f4" fill-opacity="0.3" stroke="#4285f4" stroke-width="1"/>
  <text x="680" y="282" font-family="Arial, sans-serif" font-size="12" text-anchor="middle" fill="#333">Worker 4</text>
  
  <!-- Workers - Máquina 3 -->
  <rect x="600" y="340" width="160" height="80" rx="5" ry="5" fill="#4285f4" fill-opacity="0.2" stroke="#4285f4" stroke-width="2"/>
  <text x="680" y="320" font-family="Arial, sans-serif" font-size="14" font-weight="bold" text-anchor="middle" fill="#333">Máquina 3</text>
  <rect x="620" y="355" width="120" height="25" rx="3" ry="3" fill="#4285f4" fill-opacity="0.3" stroke="#4285f4" stroke-width="1"/>
  <text x="680" y="372" font-family="Arial, sans-serif" font-size="12" text-anchor="middle" fill="#333">Worker 5</text>
  <rect x="620" y="385" width="120" height="25" rx="3" ry="3" fill="#4285f4" fill-opacity="0.3" stroke="#4285f4" stroke-width="1"/>
  <text x="680" y="402" font-family="Arial, sans-serif" font-size="12" text-anchor="middle" fill="#333">Worker 6</text>
  
  <!-- Dashboard de Monitoreo -->
  <rect x="320" y="340" width="160" height="100" rx="5" ry="5" fill="#fbbc05" fill-opacity="0.2" stroke="#fbbc05" stroke-width="2"/>
  <text x="400" y="320" font-family="Arial, sans-serif" font-size="16" font-weight="bold" text-anchor="middle" fill="#333">Dashboard</text>
  <text x="400" y="370" font-family="Arial, sans-serif" font-size="14" text-anchor="middle" fill="#333">Monitoreo</text>
  <text x="400" y="390" font-family="Arial, sans-serif" font-size="12" text-anchor="middle" fill="#555">Seguimiento en</text>
  <text x="400" y="410" font-family="Arial, sans-serif" font-size="12" text-anchor="middle" fill="#555">tiempo real</text>
  
  <!-- Líneas de conexión -->
  <!-- Supabase a Redis -->
  <path d="M200 250 C250 200, 230 150, 320 150" stroke="#666" stroke-width="2" fill="none" stroke-dasharray="5,5"/>
  <polygon points="315,150 325,145 325,155" fill="#666"/>
  
  <!-- Redis a Supabase -->
  <path d="M320 170 C250 220, 250 260, 200 270" stroke="#666" stroke-width="2" fill="none" stroke-dasharray="5,5"/>
  <polygon points="205,270 195,265 195,275" fill="#666"/>
  
  <!-- Redis a Máquina 1 -->
  <path d="M480 130 L600 130" stroke="#666" stroke-width="2" fill="none"/>
  <polygon points="595,130 605,125 605,135" fill="#666"/>
  
  <!-- Máquina 1 a Redis -->
  <path d="M600 150 L480 150" stroke="#666" stroke-width="2" fill="none"/>
  <polygon points="485,150 475,145 475,155" fill="#666"/>
  
  <!-- Redis a Máquina 2 -->
  <path d="M480 160 C550 180, 550 230, 600 240" stroke="#666" stroke-width="2" fill="none"/>
  <polygon points="595,240 605,235 605,245" fill="#666"/>
  
  <!-- Máquina 2 a Redis -->
  <path d="M600 260 C550 270, 540 190, 480 170" stroke="#666" stroke-width="2" fill="none"/>
  <polygon points="485,170 475,165 475,175" fill="#666"/>
  
  <!-- Redis a Máquina 3 -->
  <path d="M480 170 C500 240, 550 350, 600 360" stroke="#666" stroke-width="2" fill="none"/>
  <polygon points="595,360 605,355 605,365" fill="#666"/>
  
  <!-- Máquina 3 a Redis -->
  <path d="M600 380 C540 370, 480 250, 460 180" stroke="#666" stroke-width="2" fill="none"/>
  <polygon points="465,180 455,175 455,185" fill="#666"/>
  
  <!-- Redis a Dashboard -->
  <path d="M400 210 L400 340" stroke="#666" stroke-width="2" fill="none"/>
  <polygon points="400,335 395,345 405,345" fill="#666"/>
  
  <!-- Dashboard a Supabase -->
  <path d="M320 390 C250 380, 220 320, 200 300" stroke="#666" stroke-width="2" fill="none" stroke-dasharray="5,5"/>
  <polygon points="205,300 195,295 195,305" fill="#666"/>
  
  <!-- Leyenda -->
  <rect x="40" y="400" width="200" height="80" rx="5" ry="5" fill="white" stroke="#ccc" stroke-width="1"/>
  <text x="60" y="420" font-family="Arial, sans-serif" font-size="14" font-weight="bold" fill="#333">Leyenda:</text>
  
  <line x1="60" y1="435" x2="90" y2="435" stroke="#666" stroke-width="2" fill="none"/>
  <polygon points="85,435 95,430 95,440" fill="#666"/>
  <text x="105" y="440" font-family="Arial, sans-serif" font-size="12" fill="#333">Flujo de datos</text>
  
  <line x1="60" y1="460" x2="90" y2="460" stroke="#666" stroke-width="2" fill="none" stroke-dasharray="5,5"/>
  <polygon points="85,460 95,455 95,465" fill="#666"/>
  <text x="105" y="465" font-family="Arial, sans-serif" font-size="12" fill="#333">Consultas a BD</text>
</svg>

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

Hay dos opciones para cargar y encolar tareas:

#### Opción 1: Usando el script load_and_enqueue.py

Para cargar un archivo Excel o CSV y encolar las empresas para procesamiento:

```bash
python load_and_enqueue.py datos_empresas.xlsx --batch-size 1000
```

Opciones:
- `--batch-size`: Tamaño de lote para encolar (por defecto: 1000)
- `--reset`: Reiniciar todas las colas antes de cargar

#### Opción 2: Usando distributed_scraping.py con comando enqueue

Para encolar empresas directamente desde la base de datos:

```bash
python distributed_scraping.py enqueue --limit 1000
```

Opciones:
- `--limit`: Número máximo de empresas a encolar

### 2. Ejecutar Workers

Hay dos opciones para ejecutar workers:

#### Opción 1: Usando worker.py

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

#### Opción 2: Usando distributed_scraping.py con comando worker

Para ejecutar un worker con el script consolidado:

```bash
python distributed_scraping.py worker
```

Opciones:
- `--worker-id`: ID personalizado para el worker (por defecto: se genera automáticamente)
- `--max-tasks`: Número máximo de tareas a procesar (por defecto: sin límite)
- `--idle-timeout`: Tiempo máximo de espera en segundos (por defecto: 60)

Ejemplo con ID personalizado:

```bash
python distributed_scraping.py worker --worker-id "maquina1_angel"
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
| `python distributed_scraping.py enqueue --limit N` | Encola N empresas desde la base de datos |
| `python distributed_scraping.py worker` | Ejecuta un worker con el script consolidado |
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

├── redis_config.py          # Configuración de Redis
├──supabase_config.py       # Configuración de Supabase
├──config.py        # Configuración de timeouts
├── database_supabase.py         # Gestor de base de datos Supabase
├── db_validator.py              # Validación de datos
├── dashboard.py                 # Dashboard web con Streamlit
├── distributed_scraping.py      # Script consolidado (enqueue y worker)
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

### Error 'TaskManager' object has no attribute 'worker_id'
- Asegúrate de que la clase `TaskManager` inicializa correctamente el atributo `worker_id`
- Verifica que estás pasando el `worker_id` correctamente al crear el `TaskManager`

## Próximos Pasos

- Implementar autenticación en el dashboard
- Añadir funcionalidad de re-intentar tareas fallidas
- Desarrollar sistema de notificaciones (email, Slack, etc.)
- Implementar mecanismos avanzados de detección de proxy para evitar bloqueos