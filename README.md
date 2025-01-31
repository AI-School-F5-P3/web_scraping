# web_scraping
Proyecto Web Scraping - Experian

```
/project-root
├── /scraper              # Módulo principal de scraping
│   ├── workers.py        # Implementación de workers distribuidos
│   ├── rate_limiter.py   # Gestión de tasas usando Redis
│   └── data_pipeline.py  # Transformación y limpieza de datos
│
├── /database             # Capa de persistencia de datos
│   ├── models.py         # Modelos SQLAlchemy
│   ├── connectors.py     # Manejo de conexiones a SQL Server
│   └── migrations        # Migraciones de base de datos
│
├── /chatbot              # Motor de consultas analíticas
│   ├── nlp_to_sql.py     # Traductor lenguaje natural -> SQL
│   └── query_executor.py # Ejecución segura de queries
│
├── /frontend             # Interfaz de usuario Streamlit
│   ├── main.py           # Dashboard principal
│   ├── utils.py          # Funciones auxiliares
│   └── /assets           # Recursos estáticos (CSS/Imágenes)
│
├── /monitoring           # Configuración de monitorización
│   ├── prometheus.yml    # Configuración de métricas
│   └── grafana_dashboards# JSON de dashboards preconfigurados
│
├── config.py             # Configuración global del proyecto
├── requirements.txt      # Dependencias de Python
└── .env                  # Variables de entorno sensibles
```

## Flujo de Trabajo del Sistema

```mermaid
graph TD
    A[Inicio: Datos Experian] --> B[Redis Queue]
    B --> C{Worker Disponible?}
    C -->|Sí| D[Scrapear Web]
    D --> E[Validar con OpenAI?]
    E -->|No| F[Procesamiento Local]
    E -->|Sí| G[Llamada API OpenAI]
    G --> H[Extraer Datos Clave]
    H --> I[Guardar en SQL Server]
    I --> J[Actualizar Redis]
    J --> K{¿Errores?}
    K -->|Sí| L[Reintentar o Log]
    K -->|No| M[Notificar Éxito]
    M --> N[Streamlit Dashboard]
    N --> O[Consultas Naturales]
    O --> P[Generar SQL]
    P --> Q[Visualizar Datos]