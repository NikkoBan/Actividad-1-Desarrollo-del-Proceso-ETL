# 🔄 Sistema ETL en Python — Clean Architecture

## Descripción

Sistema de **Extracción, Transformación y Carga (ETL)** implementado en Python, diseñado con principios **SOLID** y **Clean Architecture**. Extrae datos desde múltiples fuentes (CSV, SQL Server, API REST), los valida, transforma y almacena en staging.

---

## 🏗️ Arquitectura

```
etl/
│
├── config/                    # Configuración y settings
│   ├── config.json            # Parámetros del sistema
│   └── settings.py            # Carga de configuración
│
├── core/                      # Capa de lógica de negocio
│   ├── data_source_adapter.py # Interfaz base (ABC)
│   ├── flat_file_reader.py    # Extractor CSV
│   ├── sql_data_gateway.py    # Extractor SQL Server
│   ├── http_data_collector.py # Extractor API REST
│   ├── data_sink_manager.py   # Carga en staging
│   └── trace_manager.py       # Logging y métricas
│
├── dto/                       # Data Transfer Objects (Pydantic)
│   ├── customer_dto.py
│   ├── order_dto.py
│   ├── product_dto.py
│   ├── order_detail_dto.py
│   └── api_dto.py
│
├── models/                    # Modelos de dominio
│   ├── customer.py
│   ├── order.py
│   ├── product.py
│   └── order_detail.py
│
├── dashboard/                 # Módulo de visualización
│   └── app.py                 # Dashboard Streamlit (KPIs y gráficos)
│
├── cvs/                       # Archivos CSV fuente
├── database/                  # Scripts SQL
│   ├── Ventas.sql             # DDL — BD operacional (OLTP)
│   └── VentasAnalisis.sql     # DDL — Data Warehouse (estrella)
│
├── staging/                   # Datos procesados (salida)
├── logs/                      # Archivos de log
├── main.py                    # Worker Service (punto de entrada)
├── requirements.txt           # Dependencias
├── .env                       # Variables de entorno
└── README.md
```

---

## 🧩 Componentes Principales

| Componente | Clase | Responsabilidad |
|---|---|---|
| **DataSourceAdapter** | `data_source_adapter.py` | Interfaz ABC para todas las fuentes de datos |
| **FlatFileReader** | `flat_file_reader.py` | Lee y valida archivos CSV |
| **SqlDataGateway** | `sql_data_gateway.py` | Ejecuta queries SQL contra SQL Server |
| **HttpDataCollector** | `http_data_collector.py` | Consume APIs REST con retry automático |
| **DataSinkManager** | `data_sink_manager.py` | Almacena datos en staging (JSON/CSV) |
| **TraceManager** | `trace_manager.py` | Logging, métricas y monitoreo |
| **DwLoader** | `dw_loader.py` | Carga staging → dimensiones → hechos en el DW |
| **Dashboard** | `dashboard/app.py` | Visualización analítica con Streamlit |

---

## ⚙️ Flujo ETL

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐
│  EXTRACCIÓN  │───►│ VALIDACIÓN   │───►│ TRANSFORM.  │───►│    CARGA     │
│              │    │  (Pydantic)  │    │  (DTOs)     │    │  (Staging)   │
└─────────────┘    └──────────────┘    └─────────────┘    └──────────────┘
      │                                                          │
      ├── CSV (FlatFileReader)                                   ├── JSON
      ├── SQL (SqlDataGateway)                                   └── CSV
      └── API (HttpDataCollector)
```

1. **Extracción**: Lee datos desde las 3 fuentes en paralelo (asyncio)
2. **Validación**: Cada registro se valida con DTOs Pydantic
3. **Transformación**: Se normalizan campos y se limpian datos inválidos
4. **Carga**: Se almacenan en `staging/` organizados por fuente

---

## 🚀 Instalación y Ejecución

### Prerrequisitos

- Python 3.11+
- SQL Server LocalDB (opcional, para extracción SQL)
- Conexión a internet (para API REST)

### Instalación

```bash
cd etl
pip install -r requirements.txt
```

### Configuración

1. Editar `config/config.json` con los parámetros deseados
2. Configurar variables sensibles en `.env`:

```env
DB_CONNECTION_STRING=Driver={SQL Server};Server=(localdb)\MSSQLLocalDB;Database=VentasDB;Trusted_Connection=yes;
API_BASE_URL=https://jsonplaceholder.typicode.com
LOG_LEVEL=INFO
```

### Ejecución

```bash
python main.py
```

### Dashboard de Visualización

```bash
streamlit run dashboard/app.py
```

Abre automáticamente el navegador con el dashboard de KPIs y gráficos conectado al Data Warehouse.

### Crear la base de datos (opcional)

```bash
sqlcmd -S "(localdb)\MSSQLLocalDB" -i database/Ventas.sql
```

---

## 📐 Principios SOLID Aplicados

| Principio | Aplicación |
|---|---|
| **S** – Single Responsibility | Cada clase tiene una única responsabilidad (extracción, validación, logging) |
| **O** – Open/Closed | Nuevas fuentes de datos se agregan creando nuevos adaptadores sin modificar código existente |
| **L** – Liskov Substitution | Todos los extractores implementan `DataSourceAdapter` y son intercambiables |
| **I** – Interface Segregation | `DataSourceAdapter` define solo los métodos necesarios (`extract`, `validate`, `get_source_name`) |
| **D** – Dependency Inversion | `main.py` depende de abstracciones (`DataSourceAdapter`), no de implementaciones concretas |

---

## 📈 Características Técnicas

- **Async/Await**: Extracción asíncrona con `asyncio` para máximo rendimiento
- **Retry automático**: `HttpDataCollector` reintenta peticiones fallidas con backoff progresivo
- **Validación Pydantic**: DTOs con validación de tipos, rangos y formatos
- **Variables de entorno**: Credenciales seguras via `.env` (nunca hardcodeadas)
- **Métricas**: Tiempos de ejecución, contadores de registros procesados
- **Logging dual**: Salida a archivo (`logs/etl.log`) y consola simultáneamente

---

## 🔐 Seguridad

- Las credenciales se cargan desde `.env` y nunca se hardcodean
- El archivo `.env` debe agregarse a `.gitignore`
- Las conexiones SQL usan timeout para evitar bloqueos
- Las peticiones HTTP usan timeout y límite de reintentos

---

## 📝 Justificación Técnica

### ¿Por qué Clean Architecture?

La separación en capas (`core/`, `dto/`, `models/`, `config/`) permite:
- **Testabilidad**: Cada componente se puede probar de forma aislada
- **Mantenibilidad**: Los cambios en una fuente de datos no afectan a las demás
- **Escalabilidad**: Agregar nuevas fuentes solo requiere implementar `DataSourceAdapter`

### ¿Por qué asyncio?

Las operaciones de I/O (lectura CSV, consultas SQL, peticiones HTTP) son el cuello de botella. Con `asyncio`:
- Las peticiones API se ejecutan en paralelo
- Las lecturas de archivo se delegan a threads (`asyncio.to_thread`)
- El worker no se bloquea esperando respuestas

### ¿Por qué Pydantic?

- Validación declarativa con tipos de Python
- Mensajes de error claros y estructurados
- Serialización/deserialización automática
- Alias para mapear nombres de columnas CSV a campos Python

---

## 📄 Licencia

Proyecto académico — Uso educativo.
