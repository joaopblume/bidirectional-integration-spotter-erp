# Big Workload ERP-CRM Integration Platform

A high-performance bidirectional data synchronization engine between Oracle ERP database and external CRM REST API. Built to handle enterprise-scale data volumes with real-time updates and comprehensive data integrity management.

## 🚀 Overview

This integration platform ensures seamless data synchronization between legacy Oracle ERP systems and modern CRM solutions. Changes in either system automatically reflect in the other, maintaining data consistency across the enterprise.

### Key Features

- **Bidirectional Real-time Sync**: Webhook-based updates from CRM, batch processing from ERP
- **Hash-based Change Detection**: Prevents duplicate processing and ensures efficiency
- **Entity Relationship Management**: Maintains complex relationships between leads, contacts, and transactions
- **Fault Tolerance**: Comprehensive error handling with retry mechanisms
- **Scalable Architecture**: Distributed processing with Apache Spark

## 🏗️ Architecture

### Infrastructure Decision

**Important Note**: The customer's infrastructure consisted of a Hyper-V virtualized environment with limited CPU resources but abundant RAM (128GB+). This constraint led to the architectural decision to offload heavy data processing from Oracle to Apache Spark standalone cluster, leveraging in-memory computing capabilities rather than CPU-intensive database operations.

### Technology Stack

- **Orchestration**: Apache Airflow 2.x
- **Data Processing**: Apache Spark 3.x (Standalone mode)
- **Database**: Oracle 19c
- **API Layer**: Python Flask REST API
- **Language**: Python 3.8+
- **File Storage**: JSON-based queue system

## 📋 System Components

### 1. Apache Airflow DAGs

Located in `/airflow_dags/`:

- **Lead Management**
  - `job_insert_leads_in_spotter.py` - New lead creation
  - `job_update_leads_in_spotter.py` - Lead updates to CRM
  - `job_update_leads_in_erp.py` - Lead updates from CRM
  - `job_update_converted_lead_in_erp.py` - Lead stage transitions
  - `job_descarta_lead.py` - Lead discard/cleanup

- **Contact Management**
  - `job_insert_contacts_in_spotter.py` - Contact creation
  - `job_update_contacts_in_spotter.py` - Contact updates
  - `job_update_contact_in_erp.py` - ERP contact sync

- **Business Operations**
  - `job_orca_lead_in_spotter.py` - Budget management
  - `job_vende_lead_in_spotter.py` - Sales conversion
  - `job_leads_full_sync.py` - Full synchronization
  - `job_validade_full.py` - Data validation

- **Processing**
  - `job_execute_spark.py` - Spark job orchestration
  - `backup_postgresql.py` - Database backup

### 2. Apache Spark Jobs

Located in `/spark_scripts/`:

- `main.py` - Main orchestrator for Spark notebooks
- `spark_insert_clientes.py` - New client data processing
- `update_clientes_spark.py` - Client update processing  
- `update_contatos_spark.py` - Contact update processing

### 3. Flask REST API

Located in `/core-api/app.py`:

**Key Endpoints**:
- `/leads` - Lead CRUD operations
- `/atualizalead` - Lead updates webhook
- `/convertedlead` - Lead stage conversion webhook
- `/inserecontato` - Contact creation
- `/vendelead` - Sales conversion
- `/orcalead/<lead_id>` - Budget management

### 4. Oracle Database Components

Located in `/oracle_db_source/`:

**Tables**:
- `LEADS` - Lead master data
- `CONTACTS` - Contact information
- `ERROR_LOG` - System error tracking

**Views**:
- `VW_CRM_CLIENTS` - Client data aggregation
- `VIEW_INSERT_LEAD_IN_CRM` - New leads for CRM
- `VIEW_UPDATE_LEAD_IN_CRM` - Lead updates
- `VIEW_LEADS_BUDGETED` - Budgeted leads
- `VIEW_LEADS_SOLD` - Sold leads

**Stored Procedures**:
- `PRC_UPDATE_CLIENTS` - Client data synchronization
- `PRC_UPDATE_CLIENT_CONTACTS` - Contact synchronization
- `PKG_CRM_CLIENT` - Client business logic package

**Triggers**:
- `TRG_CRM_UPDATE_ERP` - Automatic ERP updates on lead changes
- `TRG_CRM_UPDATE_CONTACT_ERP` - Contact sync trigger

## 🔄 Data Flow

### CRM to ERP Flow
1. CRM sends webhook to Flask API endpoints
2. API validates and saves data as JSON files
3. Airflow DAGs pick up JSON files for processing
4. Data is inserted/updated in Oracle via stored procedures
5. Triggers ensure data integrity and cascade updates

### ERP to CRM Flow
1. Oracle views detect changes using MINUS operations
2. Spark jobs read changed data and calculate hashes
3. New/modified records saved as JSON files
4. Airflow DAGs process files and call CRM API
5. Processed files moved to archive directory

## 📁 Directory Structure

```
/opt/airflow/
├── dags/                    # Airflow DAG definitions
├── logs/                    # Execution logs
└── scripts/                 # Shell scripts

/opt/spark/
├── scripts/                 # Spark processing scripts
└── drivers/jdbcdriver/      # Oracle JDBC driver

/data/
├── integration/
│   ├── leads/
│   │   ├── processar/      # Pending processing
│   │   ├── processados/    # Completed
│   │   └── falhas/         # Failed processing
│   └── contacts/
│       ├── processar/
│       ├── processados/
│       └── falhas/

/app/
└── sync/
    ├── leads/
    └── contacts/
```

## 🚀 Installation

### Prerequisites

- Oracle Database 19c+
- Apache Spark 3.x
- Apache Airflow 2.x
- Python 3.8+
- 16GB+ RAM (recommended 32GB+)

### Setup Steps

1. **Database Setup**
```sql
-- Run all SQL scripts in order:
@sequences.sql
@table_leads.sql
@table_contatos.sql
@table_error_log.sql
@procedures.sql
@triggers.sql
@views.sql
```

2. **Environment Configuration**
```bash
# Create .env file
cp .env.example .env

# Configure database connection
DB_USER=your_oracle_user
DB_PASSWD=your_oracle_password
DB_HOST=oracle_host
DB_PORT=1521
DB_SERVICE_NAME=your_service_name

# Configure CRM API
API_URL=https://your-crm-api.com/api/v1/
API_TOKEN=your_api_token
```

3. **Spark Configuration**
```bash
# Start Spark standalone
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077

# Copy Oracle JDBC driver
cp ojdbc8.jar $SPARK_HOME/jars/
```

4. **Airflow Setup**
```bash
# Initialize Airflow DB
airflow db init

# Create admin user
airflow users create --username admin --password admin --role Admin --email admin@example.com

# Start services
airflow scheduler -D
airflow webserver -p 8080 -D
```

5. **Flask API**
```bash
cd core-api
pip install -r requirements.txt
python app.py
```

## 🔧 Configuration

### Lead Stage Flow
```
Novos leads → Pesquisa → Prospecção → Passagem de bastão → 
Desenvolvimento/Orçamento → Ativação → Retenção/Expansão
```

### Custom Fields Mapping
- `_cod.gestores` - Sales representative
- `_cod.cliente` - Client code
- `_inscricaoestadual` - Tax registration
- `_analisefinanceira` - Financial analysis

## 📊 Monitoring

- **Airflow UI**: http://localhost:8080
- **Spark UI**: http://localhost:8081
- **Logs**: `/opt/airflow/logs/execucao_tasks_airflow.log`

### Key Metrics to Monitor
- JSON files in queue directories
- Failed job executions
- Data synchronization lag
- Oracle tablespace usage

## 🔒 Security Considerations

- API endpoints require token authentication
- Oracle connections use encrypted passwords
- Sensitive data masked in logs
- File permissions restricted to service accounts

