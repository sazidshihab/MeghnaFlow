## Architecture update(2026-05-18)  
  
MeghnaFlow/  
├── .gitignore                  ← includes venv/, data/, __pycache__/  
├── README.md  
├── requirements.txt            ← psycopg2-binary, faker, etc.  
├── .env.example                ← DB_HOST, DB_PORT, DB_NAME, DB_USER, etc.  
│  
├── config/  
│   └── settings.py             ← loads from .env, single source of truth for paths/creds  
│  
├── sql/  
│   ├── ddl/                    ← run once, numbered for order  
│   │   ├── 01_create_database.sql  
│   │   ├── 02_create_schemas.sql  
│   │   ├── 03_bronze_tables.sql  
│   │   ├── 04_silver_tables.sql  
│   │   └── 05_operational_log.sql  
│   ├── bronze/  
│   │   └── bronze_procedures.sql  
│   ├── silver/  
│   │   ├── silver_procedures.sql  
│   │   └── silver_validation.sql  
│   ├── gold/                   ← coming next  
│   │   └── .gitkeep  
│   └── tuning/  
│       └── pg_tuning.sql  
│  
├── src/  
│   ├── config.py               ← DB connection factory  
│   ├── executor.py  
│   └── loaders/  
│       ├── __init__.py  
│       ├── landing_to_bronze_daily.py  
│       └── bronze_daily_to_bronze_main.py  
│  
├── data/                       ← gitignored  
│   ├── landing/  
│   └── archive/  
│  
└── airflow/                    ← future  
    └── dags/  
