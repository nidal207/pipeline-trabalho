import pandas as pd
from sqlalchemy import create_engine

# CONFIG
DB_USER = "postgres"
DB_PASS = "sua_senha"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "iot"
CSV_PATH = "data/IOT-temp.csv"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


def process_and_load():
    print("Lendo CSV...")
    df = pd.read_csv(CSV_PATH)

    print("Tratando dados...")
    df.columns = df.columns.str.lower()

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    print("Inserindo no banco...")
    df.to_sql("temperature_readings", engine, if_exists="replace", index=False)

    print("Criando views...")
    create_views()

    print("Pipeline finalizado com sucesso!")


def create_views():
    with engine.connect() as conn:
        conn.execute("""
        CREATE OR REPLACE VIEW avg_temp_por_dispositivo AS
        SELECT device_id, AVG(temperature) AS avg_temp
        FROM temperature_readings
        GROUP BY device_id;
        """)

        conn.execute("""
        CREATE OR REPLACE VIEW leituras_por_hora AS
        SELECT EXTRACT(HOUR FROM timestamp) AS hora,
               COUNT(*) AS contagem
        FROM temperature_readings
        GROUP BY hora
        ORDER BY hora;
        """)

        conn.execute("""
        CREATE OR REPLACE VIEW temp_max_min_por_dia AS
        SELECT DATE(timestamp) AS data,
               MAX(temperature) AS temp_max,
               MIN(temperature) AS temp_min
        FROM temperature_readings
        GROUP BY data
        ORDER BY data;
        """)


if __name__ == "__main__":
    process_and_load()