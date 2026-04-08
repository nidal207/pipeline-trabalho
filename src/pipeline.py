import pandas as pd
from sqlalchemy import create_engine

# CONFIG
DB_USER = "postgres"
DB_PASS = "1234"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "iot"
CSV_PATH = "data/IOT-temp.csv"   # ajuste o caminho se necessário

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def process_and_load():
    print("Lendo CSV...")
    df = pd.read_csv(CSV_PATH)

    print("Colunas originais:", df.columns.tolist())
    
    # --- Mapeamento flexível ---
    # Identifica a coluna de dispositivo (pode ser 'room_id', 'id', 'room_id/id', 'device_id')
    device_col = None
    for col in df.columns:
        col_lower = col.lower()
        if 'room' in col_lower or col_lower == 'id':
            device_col = col
            break
    if device_col is None:
        raise ValueError("Nenhuma coluna de dispositivo encontrada. Colunas: " + str(df.columns.tolist()))
    
    # Identifica a coluna de temperatura
    temp_col = None
    for col in df.columns:
        if col.lower() == 'temp':
            temp_col = col
            break
    if temp_col is None:
        raise ValueError("Nenhuma coluna de temperatura ('temp') encontrada.")
    
    # Identifica a coluna de data/hora
    date_col = None
    for col in df.columns:
        if 'noted_date' in col.lower() or 'timestamp' in col.lower() or 'datetime' in col.lower():
            date_col = col
            break
    if date_col is None:
        raise ValueError("Nenhuma coluna de data/hora encontrada.")
    
    # Renomeia para os nomes padrão
    rename_dict = {
        device_col: 'device_id',
        temp_col: 'temperature',
        date_col: 'timestamp'
    }
    df.rename(columns=rename_dict, inplace=True)
    
    # Converte timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df.dropna(subset=['timestamp'], inplace=True)
    
    # Mantém apenas as colunas necessárias
    df = df[['device_id', 'temperature', 'timestamp']]
    
    print("Colunas finais:", df.columns.tolist())
    print("Primeiras 5 linhas:")
    print(df.head())
    
    print("Inserindo no banco...")
    df.to_sql("temperature_readings", engine, if_exists="replace", index=False)
    
    print("Criando views...")
    with engine.raw_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE OR REPLACE VIEW avg_temp_por_dispositivo AS
        SELECT device_id, AVG(temperature) AS avg_temp
        FROM temperature_readings
        GROUP BY device_id;
        """)
        cursor.execute("""
        CREATE OR REPLACE VIEW leituras_por_hora AS
        SELECT EXTRACT(HOUR FROM timestamp) AS hora,
               COUNT(*) AS contagem
        FROM temperature_readings
        GROUP BY hora
        ORDER BY hora;
        """)
        cursor.execute("""
        CREATE OR REPLACE VIEW temp_max_min_por_dia AS
        SELECT DATE(timestamp) AS data,
               MAX(temperature) AS temp_max,
               MIN(temperature) AS temp_min
        FROM temperature_readings
        GROUP BY data
        ORDER BY data;
        """)
        conn.commit()
    
    print("Pipeline finalizado com sucesso!")

if __name__ == "__main__":
    process_and_load()