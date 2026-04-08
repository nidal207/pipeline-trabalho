import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

DB_USER = "postgres"
DB_PASS = "1234"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "iot"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def load_data(view_name):
    try:
        with engine.raw_connection() as conn:
            return pd.read_sql(f"SELECT * FROM {view_name}", conn)
    except Exception as e:
        st.error(f"Erro ao carregar {view_name}: {e}")
        return pd.DataFrame()

st.title("📊 Dashboard de Temperaturas IoT")

# Gráfico 1
st.header("Média de Temperatura por Dispositivo")
df1 = load_data("avg_temp_por_dispositivo")
if not df1.empty:
    fig1 = px.bar(df1, x="device_id", y="avg_temp")
    st.plotly_chart(fig1)
else:
    st.warning("Sem dados para exibir.")

# Gráfico 2
st.header("Leituras por Hora")
df2 = load_data("leituras_por_hora")
if not df2.empty:
    fig2 = px.line(df2, x="hora", y="contagem")
    st.plotly_chart(fig2)
else:
    st.warning("Sem dados para exibir.")

# Gráfico 3
st.header("Temperaturas Máximas e Mínimas por Dia")
df3 = load_data("temp_max_min_por_dia")
if not df3.empty:
    fig3 = px.line(df3, x="data", y=["temp_max", "temp_min"])
    st.plotly_chart(fig3)
else:
    st.warning("Sem dados para exibir.")