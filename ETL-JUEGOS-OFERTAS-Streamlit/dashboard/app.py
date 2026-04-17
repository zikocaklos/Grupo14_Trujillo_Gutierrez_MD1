import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# -----------------------------
# CONFIGURACIÓN DE PÁGINA
# -----------------------------
st.set_page_config(
    page_title="Steam Deals Analytics",
    page_icon="🎮",
    layout="wide"
)

# -----------------------------
# ESTILO CSS PERSONALIZADO
# -----------------------------
st.markdown("""
<style>
.big-font {
    font-size:22px !important;
    font-weight:600;
}
.metric-box {
    background-color:#111;
    padding:20px;
    border-radius:10px;
}
</style>
""", unsafe_allow_html=True)

# -----------------------------
# CONEXIÓN A POSTGRES / SUPABASE
# -----------------------------
DATABASE_URL = st.secrets["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

@st.cache_data
def load_data():
    query = "SELECT * FROM deals"
    df = pd.read_sql(query, engine)
    return df

try:
    df = load_data()
except Exception as e:
    st.error("No se pudo conectar a la base de datos.")
    st.exception(e)
    st.stop()

# -----------------------------
# TÍTULO
# -----------------------------
st.title("🎮 Steam Deals Analytics Dashboard")
st.caption("Análisis de ofertas de videojuegos extraídos mediante ETL")

st.divider()

# -----------------------------
# SIDEBAR FILTROS
# -----------------------------
st.sidebar.title("🎛 Filtros")

if df.empty:
    st.warning("La tabla deals no tiene datos.")
    st.stop()

df["precio_oferta"] = pd.to_numeric(df["precio_oferta"], errors="coerce")
df["rating_steam"] = pd.to_numeric(df["rating_steam"], errors="coerce")
df["ahorro_porcentaje"] = pd.to_numeric(df["ahorro_porcentaje"], errors="coerce")
df["titulo"] = df["titulo"].fillna("")

precio_max_val = int(df["precio_oferta"].fillna(0).max()) if not df["precio_oferta"].dropna().empty else 0

precio_max = st.sidebar.slider(
    "Precio máximo ($)",
    0,
    precio_max_val,
    precio_max_val
)

rating_min = st.sidebar.slider(
    "Rating mínimo Steam",
    0,
    100,
    0
)

buscar = st.sidebar.text_input("🔎 Buscar juego")

df = df[
    (df["precio_oferta"].fillna(0) <= precio_max) &
    (df["rating_steam"].fillna(0) >= rating_min)
]

if buscar:
    df = df[df["titulo"].str.contains(buscar, case=False, na=False)]

# -----------------------------
# KPIs
# -----------------------------
col1, col2, col3, col4 = st.columns(4)

col1.metric("🎮 Juegos", len(df))
col2.metric("💸 Descuento promedio", f"{round(df['ahorro_porcentaje'].mean(), 2) if not df.empty else 0}%")
col3.metric("💲 Precio promedio", f"${round(df['precio_oferta'].mean(), 2) if not df.empty else 0}")
col4.metric("⭐ Rating promedio", f"{round(df['rating_steam'].mean(), 2) if not df.empty else 0}%")

st.divider()

# -----------------------------
# GRÁFICOS PRINCIPALES
# -----------------------------
col1, col2 = st.columns(2)

with col1:
    fig1 = px.histogram(
        df,
        x="ahorro_porcentaje",
        nbins=25,
        title="Distribución de descuentos",
        template="plotly_dark"
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.scatter(
        df,
        x="ahorro_porcentaje",
        y="rating_steam",
        hover_data=["titulo"],
        title="Relación Descuento vs Rating",
        template="plotly_dark"
    )
    st.plotly_chart(fig2, use_container_width=True)

# -----------------------------
# TOP OFERTAS
# -----------------------------
st.subheader("🔥 Top 10 Mejores Ofertas")

top = df.sort_values(
    by="ahorro_porcentaje",
    ascending=False
).head(10)

fig3 = px.bar(
    top,
    x="ahorro_porcentaje",
    y="titulo",
    orientation="h",
    title="Ranking de mejores descuentos",
    template="plotly_dark"
)

st.plotly_chart(fig3, use_container_width=True)

# -----------------------------
# PRECIO VS RATING
# -----------------------------
st.subheader("📊 Relación Precio vs Rating")

fig4 = px.scatter(
    df,
    x="precio_oferta",
    y="rating_steam",
    size="ahorro_porcentaje",
    hover_data=["titulo"],
    title="Relación precio, rating y descuento",
    template="plotly_dark"
)

st.plotly_chart(fig4, use_container_width=True)

# -----------------------------
# TABLA DE DATOS
# -----------------------------
st.subheader("📋 Datos completos")

st.dataframe(
    df.sort_values("ahorro_porcentaje", ascending=False),
    use_container_width=True
)

st.caption("Fuente de datos: API de ofertas de videojuegos procesada mediante pipeline ETL.")
