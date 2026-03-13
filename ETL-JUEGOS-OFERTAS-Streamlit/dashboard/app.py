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
# CONEXIÓN A POSTGRES
# -----------------------------
engine = create_engine(
    "postgresql://postgres:1221@localhost:5432/juegos_ofertas_etl"
)

@st.cache_data
def load_data():
    query = "SELECT * FROM deals"
    df = pd.read_sql(query, engine)
    return df

df = load_data()

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

precio_max = st.sidebar.slider(
    "Precio máximo ($)",
    0,
    int(df["precio_oferta"].max()),
    int(df["precio_oferta"].max())
)

rating_min = st.sidebar.slider(
    "Rating mínimo Steam",
    0,
    100,
    0
)

buscar = st.sidebar.text_input("🔎 Buscar juego")

df = df[
    (df["precio_oferta"] <= precio_max) &
    (df["rating_steam"] >= rating_min)
]

if buscar:
    df = df[df["titulo"].str.contains(buscar, case=False)]

# -----------------------------
# KPIs
# -----------------------------
col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "🎮 Juegos",
    len(df)
)

col2.metric(
    "💸 Descuento promedio",
    f"{round(df['ahorro_porcentaje'].mean(),2)}%"
)

col3.metric(
    "💲 Precio promedio",
    f"${round(df['precio_oferta'].mean(),2)}"
)

col4.metric(
    "⭐ Rating promedio",
    f"{round(df['rating_steam'].mean(),2)}%"
)

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