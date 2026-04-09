import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd
import requests
import io

# --- 1. CONFIGURACIÓN TÁCTICA ---
st.set_page_config(page_title="VORTICE-8 | SIIPNE Inteligencia", layout="wide", page_icon="🛡️")

# Estilo Antigravity (Modo Comando)
st.markdown("""
    <style>
    .main { background-color: #0d1117; color: #c9d1d9; }
    .stMetric { background-color: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 15px; }
    div[data-testid="stMetricValue"] { color: #00ffcc; font-family: 'Courier New', monospace; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. DATA LAKE: HUGGING FACE (JAIRO1987/vortice-z8-data) ---
HF_URL = "https://huggingface.co/datasets/JAIRO1987/vortice-z8-data/resolve/main/"

# ADN de Datos: Mapeo de archivos (Nombres exactos según tu repositorio)
MAPEO_HF = {
    "usuarios": "usuarios.parquet",        # MAESTRA OPERATIVOS
    "check_personas": "personas.parquet", # VINCULADA POR ID OPERATIVO
    "check_vehiculos": "vehiculos.parquet",# VINCULADA POR ID OPERATIVO
    "evento": "5%20EVENTO%20PEGAR%20HOJA.parquet",
    "violencia": "1%20VIOLENCIA.parquet",
    "delincuencia": "2%20DELINCUENCIA.parquet",
    "ecu911": "ECU911.parquet",
    "armas": "1%20ARMAS%20PEGAR%20HOJA.parquet",
    "detenidos": "2%20DETENIDOS%20PEGAR%20HOJA.parquet"
}

@st.cache_resource
def load_vortice_cloud():
    con = duckdb.connect(database=':memory:')
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        for tabla, file in MAPEO_HF.items():
            url = f"{HF_URL}{file}"
            # Creamos VISTAS: Eficiencia pura de red
            con.execute(f"CREATE OR REPLACE VIEW t_{tabla} AS SELECT * FROM read_parquet('{url}')")
        return con, True
    except Exception as e:
        return con, str(e)

con, status = load_vortice_cloud()

# --- 3. MENÚ DE OPERACIONES ---
st.sidebar.title("🛡️ VORTICE-8 Z8")
menu = st.sidebar.radio("Módulos", [
    "📌 Dashboard SIIPNE Móvil", 
    "🗺️ Geo-Inteligencia (Mapas)", 
    "💀 Análisis Crítico (Violencia)", 
    "🧠 Patrones Cuánticos (Cruces)"
])

if status is not True:
    st.error(f"Error de Conexión: {status}")
else:
    # --- NIVEL 1: SIIPNE MÓVIL (MAESTRO-DETALLE) ---
    if menu == "📌 Dashboard SIIPNE Móvil":
        st.title("📱 Control de Operatividad SIIPNE Móvil")
        st.info("Lógica de Vinculación: Usuarios -> Personas/Vehículos por ID OPERATIVO")
        
        c1, c2, c3 = st.columns(3)
        
        # Consultas Relacionales Rápidas
        ops = con.execute("SELECT COUNT(*) FROM t_usuarios").fetchone()[0]
        pers = con.execute("SELECT COUNT(*) FROM t_check_personas").fetchone()[0]
        vehs = con.execute("SELECT COUNT(*) FROM t_check_vehiculos").fetchone()[0]
        
        c1.metric("Operativos Realizados", f"{ops:,}")
        c2.metric("Personas Registradas", f"{pers:,}")
        c3.metric("Vehículos Registrados", f"{vehs:,}")

        st.subheader("📊 Productividad por Unidad / Distrito")
        df_prod = con.execute("""
            SELECT DISTRITO, 
                   COUNT(DISTINCT \"QUIEN HACE OPERATIVO\") as Agentes,
                   COUNT(*) as Total_Registros
            FROM t_usuarios 
            GROUP BY DISTRITO ORDER BY Total_Registros DESC
        """).df()
        st.plotly_chart(px.bar(df_prod, x='DISTRITO', y='Total_Registros', color='Agentes', template="plotly_dark"), width='stretch')

    # --- NIVEL 2: GEO-INTELIGENCIA (PATRONES OCULTOS) ---
    elif menu == "🗺️ Geo-Inteligencia (Mapas)":
        st.title("🛰️ Análisis Espacial de lo No Percibido")
        st.write("Visualización de coordenadas de SIIPNE Móvil vs Eventos Delictivos.")
        
        # Capa 1: Operativos (Mapa de Calor)
        df_geo = con.execute("""
            SELECT CAST(LATITUD AS FLOAT) as lat, CAST(LONGITUD AS FLOAT) as lon, 'Operativo' as fuente FROM t_usuarios
            UNION ALL
            SELECT CAST(LATITUD AS FLOAT) as lat, CAST(LONGITUD AS FLOAT) as lon, 'Delito' as fuente FROM t_delincuencia
        """).df()
        
        fig_map = px.density_mapbox(df_geo, lat='lat', lon='lon', z=None, radius=10,
                                    center=dict(lat=-2.19, lon=-79.88), zoom=10,
                                    mapbox_style="carto-darkmatter", template="plotly_dark",
                                    title="Concentración de Actividad Operativa vs Delincuencia")
        st.plotly_chart(fig_map, width='stretch')
        st.markdown("> **Análisis Cuántico:** Las zonas con alto brillo de 'Delito' y bajo brillo de 'Operativo' son tus puntos ciegos de patrullaje.")

    # --- NIVEL 3: ANÁLISIS CRÍTICO (VIOLENCIA) ---
    elif menu == "💀 Análisis Crítico (Violencia)":
        st.title("💀 Muertes Violentas Zona 8")
        
        # Cruce de Muertes con Tipo de Arma
        df_v = con.execute("SELECT DISTRITO, TIPO_ARMA, COUNT(*) as n FROM t_violencia GROUP BY ALL").df()
        st.plotly_chart(px.bar(df_v, x='DISTRITO', y='n', color='TIPO_ARMA', barmode='group', template="plotly_dark"), width='stretch')

    # --- NIVEL 4: PATRONES CUÁNTICOS (VINCULACIÓN TOTAL) ---
    elif menu == "🧠 Patrones Cuánticos (Cruces)":
        st.title("🧠 Correlación de Productividad Preventiva")
        st.write("¿La cantidad de registros de Personas/Vehículos reduce realmente el delito?")
        
        # JOIN Multinivel: Usuarios vs Delincuencia por Distrito
        df_quantum = con.execute("""
            SELECT u.DISTRITO, 
                   COUNT(p.CEDULA) as Registros_Personas,
                   (SELECT COUNT(*) FROM t_delincuencia d WHERE d.DISTRITO = u.DISTRITO) as Delitos
            FROM t_usuarios u
            LEFT JOIN t_check_personas p ON u.\"ID OPERATIVO\" = p.\"ID OPERATIVO\"
            GROUP BY u.DISTRITO
        """).df()
        
        fig_q = px.scatter(df_quantum, x='Registros_Personas', y='Delitos', text='DISTRITO', 
                           size='Delitos', color='Registros_Personas', template="plotly_dark",
                           title="Productividad SIIPNE vs Incidencia Delictiva")
        st.plotly_chart(fig_q, width='stretch')

st.sidebar.markdown("---")
st.sidebar.caption("SISTEMA VORTICE-8 | Ing. Jairo Peso")
