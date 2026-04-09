import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd
import gc

# --- 1. CONFIGURACIÓN E INTERFAZ ---
st.set_page_config(page_title="VORTICE-8 | Inteligencia Z8", layout="wide", page_icon="👮")

# Estética Táctica (Dark Mode)
st.markdown("""
    <style>
    .main { background-color: #0d1117; color: #c9d1d9; }
    .stMetric { background-color: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 15px; }
    div[data-testid="stMetricValue"] { color: #00ffcc; font-family: 'monospace'; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. DATA LAKE: HUGGING FACE (JAIRO1987/vortice-z8-data) ---
HF_BASE = "https://huggingface.co/datasets/JAIRO1987/vortice-z8-data/resolve/main/"

# Diccionario Maestro de los 14 archivos Parquet
MAPEO_DATA = {
    "usuarios": "usuarios.parquet",        # MAESTRA SIIPNE
    "pers_reg": "personas.parquet",        # VINCULADA
    "veh_reg": "vehiculos.parquet",         # VINCULADA
    "violencia": "1%20VIOLENCIA.parquet",
    "delincuencia": "2%20DELINCUENCIA.parquet",
    "evento": "5%20EVENTO%20PEGAR%20HOJA.parquet",
    "ecu911": "ECU911.parquet",
    "detenidos": "2%20DETENIDOS%20PEGAR%20HOJA.parquet",
    "armas": "1%20ARMAS%20PEGAR%20HOJA.parquet",
    "veh_hoja": "3%20VEHICULOS%20PEGAR%20HOJA.parquet",
    "indicios": "4%20INDICIOS%20PEGAR%20HOJA.parquet",
    "personal": "6%20PERSONAL%20POLICIAL%20PEGAR%20HOJA.parquet",
    "objetos": "10%20OBJETOS.parquet",
    "desaparecidos": "11%20DESAPARECIDOS.parquet"
}

@st.cache_resource
def init_engine():
    con = duckdb.connect(database=':memory:')
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("SET http_keep_alive=false;")
        # Creamos VISTAS perezosas (Lazy Loading) para no saturar la RAM
        for tabla, file in MAPEO_DATA.items():
            url = f"{HF_BASE}{file}"
            con.execute(f"CREATE OR REPLACE VIEW t_{tabla} AS SELECT * FROM read_parquet('{url}')")
        return con, True
    except Exception as e:
        return con, str(e)

con, status = init_engine()

# --- 3. NAVEGACIÓN TÁCTICA ---
st.sidebar.title("🛡️ VORTICE-8")
menu = st.sidebar.radio("Módulos de Inteligencia", [
    "📱 Operatividad SIIPNE Móvil",
    "🗺️ Geo-Inteligencia (Mapas)",
    "💀 Análisis Crítico (Violencia)",
    "📞 Emergencias ECU911",
    "🧠 Análisis Cuántico (Cruces)"
])

if status is not True:
    st.error(f"🚨 Error en el Data Lake: {status}")
else:
    # --- NIVEL 1: SIIPNE MÓVIL (RELACIONAL) ---
    if menu == "📱 Operatividad SIIPNE Móvil":
        st.title("📱 Inteligencia SIIPNE Móvil - Zona 8")
        st.info("Estructura: Usuarios (Master) ➡️ Personas/Vehículos (Detalle) via ID OPERATIVO")

        c1, c2, c3 = st.columns(3)
        # Consultas rápidas sobre vistas
        ops_t = con.execute("SELECT COUNT(*) FROM t_usuarios").fetchone()[0]
        per_t = con.execute("SELECT COUNT(*) FROM t_pers_reg").fetchone()[0]
        veh_t = con.execute("SELECT COUNT(*) FROM t_veh_reg").fetchone()[0]

        c1.metric("Operativos Realizados", f"{ops_t:,}")
        c2.metric("Personas Registradas", f"{per_t:,}")
        c3.metric("Vehículos Registrados", f"{veh_t:,}")

        st.subheader("📊 Productividad por Agente/Unidad")
        df_agente = con.execute("""
            SELECT "QUIEN HACE OPERATIVO" as Agente, COUNT(*) as Registros 
            FROM t_usuarios GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """).df()
        st.plotly_chart(px.bar(df_agente, x='Registros', y='Agente', orientation='h', template="plotly_dark"), width='stretch')

    # --- NIVEL 2: GEO-INTELIGENCIA (MAPAS) ---
    elif menu == "🗺️ Geo-Inteligencia (Mapas)":
        st.title("🛰️ Análisis Espacial de lo No Percibido")
        st.write("Cruce de coordenadas: SIIPNE Móvil vs Delincuencia vs Violencia.")
        
        # Unificamos coordenadas de diferentes fuentes para el mapa
        df_map = con.execute("""
            SELECT CAST(LATITUD AS FLOAT) as lat, CAST(LONGITUD AS FLOAT) as lon, 'Operativo SIIPNE' as tipo FROM t_usuarios
            UNION ALL
            SELECT CAST(LATITUD AS FLOAT) as lat, CAST(LONGITUD AS FLOAT) as lon, 'Delincuencia' as tipo FROM t_delincuencia
            UNION ALL
            SELECT CAST(COORD_X AS FLOAT) as lat, CAST(COORD_Y AS FLOAT) as lon, 'Violencia' as tipo FROM t_violencia
        """).df()
        
        fig_map = px.scatter_mapbox(df_map, lat="lat", lon="lon", color="tipo", 
                                    mapbox_style="carto-darkmatter", zoom=10, 
                                    center={"lat": -2.19, "lon": -79.88}, template="plotly_dark")
        st.plotly_chart(fig_map, width='stretch')
        st.markdown("> **Interpretación:** Los vacíos de color 'Operativo' sobre zonas de 'Violencia' indican áreas de intervención urgente.")

    # --- NIVEL 3: VIOLENCIA ---
    elif menu == "💀 Análisis Crítico (Violencia)":
        st.title("💀 Muertes Violentas Zona 8")
        
        df_v = con.execute("SELECT DISTRITO, TIPO_ARMA, COUNT(*) as n FROM t_violencia GROUP BY ALL").df()
        st.plotly_chart(px.bar(df_v, x='DISTRITO', y='n', color='TIPO_ARMA', template="plotly_dark"), width='stretch')

    # --- NIVEL 4: ECU911 ---
    elif menu == "📞 Emergencias ECU911":
        st.title("📞 Dinámica de Auxilio (ECU911)")
        df_ecu = con.execute("SELECT \"Tipo de Incidente\" as tipo, COUNT(*) as n FROM t_ecu911 GROUP BY 1 ORDER BY n DESC").df()
        st.plotly_chart(px.treemap(df_ecu, path=['tipo'], values='n', template="plotly_dark"), width='stretch')

    # --- NIVEL 5: ANÁLISIS CUÁNTICO (VINCULACIÓN) ---
    elif menu == "🧠 Análisis Cuántico (Cruces)":
        st.title("🧠 Correlación de Productividad Preventiva")
        st.info("Módulo de Probabilidad: ¿La revisión de Personas/Vehículos impacta en la reducción del delito?")
        
        # JOIN Maestro-Detalle: Usuarios con Personas + Subconsulta de Delitos
        df_quantum = con.execute("""
            SELECT u.DISTRITO, 
                   COUNT(p.CEDULA) as Antecedentes_Revisados,
                   (SELECT COUNT(*) FROM t_delincuencia d WHERE d.DISTRITO = u.DISTRITO) as Delitos_Distrito
            FROM t_usuarios u
            LEFT JOIN t_pers_reg p ON u."ID OPERATIVO" = p."ID OPERATIVO"
            WHERE u.DISTRITO IS NOT NULL
            GROUP BY u.DISTRITO
        """).df()
        
        fig_q = px.scatter(df_quantum, x='Antecedentes_Revisados', y='Delitos_Distrito', text='DISTRITO',
                           size='Delitos_Distrito', color='Antecedentes_Revisados', template="plotly_dark")
        st.plotly_chart(fig_q, width='stretch')

# Garbage collector para limpiar RAM después de cada ejecución
gc.collect()
