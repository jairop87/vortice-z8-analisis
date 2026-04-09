import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd
import requests
import io
import gc

# --- 1. CONFIGURACIÓN E INTERFAZ ---
st.set_page_config(page_title="VORTICE-8 | Inteligencia Z8", layout="wide", page_icon="👮")

# Estética Táctica (Modo Comando)
st.markdown("""
    <style>
    .main { background-color: #0d1117; color: #c9d1d9; }
    .stMetric { background-color: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 15px; }
    div[data-testid="stMetricValue"] { color: #00ffcc; font-family: 'Courier New', monospace; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. CONFIGURACIÓN DEL DATA LAKE (HUGGING FACE) ---
HF_BASE = "https://huggingface.co/datasets/JAIRO1987/vortice-z8-data/resolve/main/"

# Función para cargar datos SOLAMENTE cuando se necesitan (Lazy Loading)
def load_specific_data(table_names):
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # Mapeo de archivos específicos
    MAPEO = {
        "usuarios": "usuarios.parquet",
        "personas": "personas.parquet",
        "veh_reg": "vehiculos.parquet",
        "evento": "5%20EVENTO%20PEGAR%20HOJA.parquet",
        "violencia": "1%20VIOLENCIA.parquet",
        "delincuencia": "2%20DELINCUENCIA.parquet",
        "ecu911": "ECU911.parquet"
    }
    
    with st.spinner("🛰️ Sincronizando datos con Hugging Face..."):
        for name in table_names:
            if name in MAPEO:
                url = f"{HF_BASE}{MAPEO[name]}"
                resp = requests.get(url, headers=headers)
                if resp.status_code == 200:
                    df = pd.read_parquet(io.BytesIO(resp.content))
                    con.register(f"t_{name}", df)
                    # No borramos el df aquí para que DuckDB pueda consultarlo
                else:
                    st.error(f"Fallo al conectar con {name}")
    return con

# --- 3. NAVEGACIÓN LATERAL ---
st.sidebar.title("🛡️ VORTICE-8")
st.sidebar.caption("Ingeniería de Sistemas | Zona 8")
menu = st.sidebar.radio("Módulos de Inteligencia", [
    "🏠 Inicio",
    "📱 Operatividad SIIPNE Móvil",
    "🗺️ Geo-Inteligencia (Mapas)",
    "💀 Análisis de Violencia",
    "👤 Delincuencia y Eventos",
    "🧠 Análisis Cuántico (Cruces)"
])

# --- 4. LÓGICA DE MÓDULOS (ON-DEMAND) ---

if menu == "🏠 Inicio":
    st.title("🛡️ VORTICE-8 | Plataforma de Inteligencia Policial")
    st.markdown("""
    ---
    ### Bienvenido, Ingeniero Peso.
    Este sistema opera bajo una arquitectura **Antigravity de carga bajo demanda**. 
    Los datos no se cargarán en la memoria del servidor hasta que usted seleccione un módulo específico en el menú lateral.
    
    **Capacidades Actuales:**
    * **SIIPNE Móvil:** Vinculación Maestro-Detalle por ID OPERATIVO.
    * **Geo-Inteligencia:** Cruce de coordenadas de operativos vs delincuencia.
    * **Cruce Cuántico:** Correlación llamadas ECU911 vs judicialización real.
    """)

elif menu == "📱 Operatividad SIIPNE Móvil":
    st.title("📱 Inteligencia SIIPNE Móvil")
    # Cargamos solo lo necesario para este módulo
    con = load_specific_data(["usuarios", "personas", "veh_reg"])
    
    c1, c2, c3 = st.columns(3)
    ops = con.execute("SELECT COUNT(*) FROM t_usuarios").fetchone()[0]
    pers = con.execute("SELECT COUNT(*) FROM t_personas").fetchone()[0]
    vehs = con.execute("SELECT COUNT(*) FROM t_veh_reg").fetchone()[0]
    
    c1.metric("Operativos Realizados", f"{ops:,}")
    c2.metric("Personas Registradas", f"{pers:,}")
    c3.metric("Vehículos Registrados", f"{vehs:,}")

    st.subheader("📊 Productividad por Agente (Top 10)")
    df_agente = con.execute("""
        SELECT "QUIEN HACE OPERATIVO" as Agente, COUNT(*) as n 
        FROM t_usuarios GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).df()
    st.plotly_chart(px.bar(df_agente, x='n', y='Agente', orientation='h', template="plotly_dark"), width='stretch')
    gc.collect()

elif menu == "🗺️ Geo-Inteligencia (Mapas)":
    st.title("🗺️ Análisis Espacial de lo No Percibido")
    con = load_specific_data(["usuarios", "delincuencia", "violencia"])
    
    df_map = con.execute("""
        SELECT CAST(LATITUD AS FLOAT) as lat, CAST(LONGITUD AS FLOAT) as lon, 'Operativo SIIPNE' as tipo FROM t_usuarios
        UNION ALL
        SELECT CAST(LATITUD AS FLOAT) as lat, CAST(LONGITUD AS FLOAT) as lon, 'Delito' as tipo FROM t_delincuencia
        UNION ALL
        SELECT CAST(COORD_X AS FLOAT) as lat, CAST(COORD_Y AS FLOAT) as lon, 'Violencia' as tipo FROM t_violencia
    """).df()
    
    fig_map = px.scatter_mapbox(df_map, lat="lat", lon="lon", color="tipo", 
                                mapbox_style="carto-darkmatter", zoom=10, 
                                center={"lat": -2.19, "lon": -79.88}, template="plotly_dark")
    st.plotly_chart(fig_map, width='stretch')
    gc.collect()

elif menu == "💀 Análisis de Violencia":
    st.title("💀 Análisis Crítico de Muertes Violentas")
    con = load_specific_data(["violencia"])
    
    df_v = con.execute("SELECT DISTRITO, TIPO_ARMA, COUNT(*) as n FROM t_violencia GROUP BY ALL").df()
    st.plotly_chart(px.bar(df_v, x='DISTRITO', y='n', color='TIPO_ARMA', barmode='group', template="plotly_dark"), width='stretch')
    gc.collect()

elif menu == "👤 Delincuencia y Eventos":
    st.title("👤 Caracterización del Delito y ECU911")
    con = load_specific_data(["delincuencia", "ecu911"])
    
    df_ecu = con.execute("SELECT \"Tipo de Incidente\" as tipo, COUNT(*) as n FROM t_ecu911 GROUP BY 1 ORDER BY n DESC").df()
    st.plotly_chart(px.treemap(df_ecu, path=['tipo'], values='n', template="plotly_dark"), width='stretch')
    gc.collect()

elif menu == "🧠 Análisis Cuántico (Cruces)":
    st.title("🧠 Correlación de Productividad SIIPNE")
    con = load_specific_data(["usuarios", "personas", "delincuencia"])
    
    df_quantum = con.execute("""
        SELECT u.DISTRITO, 
               COUNT(p.CEDULA) as Antecedentes_Revisados,
               (SELECT COUNT(*) FROM t_delincuencia d WHERE d.DISTRITO = u.DISTRITO) as Delitos
        FROM t_usuarios u
        LEFT JOIN t_personas p ON u."ID OPERATIVO" = p."ID OPERATIVO"
        GROUP BY u.DISTRITO
    """).df()
    
    fig_q = px.scatter(df_quantum, x='Antecedentes_Revisados', y='Delitos', text='DISTRITO',
                       size='Delitos', color='Antecedentes_Revisados', template="plotly_dark")
    st.plotly_chart(fig_q, width='stretch')
    gc.collect()

st.sidebar.markdown("---")
st.sidebar.caption("SISTEMA VORTICE-8 | v3.0")
