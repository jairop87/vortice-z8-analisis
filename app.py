import streamlit as st
import duckdb
import plotly.express as px
import requests

# --- 1. CONFIGURACIÓN E INTERFAZ ---
st.set_page_config(page_title="VORTICE-8 | Inteligencia Z8", layout="wide", page_icon="🛡️")

# CSS Táctico para limpiar logs y mejorar visual
st.markdown("""
    <style>
    .main { background-color: #0d1117; color: #c9d1d9; }
    .stMetric { background-color: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 15px; }
    div[data-testid="stMetricValue"] { color: #00ffcc; font-family: 'monospace'; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. CONFIGURACIÓN DE DATA LAKE (HUGGING FACE) ---
HF_USER = "JAIRO1987"
HF_REPO = "vortice-z8-data"

# Mapeo de ADN de datos (Nombres de archivos en HF)
ARCHIVOS = {
    "evento": "5%20EVENTO%20PEGAR%20HOJA.parquet",
    "violencia": "1%20VIOLENCIA.parquet",
    "delincuencia": "2%20DELINCUENCIA.parquet",
    "ecu911": "ECU911.parquet",
    "detenidos": "2%20DETENIDOS%20PEGAR%20HOJA.parquet"
}

@st.cache_resource
def init_engine():
    con = duckdb.connect(database=':memory:')
    try:
        # Usamos httpfs para leer por partes (Range Requests) - Esto ahorra un 80% de RAM
        con.execute("INSTALL httpfs; LOAD httpfs;")
        for tabla, file in ARCHIVOS.items():
            url = f"https://huggingface.co/datasets/{HF_USER}/{HF_REPO}/resolve/main/{file}"
            # Creamos VISTAS en lugar de tablas físicas para no saturar la RAM
            con.execute(f"CREATE OR REPLACE VIEW t_{tabla} AS SELECT * FROM read_parquet('{url}')")
        return con, True
    except Exception as e:
        return con, str(e)

con, status = init_engine()

# --- 3. MENÚ DE OPERACIONES ---
st.sidebar.title("🛡️ VORTICE-8")
menu = st.sidebar.radio("Módulos de Análisis", [
    "📌 Visión Estratégica Z8", 
    "💀 Muertes Violentas", 
    "👤 Delincuencia Común", 
    "📞 Emergencias ECU911",
    "🧠 Análisis Cuántico (Cruces)"
])

if status is not True:
    st.error(f"🚨 Error en Data Lake: {status}")
else:
    # --- NIVEL 1: VISIÓN ESTRATÉGICA ---
    if menu == "📌 Visión Estratégica Z8":
        st.title("📈 Dashboard Ejecutivo - Zona 8")
        
        c1, c2, c3 = st.columns(3)
        try:
            m_v = con.execute("SELECT COUNT(*) FROM t_violencia").fetchone()[0]
            d_c = con.execute("SELECT COUNT(*) FROM t_delincuencia").fetchone()[0]
            e_9 = con.execute("SELECT COUNT(*) FROM t_ecu911").fetchone()[0]
            
            c1.metric("Muertes Violentas", f"{m_v:,}")
            c2.metric("Delincuencia", f"{d_c:,}")
            c3.metric("Alertas ECU911", f"{e_9:,}")
        except:
            st.warning("Cargando metadatos iniciales...")

        st.subheader("📊 Línea de Tiempo Multi-Fuente")
        df_time = con.execute("""
            SELECT FECHA_INFRACCION as fecha, 'Violencia' as tipo FROM t_violencia
            UNION ALL
            SELECT FECHA_INFRACCION as fecha, 'Delincuencia' as tipo FROM t_delincuencia
        """).df()
        fig_time = px.histogram(df_time, x="fecha", color="tipo", template="plotly_dark", barmode="group")
        st.plotly_chart(fig_time, width='stretch')

    # --- NIVEL 2: MUERTES VIOLENTAS ---
    elif menu == "💀 Muertes Violentas":
        st.title("💀 Análisis de Violencia Crítica")
        col1, col2 = st.columns(2)
        
        df_armas = con.execute("SELECT TIPO_ARMA, COUNT(*) as n FROM t_violencia GROUP BY 1 ORDER BY n DESC").df()
        col1.write("### Armas del Delito")
        col1.plotly_chart(px.pie(df_armas, names='TIPO_ARMA', values='n', hole=0.4, template="plotly_dark"), width='stretch')
        
        df_dist = con.execute("SELECT DISTRITO, COUNT(*) as n FROM t_violencia GROUP BY 1 ORDER BY n DESC").df()
        col2.write("### Muertes por Distrito")
        col2.plotly_chart(px.bar(df_dist, x='n', y='DISTRITO', orientation='h', template="plotly_dark"), width='stretch')

    # --- NIVEL 3: DELINCUENCIA COMÚN ---
    elif menu == "👤 Delincuencia Común":
        st.title("👤 Caracterización del Delito")
        df_delito = con.execute("SELECT DELITO, COUNT(*) as n FROM t_delincuencia GROUP BY 1 ORDER BY n DESC LIMIT 15").df()
        st.plotly_chart(px.bar(df_delito, x='n', y='DELITO', orientation='h', color='n', template="plotly_dark"), width='stretch')

    # --- NIVEL 4: ECU911 ---
    elif menu == "📞 Emergencias ECU911":
        st.title("📞 Dinámica de Auxilio (ECU911)")
        df_ecu = con.execute("SELECT \"Tipo de Incidente\" as tipo, COUNT(*) as n FROM t_ecu911 GROUP BY 1 ORDER BY n DESC").df()
        st.plotly_chart(px.treemap(df_ecu, path=['tipo'], values='n', template="plotly_dark"), width='stretch')

    # --- NIVEL 5: ANÁLISIS CUÁNTICO ---
    elif menu == "🧠 Análisis Cuántico (Cruces)":
        st.title("🧠 Correlaciones No Percibidas")
        st.info("Cruce de Llamadas ECU911 vs Delincuencia Real por Distrito.")
        
        df_cross = con.execute("""
            SELECT e.Distrito, 
                   COUNT(DISTINCT e."Fecha y Hora") as Alertas_ECU,
                   (SELECT COUNT(*) FROM t_delincuencia d WHERE d.DISTRITO = e.Distrito) as Delitos_Reales
            FROM t_ecu911 e
            GROUP BY e.Distrito
        """).df()
        
        fig_cross = px.scatter(df_cross, x='Alertas_ECU', y='Delitos_Reales', text='Distrito', 
                               size='Delitos_Reales', color='Alertas_ECU', template="plotly_dark")
        st.plotly_chart(fig_cross, width='stretch')

st.sidebar.markdown("---")
st.sidebar.caption("SISTEMA VORTICE-8 | v2.0")
