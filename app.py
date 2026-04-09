import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import requests
import io
import gc

# --- CONFIGURACIÓN DE INTERFAZ ---
st.set_page_config(page_title="VORTICE-8 | Inteligencia Integral", layout="wide", page_icon="🛡️")

# Estética Táctica
st.markdown("""
    <style>
    .main { background-color: #0d1117; color: #c9d1d9; }
    .stMetric { background-color: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 15px; }
    div[data-testid="stMetricValue"] { color: #00ffcc; font-family: 'monospace'; }
    </style>
    """, unsafe_allow_html=True)

# --- REPOSITORIO HUGGING FACE ---
HF_USER = "JAIRO1987"
HF_REPO = "vortice-z8-data"

# Mapeo de archivos (ADN de datos actualizado)
ARCHIVOS = {
    "violencia": "1%20VIOLENCIA.parquet",
    "delincuencia": "2%20DELINCUENCIA.parquet",
    "ecu911": "ECU911.parquet",
    "evento": "5%20EVENTO%20PEGAR%20HOJA.parquet"
}

@st.cache_resource
def load_vortice_engine():
    con = duckdb.connect(database=':memory:')
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        for tabla, file in ARCHIVOS.items():
            url = f"https://huggingface.co/datasets/{HF_USER}/{HF_REPO}/resolve/main/{file}"
            # Creamos tablas físicas para máxima velocidad en cruces
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                df_tmp = pd.read_parquet(io.BytesIO(resp.content))
                con.register("df_tmp", df_tmp)
                con.execute(f"CREATE OR REPLACE TABLE t_{tabla} AS SELECT * FROM df_tmp")
                del df_tmp
                gc.collect()
        return con, True
    except Exception as e:
        return con, str(e)

con, status = load_vortice_engine()

# --- NAVEGACIÓN LATERAL ---
st.sidebar.title("🛡️ VORTICE-8")
menu = st.sidebar.radio("Módulos de Inteligencia", [
    "📌 Visión Global Z8", 
    "💀 Análisis de Violencia", 
    "👤 Delincuencia Común", 
    "📞 Eventos ECU911",
    "🧠 Análisis Cuántico (Cruces)"
])

if status is not True:
    st.error(f"Error de Ingesta: {status}")
else:
    # --- 1. VISIÓN GLOBAL ---
    if menu == "📌 Visión Global Z8":
        st.title("📈 Dashboard Ejecutivo Zona 8")
        
        c1, c2, c3 = st.columns(3)
        muertes = con.execute("SELECT COUNT(*) FROM t_violencia").fetchone()[0]
        delitos = con.execute("SELECT COUNT(*) FROM t_delincuencia").fetchone()[0]
        llamadas = con.execute("SELECT COUNT(*) FROM t_ecu911").fetchone()[0]
        
        c1.metric("Muertes Violentas", f"{muertes:,}", delta_color="inverse")
        c2.metric("Delincuencia Reportada", f"{delitos:,}")
        c3.metric("Alertas ECU911", f"{llamadas:,}")

        st.subheader("📊 Línea de Tiempo Unificada")
        # SQL para unir fechas de distintas fuentes
        df_line = con.execute("""
            SELECT FECHA_INFRACCION as fecha, 'Violencia' as tipo FROM t_violencia
            UNION ALL
            SELECT FECHA_INFRACCION as fecha, 'Delincuencia' as tipo FROM t_delincuencia
            UNION ALL
            SELECT CAST(Fecha AS DATE) as fecha, 'ECU911' as tipo FROM t_ecu911
        """).df()
        fig_line = px.histogram(df_line, x="fecha", color="tipo", barmode="group", template="plotly_dark")
        st.plotly_chart(fig_line, width='stretch')

    # --- 2. ANÁLISIS DE VIOLENCIA ---
    elif menu == "💀 Análisis de Violencia":
        st.title("💀 Análisis de Muertes Violentas")
        col_l, col_r = st.columns(2)
        
        with col_l:
            st.write("### Armas Utilizadas")
            df_arma = con.execute("SELECT ARMA, COUNT(*) as n FROM t_violencia GROUP BY 1 ORDER BY n DESC").df()
            st.plotly_chart(px.pie(df_arma, names='ARMA', values='n', hole=0.4, template="plotly_dark"), width='stretch')
            
        with col_r:
            st.write("### Tipo de Muerte por Distrito")
            df_dist_v = con.execute("SELECT DISTRITO, TIPO_MUERTE, COUNT(*) as n FROM t_violencia GROUP BY 1, 2").df()
            st.plotly_chart(px.bar(df_dist_v, x='DISTRITO', y='n', color='TIPO_MUERTE', template="plotly_dark"), width='stretch')

    # --- 3. DELINCUENCIA COMÚN ---
    elif menu == "👤 Delincuencia Común":
        st.title("👤 Análisis del Delito")
        
        df_delito = con.execute("""
            SELECT DELITO, COUNT(*) as n 
            FROM t_delincuencia 
            GROUP BY 1 ORDER BY n DESC LIMIT 15
        """).df()
        st.plotly_chart(px.bar(df_delito, x='n', y='DELITO', orientation='h', title="Top Delitos Reportados", template="plotly_dark"), width='stretch')
        
        st.write("### Modalidad y Movilidad")
        df_mod = con.execute("SELECT MODALIDAD, MOVILIDAD_VICTIMARIO, COUNT(*) as n FROM t_delincuencia GROUP BY 1, 2 ORDER BY n DESC LIMIT 20").df()
        st.plotly_chart(px.scatter(df_mod, x='MODALIDAD', y='MOVILIDAD_VICTIMARIO', size='n', color='n', template="plotly_dark"), width='stretch')

    # --- 4. ECU911 ---
    elif menu == "📞 Eventos ECU911":
        st.title("📞 Análisis de Emergencias (ECU911)")
        
        df_ecu = con.execute("SELECT \"Tipo de Incidente\" as tipo, COUNT(*) as n FROM t_ecu911 GROUP BY 1 ORDER BY n DESC").df()
        st.plotly_chart(px.treemap(df_ecu, path=['tipo'], values='n', title="Distribución de Alertas ECU911"), width='stretch')

    # --- 5. ANÁLISIS CUÁNTICO (CRUCES) ---
    elif menu == "🧠 Análisis Cuántico (Cruces)":
        st.title("🧠 Patrones No Percibidos: Cruce de Datos")
        st.info("Este módulo busca la correlación entre las llamadas al ECU911 y la delincuencia efectiva.")
        
        # Correlación entre llamadas de auxilio y delitos reales por distrito
        df_correl = con.execute("""
            SELECT e.Distrito, 
                   COUNT(DISTINCT e."Fecha y Hora") as Alertas_ECU,
                   (SELECT COUNT(*) FROM t_delincuencia d WHERE d.DISTRITO = e.Distrito) as Delitos_Reales
            FROM t_ecu911 e
            GROUP BY e.Distrito
        """).df()
        
        fig_correl = px.scatter(df_correl, x='Alertas_ECU', y='Delitos_Reales', text='Distrito', 
                                size='Delitos_Reales', color='Distrito', template="plotly_dark",
                                title="Llamadas ECU911 vs Delitos Efectivos")
        st.plotly_chart(fig_correl, width='stretch')
        
        st.markdown("""
        > **Interpretación:** Los distritos que se alejan de la línea diagonal indican una brecha entre la percepción ciudadana (llamadas) 
        > y la judicialización (delitos reportados).
        """)

st.sidebar.markdown("---")
st.sidebar.caption("VORTICE-8 | v2.0 Integral")
