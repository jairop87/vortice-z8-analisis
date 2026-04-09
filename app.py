import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import requests
import io
import gc

# --- 1. CONFIGURACIÓN TÉCNICA ---
st.set_page_config(page_title="VORTICE-8 | Inteligencia Z8", layout="wide", page_icon="🚔")

# Estética Antigravity (Dark Mode Operativo)
st.markdown("""
    <style>
    .main { background-color: #0d1117; color: #c9d1d9; }
    .stMetric { background-color: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 15px; }
    [data-testid="stHeader"] { background: rgba(13,17,23,0.8); }
    </style>
    """, unsafe_allow_html=True)

# --- 2. DATA LAKE: MAPEADO DE SHAREPOINT ---
# IMPORTANTE: Reemplaza cada URL con el link individual CORRESPONDIENTE al archivo.
# No repitas links si quieres evitar que la memoria colapse (OOM).
DATA_LINKS = {
    "1 ARMAS PEGAR HOJA":"https://analisispolicial.sharepoint.com/:u:/g/IQByBqSIWwvlRb0C7-7yXgjbAfJvhZumoAkpRbv_tNc2t0s?e=lhDBAu?download=1"
"2 DETENIDOS PEGAR HOJA":"https://analisispolicial.sharepoint.com/:u:/g/IQByBqSIWwvlRb0C7-7yXgjbAfJvhZumoAkpRbv_tNc2t0s?e=lhDBAu?download=1"
"3 VEHICULOS PEGAR HOJA":"https://analisispolicial.sharepoint.com/:u:/g/IQByBqSIWwvlRb0C7-7yXgjbAfJvhZumoAkpRbv_tNc2t0s?e=lhDBAu?download=1"
}

# --- 3. MOTOR DE INGESTA (MEMORY SAFE) ---
@st.cache_resource
def init_engine():
    con = duckdb.connect(database=':memory:')
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    st.toast("🛰️ Conectando con el Data Lake de SharePoint...", icon="🌐")
    
    try:
        for name, url in DATA_LINKS.items():
            # Descarga vía Buffer para evitar HTTP 0 Error
            resp = requests.get(url, headers=headers, timeout=60)
            if resp.status_code == 200:
                # Lectura optimizada con PyArrow
                df_tmp = pd.read_parquet(io.BytesIO(resp.content))
                
                # Registro en DuckDB (Transforma el DataFrame en Tabla SQL física en memoria)
                con.execute(f"CREATE OR REPLACE TABLE t_{name} AS SELECT * FROM df_tmp")
                
                # Liberación agresiva de RAM
                del df_tmp
                gc.collect()
            else:
                return con, f"Error en {name}: SharePoint rechazó la conexión (Status {resp.status_code})"
        
        return con, True
    except Exception as e:
        return con, f"Fallo en Ingesta: {str(e)}"

con, status = init_engine()

# --- 4. INTERFAZ Y NAVEGACIÓN ---
st.sidebar.title("🛡️ VORTICE-8")
st.sidebar.caption("Ingeniería de Sistemas | Zona 8")
menu = st.sidebar.radio("Nivel Operativo", [
    "📌 Consolidado Zona 8", 
    "📍 Análisis por Distrito", 
    "📊 Productividad Operativa", 
    "🧠 Patrones Cuánticos",
    "☁️ Meteorología Delictiva"
])

if status is not True:
    st.error(f"🚨 Error Crítico: {status}")
    if st.button("🔄 Reintentar Conexión"):
        st.cache_resource.clear()
        st.rerun()
else:
    # --- NIVEL 1: CONSOLIDADO ---
    if menu == "📌 Consolidado Zona 8":
        st.title("📈 Dashboard Ejecutivo - Zona 8")
        
        # Consultas de alta velocidad
        m1, m2, m3 = st.columns(3)
        res_ev = con.execute("SELECT COUNT(*) FROM t_evento").fetchone()[0]
        res_det = con.execute("SELECT COUNT(*) FROM t_detenidos").fetchone()[0]
        
        m1.metric("Total Eventos", f"{res_ev:,}")
        m2.metric("Total Detenidos", f"{res_det:,}")
        m3.metric("Estatus Data Lake", "Sincronizado", delta="Antigravity ON")

        # Gráfico de Serie Temporal
        st.subheader("📅 Tendencia de Incidentes")
        df_time = con.execute("""
            SELECT CAST(FECHA_EVENTO AS DATE) as fecha, COUNT(*) as cantidad 
            FROM t_evento GROUP BY fecha ORDER BY fecha
        """).df()
        fig_time = px.area(df_time, x='fecha', y='cantidad', template="plotly_dark", color_discrete_sequence=['#00ffcc'])
        st.plotly_chart(fig_time, use_container_width=True)

    # --- NIVEL 2: ANÁLISIS POR DISTRITO ---
    elif menu == "📍 Análisis por Distrito":
        dist_list = con.execute("SELECT DISTINCT DISTRITO FROM t_evento WHERE DISTRITO IS NOT NULL").df()
        d_sel = st.sidebar.selectbox("Seleccione el Distrito", dist_list['DISTRITO'])
        
        st.title(f"🔍 Detalle Táctico: {d_sel}")
        
        col_l, col_r = st.columns(2)
        with col_l:
            st.write("### Delitos por Modalidad")
            df_mod = con.execute(f"SELECT MODALIDAD, COUNT(*) as n FROM t_evento WHERE DISTRITO = '{d_sel}' GROUP BY MODALIDAD").df()
            st.plotly_chart(px.pie(df_mod, names='MODALIDAD', values='n', hole=0.6, template="plotly_dark"), use_container_width=True)
        
        with col_r:
            st.write("### Top Circuitos con Mayor Incidencia")
            df_circ = con.execute(f"SELECT CIRCUITO, COUNT(*) as n FROM t_evento WHERE DISTRITO = '{d_sel}' GROUP BY CIRCUITO ORDER BY n DESC LIMIT 10").df()
            st.plotly_chart(px.bar(df_circ, x='n', y='CIRCUITO', orientation='h', template="plotly_dark", color='n'), use_container_width=True)

    # --- NIVEL 3: PRODUCTIVIDAD ---
    elif menu == "📊 Productividad Operativa":
        st.title("⚔️ Dinamismo: Delitos vs Detenciones")
        
        # JOIN Multinivel (Llave: N_EVENTO)
        df_prod = con.execute("""
            SELECT e.DISTRITO, 
                   COUNT(DISTINCT e.N_EVENTO) as Delitos,
                   COUNT(DISTINCT d.DOCUMENTO) as Detenidos
            FROM t_evento e
            LEFT JOIN t_detenidos d ON e.N_EVENTO = d.N_EVENTO
            GROUP BY e.DISTRITO
        """).df()
        
        fig_prod = px.scatter(df_prod, x='Delitos', y='Detenidos', text='DISTRITO', size='Detenidos', 
                             color='DISTRITO', template="plotly_dark", title="Eficiencia Operativa por Distrito")
        st.plotly_chart(fig_prod, use_container_width=True)

    # --- NIVEL 4: PATRONES CUÁNTICOS ---
    elif menu == "🧠 Patrones Cuánticos":
        st.title("🧠 Módulo Cuántico: Lo No Percibido")
        st.write("Este análisis detecta concentraciones de calor basadas en la probabilidad espacio-temporal.")
        
        # Heatmap de Densidad (Día de semana vs Hora)
        df_q = con.execute("""
            SELECT strftime('%w', CAST(FECHA_EVENTO AS DATE)) as dia,
                   strftime('%H', CAST(HORA_EVENTO AS TIME)) as hora,
                   COUNT(*) as densidad
            FROM t_evento
            GROUP BY ALL
        """).df()
        
        fig_q = px.density_heatmap(df_q, x='hora', y='dia', z='densidad', 
                                   labels={'dia': 'Día (0=Dom)', 'hora': 'Hora del Día'},
                                   color_continuous_scale='Turbo', template="plotly_dark")
        st.plotly_chart(fig_q, use_container_width=True)
        st.info("💡 Las zonas rojas indican ventanas de tiempo donde la guardia operativa debe ser reforzada preventivamente.")

    # --- NIVEL 5: METEOROLOGÍA ---
    elif menu == "☁️ Meteorología Delictiva":
        st.title("🌡️ Factor Clima en la Zona 8")
        
        # Conexión API Clima Real-Time
        try:
            w_data = requests.get("https://api.open-meteo.com/v1/forecast?latitude=-2.1962&longitude=-79.8862&current_weather=true").json()
            curr = w_data['current_weather']
            st.metric("Temperatura Actual (GYE)", f"{curr['temperature']} °C", delta=f"Viento: {curr['windspeed']} km/h")
            
            st.subheader("📊 Relación Histórica: Condición Climática")
            df_w = con.execute("SELECT CONDICION_CLIMATICA, COUNT(*) as n FROM t_evento GROUP BY 1 ORDER BY n DESC").df()
            st.plotly_chart(px.bar(df_w, x='CONDICION_CLIMATICA', y='n', template="plotly_dark", color_discrete_sequence=['#ffa500']), use_container_width=True)
        except:
            st.warning("No se pudo obtener el clima en tiempo real. Mostrando solo datos históricos.")

# --- FOOTER ---
st.sidebar.markdown("---")
st.sidebar.caption("SISTEMA VORTICE-8 | v1.2 (Repotenciado)")
