import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import requests
import io

# --- CONFIGURACIÓN DE INTERFAZ ---
st.set_page_config(page_title="VORTICE-8 | Inteligencia Z8", layout="wide", page_icon="🚔")

st.markdown("""
    <style>
    .main { background-color: #0b0e14; color: #e0e0e0; }
    div[data-testid="stMetricValue"] { color: #00ffcc; font-family: 'Courier New', monospace; }
    .stHeader { border-bottom: 2px solid #00ffcc; }
    </style>
    """, unsafe_allow_html=True)

# --- MAPEO DE DATOS (DICCIONARIO DE SHAREPOINT) ---
# Se eliminó el código redundante e=... y se dejó solo el parámetro de descarga limpia
DATA_LINKS = {
    "armas": "https://analisispolicial.sharepoint.com/:u:/g/IQByBqSIWwvlRb0C7-7yXgjbAfJvhZumoAkpRbv_tNc2t0s?download=1",
    "objetos": "https://analisispolicial.sharepoint.com/:u:/g/IQByBqSIWwvlRb0C7-7yXgjbAfJvhZumoAkpRbv_tNc2t0s?download=1",
    "detenidos": "https://analisispolicial.sharepoint.com/:u:/g/IQByBqSIWwvlRb0C7-7yXgjbAfJvhZumoAkpRbv_tNc2t0s?download=1",
    "evento": "https://analisispolicial.sharepoint.com/:u:/g/IQBRcL1hAqruSrohG7Ch8mkzAbPC4AtM9lzaqD5-QpOnU-8?download=1",
    "personal": "https://analisispolicial.sharepoint.com/:u:/g/IQBRcL1hAqruSrohG7Ch8mkzAbPC4AtM9lzaqD5-QpOnU-8?download=1",
    "vehiculos": "https://analisispolicial.sharepoint.com/:u:/g/IQBRcL1hAqruSrohG7Ch8mkzAbPC4AtM9lzaqD5-QpOnU-8?download=1"
}

# --- MOTOR DE DATOS (ANTIGRAVITY BUFFER) ---
@st.cache_resource
def load_data_lake():
    con = duckdb.connect(database=':memory:')
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    progress_bar = st.progress(0)
    step = 1 / len(DATA_LINKS)
    current_progress = 0

    try:
        for name, url in DATA_LINKS.items():
            # El "puente" para evitar el error HTTP 0 de SharePoint
            resp = requests.get(url, headers=headers, timeout=60)
            if resp.status_code == 200:
                buffer = io.BytesIO(resp.content)
                df = pd.read_parquet(buffer)
                con.register(f"t_{name}", df)
            else:
                return con, f"Error en {name}: SharePoint rechazó la conexión (Status {resp.status_code})"
            
            current_progress += step
            progress_bar.progress(min(current_progress, 1.0))
        
        progress_bar.empty()
        return con, True
    except Exception as e:
        return con, f"Fallo Crítico: {str(e)}"

con, status = load_data_lake()

# --- NAVEGACIÓN LATERAL ---
st.sidebar.title("🛡️ VORTICE-8")
st.sidebar.info("Analista Policial: Ing. Jairo Peso")
menu = st.sidebar.radio("Nivel Operativo", [
    "Consolidado Zona 8", 
    "Análisis por Distrito", 
    "Dinamismo Productividad", 
    "Patrones Cuánticos",
    "Meteorología & Delito"
])

# --- LÓGICA DE INTERFAZ ---
if status is not True:
    st.error(f"❌ Error en Data Lake: {status}")
    st.warning("Revisa que los links de SharePoint no hayan expirado.")
else:
    # 1. CONSOLIDADO ZONA 8
    if menu == "Consolidado Zona 8":
        st.title("📈 Dashboard Ejecutivo - Zona 8")
        
        # Consultas de alto rendimiento
        c1, c2, c3 = st.columns(3)
        ev_count = con.execute("SELECT COUNT(*) FROM t_evento").fetchone()[0]
        det_count = con.execute("SELECT COUNT(*) FROM t_detenidos").fetchone()[0]
        arm_count = con.execute("SELECT SUM(CAST(CANTIDAD AS INT)) FROM t_armas").fetchone()[0] or 0
        
        c1.metric("Total Eventos", f"{ev_count:,}")
        c2.metric("Detenciones", f"{det_count:,}")
        c3.metric("Armas Incautadas", int(arm_count))

        # Evolución Temporal
        st.subheader("📌 Tendencia Temporal")
        df_time = con.execute("""
            SELECT CAST(FECHA_EVENTO AS DATE) as fecha, COUNT(*) as n 
            FROM t_evento GROUP BY fecha ORDER BY fecha
        """).df()
        st.plotly_chart(px.line(df_time, x='fecha', y='n', template="plotly_dark", color_discrete_sequence=['#00ffcc']), use_container_width=True)

    # 2. ANÁLISIS POR DISTRITO
    elif menu == "Análisis por Distrito":
        dist_list = con.execute("SELECT DISTINCT DISTRITO FROM t_evento WHERE DISTRITO IS NOT NULL").df()
        d_sel = st.sidebar.selectbox("Seleccione Distrito", dist_list['DISTRITO'])
        
        st.title(f"📍 Situación Táctica: Distrito {d_sel}")
        
        col_l, col_r = st.columns(2)
        with col_l:
            st.write("### Modalidad de Infracción")
            df_pie = con.execute(f"SELECT MODALIDAD, COUNT(*) as n FROM t_evento WHERE DISTRITO = '{d_sel}' GROUP BY MODALIDAD").df()
            st.plotly_chart(px.pie(df_pie, names='MODALIDAD', values='n', hole=0.4, template="plotly_dark"), use_container_width=True)
            
        with col_r:
            st.write("### Circuitos Críticos")
            df_bar = con.execute(f"SELECT CIRCUITO, COUNT(*) as n FROM t_evento WHERE DISTRITO = '{d_sel}' GROUP BY CIRCUITO ORDER BY n DESC LIMIT 10").df()
            st.plotly_chart(px.bar(df_bar, x='n', y='CIRCUITO', orientation='h', template="plotly_dark"), use_container_width=True)

    # 3. DINAMISMO PRODUCTIVIDAD
    elif menu == "Dinamismo Productividad":
        st.title("⚔️ Estrategia Operativa")
        st.info("Cruce de Productividad (Detenidos) vs Incidencia (Eventos)")
        
        # JOIN Multinivel entre tablas
        df_prod = con.execute("""
            SELECT e.DISTRITO, 
                   COUNT(DISTINCT e.N_EVENTO) as Eventos,
                   COUNT(DISTINCT d.DOCUMENTO) as Detenidos
            FROM t_evento e
            LEFT JOIN t_detenidos d ON e.N_EVENTO = d.N_EVENTO
            GROUP BY e.DISTRITO
        """).df()
        
        fig = px.scatter(df_prod, x='Eventos', y='Detenidos', text='DISTRITO', size='Detenidos', color='DISTRITO', template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)

    # 4. PATRONES CUÁNTICOS (No percibidos)
    elif menu == "Patrones Cuánticos":
        st.title("🧠 Módulo Cuántico: Análisis No Lineal")
        st.write("Identificación de 'Hotspots' temporales y correlaciones ocultas.")
        
        # Heatmap de densidad temporal
        df_q = con.execute("""
            SELECT strftime('%w', CAST(FECHA_EVENTO AS DATE)) as dia,
                   strftime('%H', CAST(HORA_EVENTO AS TIME)) as hora,
                   COUNT(*) as densidad
            FROM t_evento
            GROUP BY ALL
        """).df()
        
        fig_q = px.density_heatmap(df_q, x='hora', y='dia', z='densidad', 
                                   labels={'dia': 'Día de la Semana', 'hora': 'Hora'},
                                   color_continuous_scale='Magma', template="plotly_dark")
        st.plotly_chart(fig_q, use_container_width=True)
        st.markdown("> **Interpretación:** Las zonas más brillantes indican ventanas críticas de tiempo donde la probabilidad de eventos es máxima.")

    # 5. METEOROLOGÍA
    elif menu == "Meteorología & Delito":
        st.title("☁️ Inteligencia Meteorológica Real-Time")
        
        # Conexión con API Meteorológica para Zona 8
        try:
            w = requests.get("https://api.open-meteo.com/v1/forecast?latitude=-2.1962&longitude=-79.8862&current_weather=true").json()
            curr = w['current_weather']
            
            st.metric("Temperatura GYE", f"{curr['temperature']} °C")
            
            # Análisis basado en tu columna de clima
            st.subheader("Análisis Histórico de Clima")
            df_w = con.execute("SELECT CONDICION_CLIMATICA, COUNT(*) as n FROM t_evento GROUP BY 1 ORDER BY n DESC").df()
            st.plotly_chart(px.bar(df_w, x='CONDICION_CLIMATICA', y='n', template="plotly_dark"), use_container_width=True)
        except:
            st.warning("Servicio meteorológico temporalmente fuera de línea.")

st.sidebar.markdown("---")
st.sidebar.caption("SISTEMA VORTICE-8 | v1.0")
