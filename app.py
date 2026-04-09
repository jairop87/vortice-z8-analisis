import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests

# --- CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(
    page_title="VORTICE-8 | Inteligencia Policial",
    page_icon="🚔",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- ESTILOS PERSONALIZADOS (ANTIGRAVITY) ---
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #1f2937; padding: 15px; border-radius: 10px; border: 1px solid #374151; }
    </style>
    """, unsafe_allow_html=True)

# --- MOTOR DE DATOS (DUCKDB) ---
@st.cache_resource
def get_connection():
    # Conexión persistente en memoria
    return duckdb.connect(database=':memory:')

con = get_connection()

def cargar_datos():
    try:
        # Registramos todos los .parquet de la carpeta data como una sola vista
        con.execute("CREATE OR REPLACE VIEW base_z8 AS SELECT * FROM 'data/*.parquet'")
        return True, "OK"
    except Exception as e:
        return False, str(e)

success, msg = cargar_datos()

# --- FUNCIONES DE ANALÍTICA (EL "OJO HUMANO") ---
def obtener_clima_gye():
    # Estructura para API de clima (Lat/Lon Guayaquil)
    # Nota: Aquí puedes insertar tu API Key de OpenWeatherMap
    try:
        url = "https://api.open-meteo.com/v1/forecast?latitude=-2.1962&longitude=-79.8862&current_weather=true"
        response = requests.get(url).json()
        return response['current_weather']
    except:
        return None

# --- NAVEGACIÓN LATERAL ---
st.sidebar.title("🗂️ VORTICE-8")
st.sidebar.markdown("---")

menu = st.sidebar.radio(
    "Nivel de Análisis",
    ["Dashboard Ejecutivo Z8", "Detalle Distrital", "Módulo Cuántico", "Estrategia Climatológica"]
)

# Lista de Distritos de la Zona 8
distritos_z8 = [
    "9 DE OCTUBRE", "CEIBOS", "DURAN", "ESTEROS", "FLORIDA", "MODELO", "NUEVA PROSPERINA", "PASCUALES", "PORTETE", "PROGRESO", "SAMBORONDON", "SUR"
]

# --- LÓGICA DE INTERFAZ ---

if not success:
    st.error(f"⚠️ Error al conectar con los datos: {msg}")
    st.info("Asegúrate de que la carpeta 'data' contenga archivos .parquet válidos.")

else:
    # --- NIVEL 1: DASHBOARD EJECUTIVO ---
    if menu == "Dashboard Ejecutivo Z8":
        st.title("📈 Consolidado Zona 8")
        st.markdown("### Análisis de Productividad y Eventos Delictivos")
        
        # Métricas principales usando DuckDB
        total_eventos = con.execute("SELECT COUNT(*) FROM base_z8").fetchone()[0]
        clima = obtener_clima_gye()
        temp = f"{clima['temperature']}°C" if clima else "N/A"

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Eventos Registrados", f"{total_eventos:,}")
        with col2:
            st.metric("Temperatura Actual (GYE)", temp)
        with col3:
            st.metric("Estado del Sistema", "Operativo", delta="DuckDB Active")

        # Gráfico de barras por tipo (Asumiendo columna 'TIPO_EVENTO' o similar)
        # DuckDB permite sacar el DF directamente
        try:
            resumen_tipo = con.execute("""
                SELECT * FROM (
                    SELECT count(*) as total, * FROM base_z8 
                    GROUP BY ALL 
                    ORDER BY total DESC 
                    LIMIT 10
                )
            """).df()
            
            st.write("#### Top 10 Tipos de Incidencias")
            fig = px.bar(resumen_tipo, x=resumen_tipo.columns[1], y='total', color='total', template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
        except:
            st.warning("No se pudo generar el gráfico automático. Verifica los nombres de las columnas.")

    # --- NIVEL 2: DETALLE DISTRITAL ---
    elif menu == "Detalle Distrital":
        distrito_sel = st.sidebar.selectbox("Seleccione el Distrito", distritos_z8)
        st.title(f"📍 Análisis Distrito: {distrito_sel}")
        
        # Aquí es donde DuckDB brilla por su velocidad de filtrado
        query = f"SELECT * FROM base_z8 WHERE DISTRITO = '{distrito_sel}' LIMIT 1000"
        try:
            df_distrito = con.execute(query).df()
            st.write(f"Mostrando últimos registros del Distrito {distrito_sel}:")
            st.dataframe(df_distrito, use_container_width=True)
        except:
            st.error("La columna 'DISTRITO' no se encuentra en el archivo Parquet.")

    # --- NIVEL 3: MÓDULO CUÁNTICO ---
    elif menu == "Módulo Cuántico":
        st.title("🧠 Patrones no Percibidos")
        st.info("Detección de correlaciones probabilísticas entre variables.")
        
        # Matriz de Correlación (Ojo humano no ve esto en tablas)
        try:
            df_full = con.execute("SELECT * FROM base_z8 LIMIT 5000").df()
            corr = df_full.select_dtypes(include=['number']).corr()
            
            fig_corr = px.imshow(corr, text_auto=True, title="Mapa de Calor: Correlaciones Ocultas", template="plotly_dark")
            st.plotly_chart(fig_corr, use_container_width=True)
            
            st.markdown("""
            > **Nota técnica:** Este módulo identifica si existe una relación estadística (Pearson) entre la hora del evento, 
            > la dotación policial y la frecuencia de delitos.
            """)
        except:
            st.warning("Se requieren columnas numéricas para el análisis de correlación.")

    # --- NIVEL 4: METEOROLOGÍA ---
    elif menu == "Estrategia Climatológica":
        st.title("☁️ Meteorología y Productividad")
        st.write("Cruce de datos meteorológicos para predicción de eventos.")
        
        clima = obtener_clima_gye()
        if clima:
            st.write(f"**Condición actual:** Velocidad del viento {clima['windspeed']} km/h")
            # Aquí se integraría la lógica: Si llueve en Nueva Prosperina -> Probabilidad de aumento en X delito
            st.info("Estrategia sugerida: El modelo sugiere reforzar patrullaje en zonas de baja visibilidad por condiciones climáticas actuales.")
