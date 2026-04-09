import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import requests

# --- CONFIGURACIÓN DE INTERFAZ ---
st.set_page_config(page_title="VORTICE-8 | Inteligencia Z8", layout="wide", page_icon="🚔")

# Estilo para modo nocturno táctico
st.markdown("""
    <style>
    .main { background-color: #0b0e14; color: #e0e0e0; }
    div[data-testid="stMetricValue"] { color: #00ffcc; }
    </style>
    """, unsafe_allow_html=True)

# --- MOTOR DUCKDB ---
@st.cache_resource
def init_engine():
    con = duckdb.connect(database=':memory:')
    # Registro de tablas principales basado en tus archivos
    try:
        con.execute("CREATE OR REPLACE VIEW t_eventos AS SELECT * FROM 'data/5 EVENTO PEGAR HOJA.parquet'")
        con.execute("CREATE OR REPLACE VIEW t_detenidos AS SELECT * FROM 'data/2 DETENIDOS PEGAR HOJA.parquet'")
        con.execute("CREATE OR REPLACE VIEW t_armas AS SELECT * FROM 'data/1 ARMAS PEGAR HOJA.parquet'")
        con.execute("CREATE OR REPLACE VIEW t_personal AS SELECT * FROM 'data/6 PERSONAL POLICIAL PEGAR HOJA.parquet'")
        return con, True
    except Exception as e:
        return con, f"Error en ingesta: {str(e)}"

con, status = init_engine()

# --- NAVEGACIÓN ---
st.sidebar.title("🛡️ VORTICE-8")
st.sidebar.subheader("Zona 8 - Análisis de Información")
menu = st.sidebar.radio("Nivel Operativo", ["Consolidado Z8", "Análisis Distrital", "Productividad vs Delito", "Análisis Cuántico"])

if status is not True:
    st.error(status)
    st.info("Asegúrate de que los archivos Parquet estén en la carpeta 'data/' de tu repositorio.")
else:
    # --- NIVEL 1: CONSOLIDADO ZONA 8 ---
    if menu == "Consolidado Z8":
        st.title("📈 Dashboard Ejecutivo - Zona 8")
        
        # Métricas de alto nivel
        m1, m2, m3 = st.columns(3)
        eventos = con.execute("SELECT COUNT(*) FROM t_eventos").fetchone()[0]
        detenidos = con.execute("SELECT COUNT(*) FROM t_detenidos").fetchone()[0]
        armas = con.execute("SELECT SUM(CAST(CANTIDAD AS INT)) FROM t_armas").fetchone()[0] or 0
        
        m1.metric("Total Eventos", f"{eventos:,}")
        m2.metric("Detenidos/Aprehendidos", f"{detenidos:,}")
        m3.metric("Armas Incautadas", int(armas))

        # Tendencia Temporal
        st.subheader("📌 Evolución Temporal de Incidentes")
        df_time = con.execute("""
            SELECT FECHA_EVENTO, COUNT(*) as Total 
            FROM t_eventos 
            GROUP BY FECHA_EVENTO 
            ORDER BY FECHA_EVENTO ASC
        """).df()
        fig_time = px.line(df_time, x='FECHA_EVENTO', y='Total', template="plotly_dark", color_discrete_sequence=['#00ffcc'])
        st.plotly_chart(fig_time, use_container_width=True)

    # --- NIVEL 2: ANÁLISIS DISTRITAL ---
    elif menu == "Análisis Distrital":
        distritos = con.execute("SELECT DISTINCT DISTRITO FROM t_eventos WHERE DISTRITO IS NOT NULL").df()
        dist_sel = st.sidebar.selectbox("Seleccione Distrito", distritos['DISTRITO'])
        
        st.title(f"📍 Situación Táctica: {dist_sel}")
        
        col_a, col_b = st.columns(2)
        
        with col_a:
            st.write("### Delitos por Modalidad")
            df_mod = con.execute(f"SELECT MODALIDAD, COUNT(*) as Cantidad FROM t_eventos WHERE DISTRITO = '{dist_sel}' GROUP BY MODALIDAD").df()
            st.plotly_chart(px.pie(df_mod, names='MODALIDAD', values='Cantidad', hole=0.4, template="plotly_dark"), use_container_width=True)
            
        with col_b:
            st.write("### Top Circuitos con más Eventos")
            df_circ = con.execute(f"SELECT CIRCUITO, COUNT(*) as Total FROM t_eventos WHERE DISTRITO = '{dist_sel}' GROUP BY CIRCUITO ORDER BY Total DESC LIMIT 10").df()
            st.plotly_chart(px.bar(df_circ, x='Total', y='CIRCUITO', orientation='h', template="plotly_dark"), use_container_width=True)

    # --- NIVEL 3: PRODUCTIVIDAD VS DELITO ---
    elif menu == "Productividad vs Delito":
        st.title("⚔️ Dinámica de Operatividad")
        st.info("Relación entre operativos realizados y reducción de delitos.")
        
        # Join entre Eventos y Detenidos por N_EVENTO
        df_prod = con.execute("""
            SELECT e.DISTRITO, 
                   COUNT(DISTINCT e.N_EVENTO) as Eventos_Delictivos,
                   COUNT(DISTINCT d.DOCUMENTO) as Detenidos
            FROM t_eventos e
            LEFT JOIN t_detenidos d ON e.N_EVENTO = d.N_EVENTO
            GROUP BY e.DISTRITO
        """).df()
        
        fig_prod = px.scatter(df_prod, x='Eventos_Delictivos', y='Detenidos', text='DISTRITO', 
                             size='Detenidos', color='DISTRITO', template="plotly_dark",
                             title="Eficiencia de Detención por Distrito")
        st.plotly_chart(fig_prod, use_container_width=True)

    # --- NIVEL 4: ANÁLISIS CUÁNTICO ---
    elif menu == "Análisis Cuántico":
        st.title("🧠 Módulo de Patrones No Percibidos")
        
        # Correlación de variables no obvias
        st.write("### Mapa de Correlación: Variables Críticas")
        df_quantum = con.execute("""
            SELECT 
                CAST(strftime('%H', CAST(HORA_EVENTO AS TIME)) AS INT) as HORA_INT,
                CASE WHEN CONDICION_CLIMATICA = 'LLUVIA' THEN 1 ELSE 0 END as ES_LLUVIA,
                (SELECT COUNT(*) FROM t_detenidos d WHERE d.N_EVENTO = e.N_EVENTO) as NUM_DETENIDOS
            FROM t_eventos e
            LIMIT 5000
        """).df()
        
        if not df_quantum.empty:
            corr = df_quantum.corr()
            st.plotly_chart(px.imshow(corr, text_auto=True, color_continuous_scale='Viridis', template="plotly_dark"), use_container_width=True)
            st.write("> **Análisis:** Este mapa revela si factores como la lluvia o la hora del día tienen una relación directa con la efectividad de las capturas.")
        else:
            st.warning("Datos insuficientes para el análisis de patrones.")

# Footer técnico
st.sidebar.markdown("---")
st.sidebar.caption("Ingeniería de Sistemas - VORTICE-8")
