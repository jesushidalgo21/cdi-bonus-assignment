import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from datetime import datetime, timedelta
import os
import time
from psycopg2 import pool

# Configuración de la página
st.set_page_config(
    page_title="Auditoría de Intereses",
    layout="wide",
    page_icon="📊"
)

# Título principal
st.title("📊 Dashboard de Auditoría de Intereses")

# Configuración de conexión con valores por defecto para Docker
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "database": os.getenv("POSTGRES_DB", "wallet_db"),
    "user": os.getenv("POSTGRES_USER", "wallet_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "wallet_pass"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "connect_timeout": 5
}

# Pool de conexiones
@st.cache_resource
def init_connection_pool():
    try:
        return psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            **DB_CONFIG
        )
    except Exception as e:
        st.error(f"⚠️ Error creando pool de conexiones: {e}")
        return None

connection_pool = init_connection_pool()

# Función para obtener conexión con reintentos
def get_connection():
    if connection_pool:
        try:
            return connection_pool.getconn()
        except:
            st.warning("Reconectando a la base de datos...")
            time.sleep(2)
            return connection_pool.getconn()
    return None

# Sidebar con filtros
with st.sidebar:
    st.header("⚙️ Filtros")
    
    # Mostrar estado de la conexión
    if connection_pool:
        st.success("✅ Conectado a PostgreSQL")
    else:
        st.error("❌ No se pudo conectar a PostgreSQL")
    
    # Rango de fechas
    default_end = datetime.now()
    default_start = default_end - timedelta(days=30)
    fecha_inicio = st.date_input("Fecha inicio", default_start)
    fecha_fin = st.date_input("Fecha fin", default_end)
    
    # Filtros adicionales
    tipo_analisis = st.radio(
        "Tipo de análisis",
        ["Resumen General", "Detalle por Cuenta", "Tendencias Temporales"]
    )
    
    # Botón para reconectar
    if st.button("🔁 Probar conexión a DB"):
        st.cache_data.clear()
        st.rerun()

# Consultas base con manejo de errores mejorado
@st.cache_data(ttl=600)
def get_audit_data(fecha_inicio, fecha_fin):
    query = f"""
    SELECT 
        user_id,
        account_id,
        qualified,
        reason,
        relevant_balance,
        calculated_interest,
        interest_rate,
        process_status,
        partition_date
    FROM wallet.interest_audit_log
    WHERE partition_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    """
    conn = get_connection()
    if conn:
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            st.error(f"Error en consulta de auditoría: {e}")
            return pd.DataFrame()
        finally:
            connection_pool.putconn(conn)
    return pd.DataFrame()

@st.cache_data(ttl=600)
def get_payments_data(fecha_inicio, fecha_fin):
    query = f"""
    SELECT 
        user_id,
        account_id,
        eligible_balance,
        interest_rate,
        interest_amount,
        partition_date
    FROM wallet.interest_payments
    WHERE partition_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    """
    conn = get_connection()
    if conn:
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            st.error(f"Error en consulta de pagos: {e}")
            return pd.DataFrame()
        finally:
            connection_pool.putconn(conn)
    return pd.DataFrame()

# Cargar datos con indicador de progreso
with st.spinner("Cargando datos..."):
    audit_df = get_audit_data(fecha_inicio, fecha_fin)
    payments_df = get_payments_data(fecha_inicio, fecha_fin)

# Verificación de datos
if audit_df.empty or payments_df.empty:
    st.warning("""
    ⚠️ No se encontraron datos. Verifique:
    1. Que el servicio PostgreSQL esté corriendo
    2. Que las tablas existan en el esquema 'wallet'
    3. Que haya datos para el rango de fechas seleccionado
    """)
    
    if st.button("🔄 Reintentar carga de datos"):
        st.cache_data.clear()
        st.rerun()

# Visualización según el tipo de análisis
if tipo_analisis == "Resumen General" and not audit_df.empty and not payments_df.empty:
    st.header("📌 Resumen General")
    
    # Métricas clave
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Cuentas", len(audit_df))
    with col2:
        st.metric("Cuentas Calificadas", audit_df['qualified'].sum())
    with col3:
        st.metric("Interés Total", f"${payments_df['interest_amount'].sum():,.2f}")
    with col4:
        tasa_error = (audit_df['process_status'] == 'error').mean() * 100
        st.metric("Tasa de Error", f"{tasa_error:.2f}%")
    
    # Gráficos
    tab1, tab2 = st.tabs(["Elegibilidad", "Distribución Intereses"])
    
    with tab1:
        fig = px.pie(
            audit_df,
            names='qualified',
            title='Proporción Cuentas Calificadas',
            hole=0.4,
            color='qualified',
            color_discrete_map={True: '#2ecc71', False: '#e74c3c'}
        )
        st.plotly_chart(fig, use_container_width=True)
        
        fig2 = px.bar(
            audit_df[~audit_df['qualified']]['reason'].value_counts().reset_index(),
            x='count',
            y='reason',
            orientation='h',
            title='Razones de Exclusión',
            color='reason'
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    with tab2:
        fig3 = px.histogram(
            payments_df,
            x='interest_amount',
            nbins=20,
            title='Distribución de Intereses Pagados',
            color_discrete_sequence=['#3498db']
        )
        st.plotly_chart(fig3, use_container_width=True)

elif tipo_analisis == "Detalle por Cuenta" and not payments_df.empty:
    st.header("🔍 Detalle por Cuenta")
    
    col1, col2 = st.columns(2)
    with col1:
        min_balance = st.number_input("Saldo mínimo relevante", min_value=0, value=100)
    with col2:
        min_interest = st.number_input("Interés mínimo", min_value=0, value=10)
    
    filtered_df = payments_df[
        (payments_df['eligible_balance'] >= min_balance) &
        (payments_df['interest_amount'] >= min_interest)
    ].sort_values('interest_amount', ascending=False)
    
    if not filtered_df.empty:
        st.dataframe(
            filtered_df,
            column_config={
                "interest_amount": st.column_config.NumberColumn(
                    "Interés",
                    format="$%.2f"
                ),
                "eligible_balance": st.column_config.NumberColumn(
                    "Saldo",
                    format="$%.2f"
                ),
                "partition_date": st.column_config.DateColumn(
                    "Fecha",
                    format="YYYY-MM-DD"
                )
            },
            hide_index=True,
            use_container_width=True
        )
    else:
        st.warning("No hay cuentas que cumplan los filtros")

elif tipo_analisis == "Tendencias Temporales" and not payments_df.empty:
    st.header("📈 Tendencias Temporales")
    
    # Agrupar por fecha
    daily_data = payments_df.groupby('partition_date').agg({
        'account_id': 'count',
        'interest_amount': 'sum',
        'eligible_balance': 'mean'
    }).reset_index()
    
    tab1, tab2 = st.tabs(["Interés Diario", "Comparativas"])
    
    with tab1:
        fig = px.line(
            daily_data,
            x='partition_date',
            y='interest_amount',
            title='Interés Total por Día',
            markers=True,
            line_shape='spline'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        fig2 = px.scatter(
            payments_df,
            x='eligible_balance',
            y='interest_amount',
            trendline="lowess",
            title='Relación Saldo vs Interés',
            color='qualified',
            color_discrete_map={True: '#2ecc71', False: '#e74c3c'}
        )
        st.plotly_chart(fig2, use_container_width=True)

# Footer con información del sistema
st.divider()
st.caption(f"""
Dashboard de Auditoría - Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
""")

# Limpieza al cerrar
def cleanup():
    if 'connection_pool' in globals():
        connection_pool.closeall()

import atexit
atexit.register(cleanup)