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

st.title("📊 Dashboard de Auditoría de Intereses")

# Configuración de conexión
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "database": os.getenv("POSTGRES_DB", "wallet_db"),
    "user": os.getenv("POSTGRES_USER", "wallet_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "wallet_pass"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "connect_timeout": 5
}

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
    if connection_pool:
        st.success("✅ Conectado a PostgreSQL")
    else:
        st.error("❌ No se pudo conectar a PostgreSQL")

    default_start = datetime(2024, 5, 1)
    default_end = datetime(2024, 10, 7)
    fecha_inicio = st.date_input("Fecha inicio", default_start)
    fecha_fin = st.date_input("Fecha fin", default_end)

    tipo_analisis = st.radio(
        "Tipo de análisis",
        ["Resumen General", "Detalle por Cuenta", "Tendencias Temporales"]
    )

    if st.button("🔁 Probar conexión a DB"):
        st.cache_data.clear()
        st.rerun()

# Consultas optimizadas
@st.cache_data(ttl=600)
def get_audit_summary(fecha_inicio, fecha_fin):
    query = f"""
    SELECT
        COUNT(*) AS total_registros,
        SUM(CASE WHEN qualified THEN 1 ELSE 0 END) AS cuentas_calificadas,
        AVG(relevant_balance) AS saldo_promedio,
        SUM(CASE WHEN process_status = 'error' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS tasa_error
    FROM wallet.interest_audit_log
    WHERE partition_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    """
    conn = get_connection()
    if conn:
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            st.error(f"Error en resumen de auditoría: {e}")
            return pd.DataFrame()
        finally:
            connection_pool.putconn(conn)
    return pd.DataFrame()

@st.cache_data(ttl=600)
def get_audit_reasons(fecha_inicio, fecha_fin):
    query = f"""
    SELECT reason, COUNT(*) AS count
    FROM wallet.interest_audit_log
    WHERE partition_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}' AND NOT qualified
    GROUP BY reason
    ORDER BY count DESC
    """
    conn = get_connection()
    if conn:
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            st.error(f"Error en consulta de razones: {e}")
            return pd.DataFrame()
        finally:
            connection_pool.putconn(conn)
    return pd.DataFrame()

@st.cache_data(ttl=600)
def get_payments_summary(fecha_inicio, fecha_fin):
    query = f"""
    SELECT
        partition_date,
        COUNT(account_id) AS cuentas,
        SUM(interest_amount) AS interes_total,
        AVG(eligible_balance) AS saldo_promedio
    FROM wallet.interest_payments
    WHERE partition_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    GROUP BY partition_date
    ORDER BY partition_date
    """
    conn = get_connection()
    if conn:
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            st.error(f"Error en resumen de pagos: {e}")
            return pd.DataFrame()
        finally:
            connection_pool.putconn(conn)
    return pd.DataFrame()

@st.cache_data(ttl=600)
def get_payments_detail(fecha_inicio, fecha_fin, min_balance=100, min_interest=10):
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
      AND eligible_balance >= {min_balance}
      AND interest_amount >= {min_interest}
    ORDER BY interest_amount DESC
    LIMIT 1000
    """
    conn = get_connection()
    if conn:
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            st.error(f"Error en detalle de pagos: {e}")
            return pd.DataFrame()
        finally:
            connection_pool.putconn(conn)
    return pd.DataFrame()

@st.cache_data(ttl=600)
def get_payments_distribution(fecha_inicio, fecha_fin):
    query = f"""
    SELECT interest_amount
    FROM wallet.interest_payments
    WHERE partition_date BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    """
    conn = get_connection()
    if conn:
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            st.error(f"Error en distribución de pagos: {e}")
            return pd.DataFrame()
        finally:
            connection_pool.putconn(conn)
    return pd.DataFrame()

# Cargar datos según análisis
with st.spinner("Cargando datos..."):
    if tipo_analisis == "Resumen General":
        audit_summary = get_audit_summary(fecha_inicio, fecha_fin)
        audit_reasons = get_audit_reasons(fecha_inicio, fecha_fin)
        payments_distribution = get_payments_distribution(fecha_inicio, fecha_fin)
    elif tipo_analisis == "Detalle por Cuenta":
        min_balance = st.sidebar.number_input("Saldo mínimo relevante", min_value=0, value=100)
        min_interest = st.sidebar.number_input("Interés mínimo", min_value=0, value=10)
        payments_detail = get_payments_detail(fecha_inicio, fecha_fin, min_balance, min_interest)
    elif tipo_analisis == "Tendencias Temporales":
        payments_summary = get_payments_summary(fecha_inicio, fecha_fin)
        payments_distribution = get_payments_distribution(fecha_inicio, fecha_fin)

# Visualización según el tipo de análisis
if tipo_analisis == "Resumen General" and not audit_summary.empty:
    st.header("📌 Resumen General")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Registros", int(audit_summary['total_registros'][0]))
    with col2:
        st.metric("Cuentas Calificadas", int(audit_summary['cuentas_calificadas'][0]))
    with col3:
        st.metric("Saldo Promedio", f"${audit_summary['saldo_promedio'][0]:,.2f}")
    with col4:
        st.metric("Tasa de Error", f"{audit_summary['tasa_error'][0]:.2f}%")

    # Gráficos
    tab1, tab2 = st.tabs(["Elegibilidad", "Distribución Intereses"])
    with tab1:
        # Pie chart de cuentas calificadas vs no calificadas
        pie_data = pd.DataFrame({
            "Estado": ["Calificadas", "No Calificadas"],
            "Cantidad": [
                int(audit_summary['cuentas_calificadas'][0]),
                int(audit_summary['total_registros'][0]) - int(audit_summary['cuentas_calificadas'][0])
            ]
        })
        fig = px.pie(
            pie_data,
            names='Estado',
            values='Cantidad',
            title='Proporción Cuentas Calificadas',
            hole=0.4,
            color='Estado',
            color_discrete_map={"Calificadas": '#2ecc71', "No Calificadas": '#e74c3c'}
        )
        st.plotly_chart(fig, use_container_width=True)

        # Razones de exclusión
        if not audit_reasons.empty:
            fig2 = px.bar(
                audit_reasons,
                x='count',
                y='reason',
                orientation='h',
                title='Razones de Exclusión',
                color='reason'
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No hay razones de exclusión registradas en este rango.")

    with tab2:
        if not payments_distribution.empty:
            fig3 = px.histogram(
                payments_distribution,
                x='interest_amount',
                nbins=20,
                title='Distribución de Intereses Pagados',
                color_discrete_sequence=['#3498db']
            )
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("No hay datos de intereses pagados para este rango.")

elif tipo_analisis == "Detalle por Cuenta" and not payments_detail.empty:
    st.header("🔍 Detalle por Cuenta")
    st.dataframe(
        payments_detail,
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

elif tipo_analisis == "Tendencias Temporales" and not payments_summary.empty:
    st.header("📈 Tendencias Temporales")
    tab1, tab2 = st.tabs(["Interés Diario", "Comparativas"])
    with tab1:
        fig = px.line(
            payments_summary,
            x='partition_date',
            y='interes_total',
            title='Interés Total por Día',
            markers=True,
            line_shape='spline'
        )
        st.plotly_chart(fig, use_container_width=True)
    with tab2:
        fig2 = px.scatter(
            payments_summary,
            x='saldo_promedio',
            y='interes_total',
            trendline="lowess",
            title='Relación Saldo Promedio vs Interés Total',
            color='cuentas',
            color_continuous_scale='Viridis'
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