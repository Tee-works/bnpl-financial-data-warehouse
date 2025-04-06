import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import duckdb

# Set page configuration
st.set_page_config(page_title="Tabby Analytics Dashboard", page_icon="📊", layout="wide")

# Database connection
@st.cache_resource
def get_connection():
    return duckdb.connect("/home/taiwo/tabby-dwh/data/tabby_dwh.duckdb", read_only=True)

conn = get_connection()

# Helper function to run queries
def run_query(query):
    try:
        return conn.execute(query).fetchdf()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

# Title
st.title("Tabby BNPL Analytics Dashboard")
st.write("A comprehensive view of Buy Now Pay Later performance metrics")

# Sidebar for filtering
st.sidebar.header("Filters")

# Date range filter
try:
    # Get min and max dates from transactions
    date_query = "SELECT MIN(transaction_date_key) as min_date, MAX(transaction_date_key) as max_date FROM bronze_silver.fact_transactions"
    date_range = conn.execute(date_query).fetchone()
    min_date, max_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    
    start_date = st.sidebar.date_input("Start Date", min_date)
    end_date = st.sidebar.date_input("End Date", max_date)
except Exception as e:
    st.sidebar.error(f"Error loading date range: {e}")
    start_date, end_date = pd.to_datetime('2024-01-01'), pd.to_datetime('2025-01-01')

# Apply filters to query
date_filter = ""
if start_date and end_date:
    date_filter = f"WHERE transaction_date_key BETWEEN '{start_date}' AND '{end_date}'"

# Create a three-column layout
col1, col2, col3 = st.columns(3)

# KPI Cards
with col1:
    try:
        total_transactions = conn.execute(f"SELECT COUNT(*) FROM bronze_silver.fact_transactions {date_filter}").fetchone()[0]
        st.metric("Total Transactions", f"{total_transactions:,}")
    except Exception as e:
        st.error(f"Error loading transaction count: {e}")

with col2:
    try:
        total_amount = conn.execute(f"SELECT SUM(amount) FROM bronze_silver.fact_transactions {date_filter}").fetchone()[0]
        st.metric("Total Transaction Value", f"${total_amount:,.2f}")
    except Exception as e:
        st.error(f"Error loading transaction value: {e}")

with col3:
    try:
        active_customers = conn.execute(f"""
            SELECT COUNT(DISTINCT customer_sk) 
            FROM bronze_silver.fact_transactions 
            {date_filter}
        """).fetchone()[0]
        st.metric("Active Customers", f"{active_customers:,}")
    except Exception as e:
        st.error(f"Error loading customer count: {e}")

# Create a two-column layout for charts
col1, col2 = st.columns(2)

# Transactions over time
with col1:
    st.subheader("Daily Transactions")
    try:
        # Get transaction data by date
        query = f"""
        SELECT 
            CAST(transaction_date_key AS DATE) as date, 
            COUNT(*) as transaction_count,
            SUM(amount) as transaction_value
        FROM bronze_silver.fact_transactions
        {date_filter}
        GROUP BY date
        ORDER BY date
        """
        transactions_by_date = conn.execute(query).fetchdf()
        
        # Plot
        fig = px.line(transactions_by_date, x='date', y=['transaction_count', 'transaction_value'], 
                      title='Transactions Over Time',
                      labels={'date': 'Date', 'value': 'Count/Value', 'variable': 'Metric'},
                      color_discrete_sequence=['blue', 'green'])
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading transactions chart: {e}")

# Payment Method Distribution
with col2:
    st.subheader("Payment Method Distribution")
    try:
        # Get payment method distribution
        query = f"""
        SELECT 
            payment_method, 
            COUNT(*) as count,
            SUM(amount) as total_amount
        FROM bronze_silver.fact_transactions
        {date_filter}
        GROUP BY payment_method
        ORDER BY count DESC
        """
        payment_methods = conn.execute(query).fetchdf()
        
        # Plot
        fig = px.pie(payment_methods, values='count', names='payment_method', 
                    title='Transaction Count by Payment Method')
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading payment methods chart: {e}")

# Create a full-width row for customer analytics
st.subheader("Customer Analytics")
try:
    # Get top customers by transaction value
    query = f"""
    SELECT 
        c.customer_id,
        c.first_name || ' ' || c.last_name as customer_name,
        COUNT(t.transaction_id) as transaction_count,
        SUM(t.amount) as total_spend
    FROM bronze_silver.fact_transactions t
    JOIN bronze_silver.dim_customers c ON t.customer_sk = c.customer_sk
    {date_filter}
    GROUP BY c.customer_id, customer_name
    ORDER BY total_spend DESC
    LIMIT 10
    """
    top_customers = conn.execute(query).fetchdf()
    
    # Plot
    fig = px.bar(top_customers, y='customer_name', x='total_spend', 
                orientation='h', title='Top 10 Customers by Spend',
                labels={'customer_name': 'Customer', 'total_spend': 'Total Spend ($)'},
                color='total_spend', color_continuous_scale='Viridis')
    st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    st.error(f"Error loading top customers chart: {e}")

# Payment Plans Analytics section
st.header("Payment Plans Analytics")

try:
    # Update date filter for payment plans
    plans_date_filter = ""
    if start_date and end_date:
        plans_date_filter = f"WHERE plan_date_key BETWEEN '{start_date}' AND '{end_date}'"
    
    # Query for payment plan metrics
    plan_metrics_query = f"""
    SELECT
        COUNT(*) AS total_plans,
        SUM(CASE WHEN status = 'active' OR status = 'in_progress' THEN 1 ELSE 0 END) AS active_plans,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_plans,
        SUM(CASE WHEN status = 'defaulted' THEN 1 ELSE 0 END) AS defaulted_plans
    FROM bronze_silver.fact_payment_plans
    {plans_date_filter}
    """
    plan_metrics = run_query(plan_metrics_query)
    
    # Display payment plan metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Payment Plans", f"{plan_metrics['total_plans'].iloc[0]:,}")
    with col2:
        st.metric("Active Plans", f"{plan_metrics['active_plans'].iloc[0]:,}")
    with col3:
        st.metric("Completed Plans", f"{plan_metrics['completed_plans'].iloc[0]:,}")
    with col4:
        st.metric("Defaulted Plans", f"{plan_metrics['defaulted_plans'].iloc[0]:,}")
    
    # Payment plan completion rate trend
    completion_query = f"""
    SELECT
        DATE_TRUNC('month', plan_date_key) AS month,
        AVG(payment_completion_rate) AS avg_completion_rate
    FROM bronze_silver.fact_payment_plans
    {plans_date_filter}
    GROUP BY month
    ORDER BY month
    """
    completion_df = run_query(completion_query)
    
    # Payment plans by installment count
    installment_query = f"""
    SELECT
        installment_count,
        COUNT(*) AS plan_count
    FROM bronze_silver.fact_payment_plans
    {plans_date_filter}
    GROUP BY installment_count
    ORDER BY installment_count
    """
    installment_df = run_query(installment_query)
    
    # Create charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Payment Plan Completion Rate Trend")
        if not completion_df.empty:
            fig_completion = px.line(
                completion_df,
                x='month',
                y='avg_completion_rate',
                labels={'month': 'Month', 'avg_completion_rate': 'Avg Completion Rate'},
                markers=True
            )
            fig_completion.update_layout(yaxis_tickformat='.1%')
            st.plotly_chart(fig_completion, use_container_width=True)
        else:
            st.info("No payment plan completion data available for the selected date range.")
    
    with col2:
        st.subheader("Plans by Installment Count")
        if not installment_df.empty:
            fig_installment = px.bar(
                installment_df,
                x='installment_count',
                y='plan_count',
                labels={'installment_count': 'Number of Installments', 'plan_count': 'Number of Plans'},
                text='plan_count'
            )
            st.plotly_chart(fig_installment, use_container_width=True)
        else:
            st.info("No installment data available for the selected date range.")
    
    # Merchants with high default rates
    default_query = f"""
    WITH merchant_defaults AS (
        SELECT
            m.merchant_name,
            COUNT(p.plan_id) AS total_plans,
            SUM(CASE WHEN p.status = 'defaulted' THEN 1 ELSE 0 END) AS defaulted_plans,
            SUM(p.total_amount) AS total_amount
        FROM bronze_silver.fact_payment_plans p
        JOIN bronze_silver.dim_merchants m ON p.merchant_sk = m.merchant_sk
        {plans_date_filter}
        GROUP BY m.merchant_name
    )
    SELECT
        merchant_name,
        total_plans,
        defaulted_plans,
        CASE 
            WHEN total_plans > 0 THEN defaulted_plans * 1.0 / total_plans
            ELSE 0
        END AS default_rate,
        total_amount
    FROM merchant_defaults
    WHERE total_plans >= 5
    ORDER BY default_rate DESC
    LIMIT 10
    """
    default_df = run_query(default_query)
    
    st.subheader("Merchants with Highest Default Rates")
    if not default_df.empty:
        # Format the columns for better display
        formatted_df = default_df.copy()
        formatted_df['default_rate'] = formatted_df['default_rate'].apply(lambda x: f"{x:.1%}")
        formatted_df['total_amount'] = formatted_df['total_amount'].apply(lambda x: f"${x:,.2f}")
        
        st.dataframe(formatted_df)
    else:
        st.info("No merchants with sufficient payment plans found in the selected date range.")

except Exception as e:
    st.error(f"Error in Payment Plans Analytics: {str(e)}")
    st.code(str(e))

# Footer
st.markdown("---")
st.markdown("Tabby BNPL Data Warehouse Project | Created by Taiwo")