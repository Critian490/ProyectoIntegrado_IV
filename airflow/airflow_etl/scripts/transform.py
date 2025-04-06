import sqlite3
import os

# 1. Conectarse a la base de datos donde están las tablas Silver
# 2. Guarda los queries realizados en el trabajo pasado como un string

def transform_data():
    db_path = '/opt/airflow/dags/data/ecommerce.db'

    try:
        # 1 Conectar a la base de datos
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("✅ Conectado a la base de datos.")

        # 2 Query 1: Top 10 estados con mayor ingreso
        query1 = """
            DROP TABLE IF EXISTS gold_top_states;
            CREATE TABLE gold_top_states AS
            SELECT 
                c.customer_state AS Estado, 
                CAST(AVG(julianday(o.order_delivered_customer_date) - julianday(o.order_estimated_delivery_date)) AS INTEGER) AS Diferencia_Entrega
            FROM silver_olist_orders o
            JOIN silver_olist_customers c ON o.customer_id = c.customer_id
            WHERE o.order_status = 'delivered' 
                AND o.order_delivered_customer_date IS NOT NULL
            GROUP BY c.customer_state
            ORDER BY Diferencia_Entrega DESC;
        """
        
        # 3 Query 2: Comparación de tiempos reales vs estimados por mes y año
        query2 = """
            DROP TABLE IF EXISTS gold_delivery_comparison;
            CREATE TABLE gold_delivery_comparison AS
            SELECT 
                strftime('%Y-%m', order_delivered_customer_date) AS month_year,
                AVG(julianday(order_delivered_customer_date) - julianday(order_estimated_delivery_date)) AS avg_delay
            FROM silver_olist_orders
            WHERE order_delivered_customer_date IS NOT NULL
                AND order_estimated_delivery_date IS NOT NULL
            GROUP BY month_year
            ORDER BY month_year;
        """

        print("🚀 Ejecutando queries para crear tablas Gold...")
        cursor.executescript(query1)  # Ejecutar Query 1
        cursor.executescript(query2)  # Ejecutar Query 2

        # 4️⃣ Guardar los cambios y cerrar la conexión
        conn.commit()
        conn.close()
        print("✅ Tablas Gold creadas en ecommerce.db: 'gold_top_states' y 'gold_delivery_comparison'")


    except sqlite3.Error as e:
        print(f"❌ Error en transform_data: {e}")