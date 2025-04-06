import sqlite3

#Pistas
# En este paso la idea sería quitar duplicados, manejar nulos, pero como no es el objetivo, 
# Vamos a hacer una copia espejo de los datos, simulando que los datos ya están limpios.
# 1.Conectarse a la base de datos ecommerce.db ubicada en /opt/airflow/dags/data
# 2: Elimine la tabla Silver si ya existe, cree una tabla nueva Silver copiando 
#    todo el contenido de su tabla Bronze correspondiente
#    Cada bloque debe hacer una copia de la tabla Bronze a una nueva tabla Silver
# 3: Guardar los cambios y cerrar la conexión
# 4: Usa print() para mostrar el estado del proceso

def cleaning_data():
    db_path = '/opt/airflow/dags/data/ecommerce.db'

    try:
        # 1 Conectar a la base de datos
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("Conexión exitosa")

#---------------------------------------------------------------------------------------------------------------------------------------

        # 2 ELIMINAR la tabla Silver si ya existe
        #Tabla silver_olis_customers
        cursor.execute("DROP TABLE IF EXISTS silver_olist_customers;")
        print("Ejecución de eliminación de la tabla Silver silver_olist_customers exitosa.")

        #Tabla: silver_olist_order_payments
        cursor.execute("DROP TABLE IF EXISTS silver_olist_order_payments;")
        print("Ejecución de eliminación de la tabla Silver silver_olist_order_payments exitosa.")

        #Tabla: bronze_olist_orders
        cursor.execute("DROP TABLE IF EXISTS silver_olist_orders;")
        print("Ejecución de eliminación de la tabla Silver silver_olist_orders exitosa.")

#---------------------------------------------------------------------------------------------------------------------------------------
        # 3 CREAR las tablas Silver copiando los datos de la tabla Bronze correspondiente
        #Tabla: bronze_olis_customers
        cursor.execute("CREATE TABLE silver_olist_customers AS SELECT * FROM bronze_olist_customers;")
        print("Tabla Silver silver_olis_customers creada como copia de Bronze.")

        #Tabla: bronze_olist_order_payments
        cursor.execute("CREATE TABLE silver_olist_order_payments AS SELECT * FROM bronze_olist_order_payments;")
        print("Tabla Silver silver_olist_order_payments creada como copia de Bronze.")

        #Tabla: bronze_olist_orders
        cursor.execute("CREATE TABLE silver_olist_orders AS SELECT * FROM bronze_olist_orders;")
        print("Tabla Silver silver_olist_orders creada como copia de Bronze.")
#---------------------------------------------------------------------------------------------------------------------------------------

        # Guardar los cambios y cerrar la conexión
        conn.commit()
        conn.close()
        print("Los cambios fueron guardados y la conexión está cerrada.")
#---------------------------------------------------------------------------------------------------------------------------------------

    except sqlite3.Error as e:
        print(f"Error en cleaning_data: {e}")