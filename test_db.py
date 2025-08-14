import mysql.connector
from mysql.connector import Error

try:
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="Allan200408",
        database="transacciones_demo"
    )
    if conn.is_connected():
        print("Conexión exitosa a MySQL")
    conn.close()
except Error as e:
    print("Error al conectar:", e)
