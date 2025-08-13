from flask import Flask, request, jsonify, send_from_directory, session
import mysql.connector
from mysql.connector import Error
import secrets
import threading
import time

app = Flask(__name__)
app.secret_key = secrets.token_hex(16)

# Configuración de la base de datos
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "S@nchez695313", 
    "database": "transacciones_demo",
    "autocommit": False  
}

# Diccionario global para mantener conexiones por sesión
connections = {}
connections_lock = threading.Lock()

def get_db_connection():
    # Conexión a la base de datos MySQL
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def get_session_connection():
    # Obtiene o crea una conexión específica para esta sesión
    if 'session_id' not in session:
        session['session_id'] = secrets.token_hex(8)
        session.permanent = True
    
    session_id = session['session_id']
    
    with connections_lock:
        # Si no existe conexión para esta sesión, crear una nueva
        if session_id not in connections:
            conn = get_db_connection()
            if conn:
                connections[session_id] = {
                    'connection': conn,
                    'last_used': time.time()
                }
                print(f"[DEBUG] Nueva conexión creada para sesión: {session_id}")
            else:
                return None
        else:
            # Actualizar tiempo de último uso
            connections[session_id]['last_used'] = time.time()
        
        return connections[session_id]['connection']

def close_session_connection():
    if 'session_id' not in session:
        return
        
    session_id = session['session_id']
    
    with connections_lock:
        if session_id in connections:
            conn_info = connections[session_id]
            conn = conn_info['connection']
            
            if conn and conn.is_connected():
                try:
                    # Si hay transacción activa, hacer rollback
                    if conn.in_transaction:
                        conn.rollback()
                        print(f"[DEBUG] Rollback automático al cerrar conexión {session_id}")
                    conn.close()
                    print(f"[DEBUG] Conexión cerrada para sesión: {session_id}")
                except:
                    pass
            
            del connections[session_id]

def cleanup_old_connections():
    current_time = time.time()
    timeout = 300  # 5 minutos
    
    with connections_lock:
        expired_sessions = []
        for session_id, conn_info in connections.items():
            if current_time - conn_info['last_used'] > timeout:
                expired_sessions.append(session_id)
        
        for session_id in expired_sessions:
            conn_info = connections[session_id]
            conn = conn_info['connection']
            if conn and conn.is_connected():
                try:
                    if conn.in_transaction:
                        conn.rollback()
                    conn.close()
                except:
                    pass
            del connections[session_id]
            print(f"[DEBUG] Conexión expirada eliminada: {session_id}")

@app.route("/")
def servir_html():
    return send_from_directory(".", "index.html")

@app.route("/styles.css")
def servir_css():
    return send_from_directory(".", "styles.css")

@app.route("/api", methods=["POST", "GET", "OPTIONS"])
def api():
    print(f"[DEBUG] API llamada - Método: {request.method}, Acción: {request.values.get('action')}")
    
    if request.method == "OPTIONS":
        return '', 200

    # Limpiar conexiones viejas ocasionalmente
    if secrets.randbelow(10) == 0:  # 10% de probabilidad
        cleanup_old_connections()

    action = request.values.get("action")

    try:
        if action == "start_transaction":
            # Cerrar cualquier conexión/transacción anterior
            close_session_connection()
            
            # Crear nueva conexión para la transacción
            connection = get_session_connection()
            if not connection:
                return jsonify({"success": False, "message": "No se pudo conectar a la base de datos"})
            
            try:
                # Iniciar transacción en MySQL
                connection.start_transaction()
                session['transaction_active'] = True
                session['inserted_count'] = 0
                session.modified = True
                
                print(f"[DEBUG] TRANSACCIÓN iniciada en MySQL para sesión: {session.get('session_id')}")
                print(f"[DEBUG] Estado de conexión - En transacción: {connection.in_transaction}")
                
                return jsonify({
                    "success": True,
                    "message": "Transacción iniciada en MySQL. Los datos se insertarán en la BD pero no serán visibles hasta el COMMIT.",
                    "transaction_id": session.get('session_id'),
                    "type": "REAL_MYSQL_TRANSACTION"
                })
                
            except Error as e:
                print(f"[DEBUG] Error al iniciar transacción: {e}")
                return jsonify({"success": False, "message": f"Error al iniciar transacción: {str(e)}"})

        elif action == "insert_data":
            if not session.get('transaction_active', False):
                return jsonify({"success": False, "message": "No hay transacción activa. Inicia una transacción primero."})
            
            nombre = request.values.get("nombre", "").strip()
            if not nombre:
                return jsonify({"success": False, "message": "El campo nombre es obligatorio"})
            
            connection = get_session_connection()
            if not connection or not connection.is_connected():
                return jsonify({"success": False, "message": "Conexión perdida. Reinicia la transacción."})
            
            if not connection.in_transaction:
                return jsonify({"success": False, "message": "No hay transacción activa en la conexión. Reinicia la transacción."})
            
            try:
                cursor = connection.cursor()
                
                # Insertar el dato en la base de datos
                query = "INSERT INTO persona (nombre) VALUES (%s)"
                cursor.execute(query, (nombre,))
                inserted_id = cursor.lastrowid
                
                # Verificar que el dato está en la base de datos (pero no confirmado)
                cursor.execute("SELECT nombre FROM persona WHERE id = %s", (inserted_id,))
                result = cursor.fetchone()
                
                session['inserted_count'] = session.get('inserted_count', 0) + 1
                session.modified = True
                
                cursor.close()
                
                print(f"[DEBUG] INSERTADO en MySQL: '{nombre}' (ID: {inserted_id}) - PENDIENTE DE COMMIT")
                print(f"[DEBUG] Verificación - Dato existe en BD: {result is not None}")
                print(f"[DEBUG] Total insertados en esta transacción: {session.get('inserted_count', 0)}")
                
                return jsonify({
                    "success": True,
                    "message": f"'{nombre}' INSERTADO en la base de datos MySQL (ID: {inserted_id})\nEstado: PENDIENTE DE COMMIT\nSolo visible para esta transacción",
                    "inserted_id": inserted_id,
                    "total_pending": session.get('inserted_count', 0),
                    "status": "INSERTED_NOT_COMMITTED",
                    "note": "El dato está en MySQL pero solo es visible para esta conexión hasta hacer COMMIT"
                })
                
            except Error as e:
                print(f"[DEBUG] Error al insertar: {e}")
                return jsonify({"success": False, "message": f"Error al insertar: {str(e)}"})

        elif action == "commit_transaction":
            if not session.get('transaction_active', False):
                return jsonify({"success": False, "message": "No hay transacción activa"})
            
            connection = get_session_connection()
            if not connection or not connection.is_connected():
                return jsonify({"success": False, "message": "Conexión perdida"})
            
            try:
                inserted_count = session.get('inserted_count', 0)
                
                if inserted_count == 0:
                    return jsonify({"success": False, "message": "No hay datos para confirmar"})
                
                # COMMIT
                connection.commit()
                
                # Limpiar sesión y cerrar conexión
                session['transaction_active'] = False
                session['inserted_count'] = 0
                session.modified = True
                
                print(f"[DEBUG] COMMIT REALIZADO! {inserted_count} registros ahora son PERMANENTES y VISIBLES para todos")
                
                # Cerrar la conexión después del commit
                close_session_connection()
                
                return jsonify({
                    "success": True, 
                    "message": f"COMMIT EXITOSO!\n{inserted_count} persona(s) confirmada(s) PERMANENTEMENTE\nAhora son visibles para todas las conexiones (MySQL Workbench, otras sesiones, etc.)",
                    "committed_count": inserted_count,
                    "status": "COMMITTED_PERMANENT"
                })
                
            except Error as e:
                print(f"[DEBUG] Error en COMMIT: {e}")
                try:
                    connection.rollback()
                    print(f"[DEBUG] Rollback automático por error en commit")
                except:
                    pass
                return jsonify({"success": False, "message": f"Error en commit: {str(e)}"})

        elif action == "rollback_transaction":
            if not session.get('transaction_active', False):
                return jsonify({"success": False, "message": "No hay transacción activa"})
            
            connection = get_session_connection()
            if not connection:
                return jsonify({"success": False, "message": "No se pudo obtener la conexión"})
            
            try:
                inserted_count = session.get('inserted_count', 0)
                
                # ROLLBACK
                connection.rollback()
                
                # Limpiar sesión y cerrar conexión
                session['transaction_active'] = False
                session['inserted_count'] = 0
                session.modified = True
                
                print(f"[DEBUG] ROLLBACK REALIZADO! {inserted_count} registros ELIMINADOS completamente de MySQL")
                
                # Cerrar la conexión después del rollback
                close_session_connection()
                
                return jsonify({
                    "success": True, 
                    "message": f"ROLLBACK EXITOSO!\n {inserted_count} operación(es) CANCELADA(S)\n Los datos fueron ELIMINADOS completamente de la base de datos",
                    "rolled_back_count": inserted_count,
                    "status": "ROLLED_BACK_DELETED"
                })
                
            except Error as e:
                print(f"[DEBUG] Error en ROLLBACK: {e}")
                return jsonify({"success": False, "message": f" Error en rollback: {str(e)}"})

        elif action == "get_data":
            print(f"[DEBUG] Obteniendo datos con conexión INDEPENDIENTE (solo datos confirmados)")
            
            # Usar otra conexion para obtener datos
            connection = get_db_connection()
            if not connection:
                return jsonify({
                    "success": False,
                    "message": "No se pudo conectar a la base de datos",
                    "data": []
                })
            
            cursor = None
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT id, nombre FROM persona ORDER BY id DESC")
                rows = cursor.fetchall()
                
                print(f"[DEBUG] Encontrados {len(rows)} registros CONFIRMADOS (visibles para todas las conexiones)")
                
                return jsonify({
                    "success": True,
                    "data": rows,
                    "total_records": len(rows),
                    "note": "Solo se muestran datos CON COMMIT (confirmados permanentemente)"
                })
                
            except Error as e:
                print(f"[DEBUG] Error al obtener datos: {e}")
                return jsonify({
                    "success": False,
                    "message": str(e),
                    "data": []
                })
            finally:
                if cursor:
                    cursor.close()
                if connection and connection.is_connected():
                    connection.close()

        return jsonify({"success": False, "message": "Acción no válida"})

    except Exception as e:
        print(f"[DEBUG] Error inesperado: {e}")
        return jsonify({"success": False, "message": f"Error inesperado: {str(e)}"})

@app.after_request
def aplicar_cors(respuesta):
    respuesta.headers["Access-Control-Allow-Origin"] = "*"
    respuesta.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    respuesta.headers["Access-Control-Allow-Headers"] = "Content-Type"
    respuesta.headers["Access-Control-Allow-Credentials"] = "true"
    return respuesta

# Limpiar conexiones al cerrar la aplicación
@app.teardown_appcontext
def close_db(error):
    if error:
        print(f"[DEBUG] Error en aplicación: {error}")

if __name__ == "__main__":
    try:
        print("Iniciando servidor Flask con TRANSACCIONES REALES de MySQL")
        print("Los datos se insertarán directamente en MySQL pero no serán visibles hasta COMMIT")
        app.run(debug=True)
    finally:
        # Limpiar todas las conexiones al salir
        with connections_lock:
            for session_id, conn_info in connections.items():
                conn = conn_info['connection']
                if conn and conn.is_connected():
                    try:
                        if conn.in_transaction:
                            conn.rollback()
                        conn.close()
                    except:
                        pass
        print("Todas las conexiones cerradas")