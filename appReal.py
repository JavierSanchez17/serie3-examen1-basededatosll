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
    "host": "127.0.0.1",
    "user": "root",
    "password": "Allan200408",
    "database": "transacciones_demo",
    "autocommit": False
}

# Diccionario global para mantener conexiones por sesión
connections = {}
connections_lock = threading.Lock()

def get_db_connection(isolation_level=None):
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if isolation_level:
            cursor = connection.cursor()
            cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
            cursor.close()
        return connection
    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def get_session_connection(isolation_level=None):
    if 'session_id' not in session:
        session['session_id'] = secrets.token_hex(8)
        session.permanent = True

    session_id = session['session_id']

    with connections_lock:
        if session_id not in connections:
            conn = get_db_connection(isolation_level)
            if conn:
                connections[session_id] = {'connection': conn, 'last_used': time.time()}
                print(f"[DEBUG] Nueva conexión creada para sesión: {session_id}")
            else:
                return None
        else:
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
    if request.method == "OPTIONS":
        return '', 200

    if secrets.randbelow(10) == 0:
        cleanup_old_connections()

    action = request.values.get("action")

    try:
        # --- Iniciar transacción ---
        if action == "start_transaction":
            close_session_connection()
            isolation_level = request.values.get("isolation-level", "").upper()
            valid_levels = ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]
            if isolation_level not in valid_levels:
                isolation_level = None
            connection = get_session_connection(isolation_level)
            if not connection:
                return jsonify({"success": False, "message": "No se pudo conectar a la base de datos"})
            try:
                connection.start_transaction()
                session['transaction_active'] = True
                session['inserted_count'] = 0
                session['pending_rows'] = []  # NUEVO: lista de cambios pendientes
                session['isolation_level'] = isolation_level or "DEFAULT"
                session.modified = True
                return jsonify({
                    "success": True,
                    "transaction_id": session.get('session_id'),
                    "isolation_level": session['isolation_level']
                })
            except Error as e:
                return jsonify({"success": False, "message": str(e)})

        # --- Insertar datos ---
        elif action == "insert_data":
            if not session.get('transaction_active', False):
                return jsonify({"success": False, "message": "No hay transacción activa"})
            nombre = request.values.get("nombre", "").strip()
            if not nombre:
                return jsonify({"success": False, "message": "El campo nombre es obligatorio"})
            connection = get_session_connection()
            cursor = connection.cursor()
            cursor.execute("INSERT INTO persona (nombre) VALUES (%s)", (nombre,))
            inserted_id = cursor.lastrowid
            cursor.close()
            # Guardamos en la lista de pendientes
            session['pending_rows'].append({'id': inserted_id, 'nombre': nombre, 'action': 'INSERT'})
            session['inserted_count'] += 1
            session.modified = True
            return jsonify({"success": True, "inserted_id": inserted_id, "pending": session['pending_rows']})

        # --- Actualizar datos ---
        elif action == "update_data":
            if not session.get('transaction_active', False):
                return jsonify({"success": False, "message": "No hay transacción activa"})
            record_id = request.values.get("id")
            new_name = request.values.get("nombre", "").strip()
            if not record_id or not new_name:
                return jsonify({"success": False, "message": "Se requiere ID y nuevo nombre"})
            connection = get_session_connection()
            cursor = connection.cursor()
            cursor.execute("UPDATE persona SET nombre = %s WHERE id = %s", (new_name, record_id))
            cursor.close()
            session['pending_rows'].append({'id': record_id, 'nombre': new_name, 'action': 'UPDATE'})
            session['inserted_count'] += 1
            session.modified = True
            return jsonify({"success": True, "updated_id": record_id, "pending": session['pending_rows']})

        # --- Eliminar datos ---
        elif action == "delete_data":
            if not session.get('transaction_active', False):
                return jsonify({"success": False, "message": "No hay transacción activa"})
            record_id = request.values.get("id")
            if not record_id:
                return jsonify({"success": False, "message": "Se requiere ID"})
            connection = get_session_connection()
            cursor = connection.cursor()
            cursor.execute("DELETE FROM persona WHERE id = %s", (record_id,))
            cursor.close()
            session['pending_rows'].append({'id': record_id, 'nombre': None, 'action': 'DELETE'})
            session['inserted_count'] += 1
            session.modified = True
            return jsonify({"success": True, "deleted_id": record_id, "pending": session['pending_rows']})

        # --- Commit ---
        elif action == "commit_transaction":
            if not session.get('transaction_active', False):
                return jsonify({"success": False, "message": "No hay transacción activa"})
            connection = get_session_connection()
            inserted_count = session.get('inserted_count', 0)
            connection.commit()
            pending_rows = session.get('pending_rows', [])
            session['transaction_active'] = False
            session['inserted_count'] = 0
            session['pending_rows'] = []
            session.modified = True
            close_session_connection()
            return jsonify({"success": True, "committed_count": inserted_count, "pending": pending_rows})

        # --- Rollback ---
        elif action == "rollback_transaction":
            if not session.get('transaction_active', False):
                return jsonify({"success": False, "message": "No hay transacción activa"})
            connection = get_session_connection()
            inserted_count = session.get('inserted_count', 0)
            connection.rollback()
            pending_rows = session.get('pending_rows', [])
            session['transaction_active'] = False
            session['inserted_count'] = 0
            session['pending_rows'] = []
            session.modified = True
            close_session_connection()
            return jsonify({"success": True, "rolled_back_count": inserted_count, "pending": pending_rows})

        # --- Obtener datos confirmados ---
        elif action == "get_data":
            connection = get_db_connection()
            cursor = connection.cursor()
            cursor.execute("SELECT id, nombre FROM persona ORDER BY id DESC")
            rows = cursor.fetchall()
            cursor.close()
            connection.close()
            return jsonify({"success": True, "data": rows})

        # --- Obtener cambios pendientes ---
        elif action == "get_pending":
            pending_rows = session.get('pending_rows', [])
            return jsonify({"success": True, "data": pending_rows})

        return jsonify({"success": False, "message": "Acción no válida"})

    except Exception as e:
        return jsonify({"success": False, "message": f"Error inesperado: {str(e)}"})

@app.after_request
def aplicar_cors(respuesta):
    respuesta.headers["Access-Control-Allow-Origin"] = "*"
    respuesta.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    respuesta.headers["Access-Control-Allow-Headers"] = "Content-Type"
    respuesta.headers["Access-Control-Allow-Credentials"] = "true"
    return respuesta

@app.teardown_appcontext
def close_db(error):
    if error:
        print(f"[DEBUG] Error en aplicación: {error}")

if __name__ == "__main__":
    try:
        app.run(debug=True)
    finally:
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

