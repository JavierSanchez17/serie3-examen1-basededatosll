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
    "password": "Ramoncito12.",
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
            cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
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
                isolation_level = "SERIALIZABLE"  # Por defecto SERIALIZABLE

            connection = get_session_connection(isolation_level)
            if not connection:
                return jsonify({"success": False, "message": "No se pudo conectar a la base de datos"})

            try:
                # Iniciar transacción
                connection.start_transaction()
                print(
                    f"[DEBUG] Transacción iniciada con nivel {isolation_level} para sesión: {session.get('session_id')}")

                # Para SERIALIZABLE, obtener un bloqueo inicial en la tabla
                if isolation_level == "SERIALIZABLE":
                    cursor = connection.cursor()
                    # Obtener un bloqueo compartido en toda la tabla para detectar conflictos
                    cursor.execute("SELECT COUNT(*) FROM persona LOCK IN SHARE MODE")
                    count = cursor.fetchone()[0]
                    cursor.fetchall()  # Consumir todos los resultados
                    cursor.close()
                    print(f"[DEBUG] Bloqueo inicial obtenido en tabla persona (registros: {count})")

                session['transaction_active'] = True
                session['inserted_count'] = 0
                session['pending_rows'] = []
                session['isolation_level'] = isolation_level
                session.modified = True

                return jsonify({
                    "success": True,
                    "transaction_id": session.get('session_id'),
                    "isolation_level": session['isolation_level']
                })
            except Error as e:
                print(f"[DEBUG] Error al iniciar transacción: {str(e)}")
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

            try:
                # Para SERIALIZABLE, bloquear toda la tabla para escritura
                if session.get('isolation_level') == "SERIALIZABLE":
                    print(f"[DEBUG] Intentando obtener bloqueo exclusivo para inserción...")
                    # Bloqueo exclusivo en la tabla completa
                    cursor.execute("SELECT * FROM persona FOR UPDATE")
                    cursor.fetchall()  # Consumir todos los resultados
                    print(f"[DEBUG] Bloqueo exclusivo obtenido para inserción")

                # Realizar la inserción
                cursor.execute("INSERT INTO persona (nombre) VALUES (%s)", (nombre,))
                inserted_id = cursor.lastrowid
                cursor.close()

                print(f"[DEBUG] Registro insertado con ID: {inserted_id}, Nombre: {nombre}")

                # Guardamos en la lista de pendientes
                session['pending_rows'].append({'id': inserted_id, 'nombre': nombre, 'action': 'INSERT'})
                session['inserted_count'] += 1
                session.modified = True

                return jsonify({"success": True, "inserted_id": inserted_id, "pending": session['pending_rows']})

            except Error as e:
                cursor.close()
                print(f"[DEBUG] Error en inserción: {str(e)}")
                return jsonify({"success": False, "message": f"Error en inserción: {str(e)}"})

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

            try:
                # Para SERIALIZABLE, bloquear el registro específico
                if session.get('isolation_level') == "SERIALIZABLE":
                    cursor.execute("SELECT * FROM persona WHERE id = %s FOR UPDATE", (record_id,))
                    cursor.fetchall()  # Consumir todos los resultados

                cursor.execute("UPDATE persona SET nombre = %s WHERE id = %s", (new_name, record_id))
                cursor.close()

                session['pending_rows'].append({'id': record_id, 'nombre': new_name, 'action': 'UPDATE'})
                session['inserted_count'] += 1
                session.modified = True

                return jsonify({"success": True, "updated_id": record_id, "pending": session['pending_rows']})

            except Error as e:
                cursor.close()
                print(f"[DEBUG] Error en actualización: {str(e)}")
                return jsonify({"success": False, "message": f"Error en actualización: {str(e)}"})

        # --- Eliminar datos ---
        elif action == "delete_data":
            if not session.get('transaction_active', False):
                return jsonify({"success": False, "message": "No hay transacción activa"})

            record_id = request.values.get("id")
            if not record_id:
                return jsonify({"success": False, "message": "Se requiere ID"})

            connection = get_session_connection()
            cursor = connection.cursor()

            try:
                # Para SERIALIZABLE, bloquear el registro antes de eliminarlo
                if session.get('isolation_level') == "SERIALIZABLE":
                    cursor.execute("SELECT * FROM persona WHERE id = %s FOR UPDATE", (record_id,))
                    cursor.fetchall()  # Consumir todos los resultados

                cursor.execute("DELETE FROM persona WHERE id = %s", (record_id,))
                cursor.close()

                session['pending_rows'].append({'id': record_id, 'nombre': None, 'action': 'DELETE'})
                session['inserted_count'] += 1
                session.modified = True

                return jsonify({"success": True, "deleted_id": record_id, "pending": session['pending_rows']})

            except Error as e:
                cursor.close()
                print(f"[DEBUG] Error en eliminación: {str(e)}")
                return jsonify({"success": False, "message": f"Error en eliminación: {str(e)}"})

        # --- Commit ---
        elif action == "commit_transaction":
            if not session.get('transaction_active', False):
                return jsonify({"success": False, "message": "No hay transacción activa"})

            connection = get_session_connection()
            inserted_count = session.get('inserted_count', 0)
            pending_rows = session.get('pending_rows', [])

            try:
                connection.commit()
                print(f"[DEBUG] Transacción confirmada para sesión: {session.get('session_id')}")
                print(f"[DEBUG] {inserted_count} operaciones confirmadas")

                session['transaction_active'] = False
                session['inserted_count'] = 0
                session['pending_rows'] = []
                session.modified = True

                close_session_connection()

                return jsonify({"success": True, "committed_count": inserted_count, "pending": pending_rows})

            except Error as e:
                print(f"[DEBUG] Error en commit: {str(e)}")
                return jsonify({"success": False, "message": f"Error en commit: {str(e)}"})

        # --- Rollback ---
        elif action == "rollback_transaction":
            if not session.get('transaction_active', False):
                return jsonify({"success": False, "message": "No hay transacción activa"})

            connection = get_session_connection()
            inserted_count = session.get('inserted_count', 0)
            pending_rows = session.get('pending_rows', [])

            try:
                connection.rollback()
                print(f"[DEBUG] Transacción revertida para sesión: {session.get('session_id')}")
                print(f"[DEBUG] {inserted_count} operaciones revertidas")

                session['transaction_active'] = False
                session['inserted_count'] = 0
                session['pending_rows'] = []
                session.modified = True

                close_session_connection()

                return jsonify({"success": True, "rolled_back_count": inserted_count, "pending": pending_rows})

            except Error as e:
                print(f"[DEBUG] Error en rollback: {str(e)}")
                return jsonify({"success": False, "message": f"Error en rollback: {str(e)}"})

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
        print(f"[DEBUG] Error inesperado: {str(e)}")
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
        app.run(debug=True, threaded=True)  # Habilitamos threading para concurrencia
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