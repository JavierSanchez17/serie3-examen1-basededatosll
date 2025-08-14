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
    "password": "Ramoncito12.",
    "database": "transacciones_demo",
    "autocommit": False
}

# Niveles de aislamiento
ISOLATION_LEVELS = {
    'READ UNCOMMITTED': 'READ UNCOMMITTED',
    'READ COMMITTED': 'READ COMMITTED',
    'REPEATABLE READ': 'REPEATABLE READ',
    'SERIALIZABLE': 'SERIALIZABLE'
}

# Diccionario global para mantener conexiones por sesión
connections = {}
connections_lock = threading.Lock()


def get_db_connection(isolation_level=None):
    """Crea y retorna una nueva conexión a la base de datos"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if isolation_level:
            cursor = connection.cursor()
            cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
            cursor.close()
            connection.commit()
        return connection
    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None


def get_session_connection():
    """Obtiene o crea una conexión específica para esta sesión"""
    if 'session_id' not in session:
        session['session_id'] = secrets.token_hex(8)
        session.permanent = True

    session_id = session['session_id']
    isolation_level = session.get('isolation_level', 'REPEATABLE READ')

    with connections_lock:
        if session_id not in connections:
            conn = get_db_connection(isolation_level)
            if conn:
                connections[session_id] = {
                    'connection': conn,
                    'last_used': time.time()
                }
                print(f"[DEBUG] Nueva conexión creada para sesión: {session_id} con nivel {isolation_level}")
            else:
                return None
        else:
            connections[session_id]['last_used'] = time.time()

        return connections[session_id]['connection']


def close_session_connection():
    """Cierra la conexión de la sesión actual"""
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
    """Limpia conexiones antiguas (llamar periódicamente)"""
    current_time = time.time()
    timeout = 300

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

    if secrets.randbelow(10) == 0:
        cleanup_old_connections()

    action = request.values.get("action")

    try:
        if action == "start_transaction":
            close_session_connection()

            connection = get_session_connection()
            if not connection:
                return jsonify({"success": False, "message": "No se pudo conectar a la base de datos"})

            try:
                connection.start_transaction()
                session['transaction_active'] = True
                session['inserted_count'] = 0
                session.modified = True

                print(f"[DEBUG] TRANSACCIÓN REAL iniciada en MySQL para sesión: {session.get('session_id')}")

                return jsonify({
                    "success": True,
                    "message": "Transacción REAL iniciada en MySQL.",
                    "transaction_id": session.get('session_id'),
                    "type": "REAL_MYSQL_TRANSACTION",
                    "isolation_level": session.get('isolation_level', 'REPEATABLE READ')
                })

            except Error as e:
                print(f"[DEBUG] Error al iniciar transacción: {e}")
                return jsonify({"success": False, "message": f"Error al iniciar transacción: {str(e)}"})

        elif action == "insert_data":
            if not session.get('transaction_active', False):
                return jsonify(
                    {"success": False, "message": "No hay transacción activa. Inicia una transacción primero."})

            nombre = request.values.get("nombre", "").strip()
            if not nombre:
                return jsonify({"success": False, "message": "El campo nombre es obligatorio"})

            connection = get_session_connection()
            if not connection or not connection.is_connected():
                return jsonify({"success": False, "message": "Conexión perdida. Reinicia la transacción."})

            if not connection.in_transaction:
                return jsonify(
                    {"success": False, "message": "No hay transacción activa en la conexión. Reinicia la transacción."})

            try:
                cursor = connection.cursor()

                query = "INSERT INTO persona (nombre) VALUES (%s)"
                cursor.execute(query, (nombre,))
                inserted_id = cursor.lastrowid

                cursor.execute("SELECT nombre FROM persona WHERE id = %s", (inserted_id,))
                result = cursor.fetchone()

                session['inserted_count'] = session.get('inserted_count', 0) + 1
                session.modified = True

                cursor.close()

                print(f"[DEBUG] INSERTADO en MySQL: '{nombre}' (ID: {inserted_id}) - PENDIENTE DE COMMIT")

                return jsonify({
                    "success": True,
                    "message": f"'{nombre}' INSERTADO en la base de datos MySQL (ID: {inserted_id})",
                    "inserted_id": inserted_id,
                    "total_pending": session.get('inserted_count', 0),
                    "status": "INSERTED_NOT_COMMITTED"
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

                connection.commit()

                session['transaction_active'] = False
                session['inserted_count'] = 0
                session.modified = True

                print(f"[DEBUG] COMMIT REALIZADO! {inserted_count} registros confirmados")

                close_session_connection()

                return jsonify({
                    "success": True,
                    "message": f"COMMIT EXITOSO! {inserted_count} persona(s) confirmada(s)",
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

                connection.rollback()

                session['transaction_active'] = False
                session['inserted_count'] = 0
                session.modified = True

                print(f"[DEBUG] ROLLBACK REALIZADO! {inserted_count} registros eliminados")

                close_session_connection()

                return jsonify({
                    "success": True,
                    "message": f"ROLLBACK EXITOSO! {inserted_count} operación(es) cancelada(s)",
                    "rolled_back_count": inserted_count,
                    "status": "ROLLED_BACK_DELETED"
                })

            except Error as e:
                print(f"[DEBUG] Error en ROLLBACK: {e}")
                return jsonify({"success": False, "message": f"Error en rollback: {str(e)}"})

        elif action == "get_data":
            print(f"[DEBUG] Obteniendo datos con conexión INDEPENDIENTE")

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

                print(f"[DEBUG] Encontrados {len(rows)} registros CONFIRMADOS")

                return jsonify({
                    "success": True,
                    "data": rows,
                    "total_records": len(rows)
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

        elif action == "set_isolation":
            isolation_level = request.values.get("level")
            if isolation_level not in ISOLATION_LEVELS.values():
                return jsonify({"success": False, "message": "Nivel de aislamiento no válido"})

            session['isolation_level'] = isolation_level
            session.modified = True

            close_session_connection()

            return jsonify({
                "success": True,
                "message": f"Nivel de aislamiento establecido a {isolation_level}",
                "current_level": isolation_level
            })

        elif action == "get_isolation":
            current_level = session.get('isolation_level', 'REPEATABLE READ')
            return jsonify({
                "success": True,
                "current_level": current_level,
                "available_levels": list(ISOLATION_LEVELS.values())
            })

        elif action == "test_dirty_read":
            # Usar una conexión nueva con READ UNCOMMITTED para detectar lecturas sucias
            test_conn = get_db_connection('READ UNCOMMITTED')
            if not test_conn:
                return jsonify({"success": False, "message": "No se pudo conectar a la base de datos"})

            try:
                cursor = test_conn.cursor()
                cursor.execute("SELECT nombre FROM persona WHERE id = %s", (request.values.get("id"),))
                result = cursor.fetchone()

                # Obtener el estado real del dato (si está confirmado o no)
                confirmed_conn = get_db_connection()
                confirmed_cursor = confirmed_conn.cursor()
                confirmed_cursor.execute("SELECT nombre FROM persona WHERE id = %s", (request.values.get("id"),))
                confirmed_result = confirmed_cursor.fetchone()

                is_dirty = result != confirmed_result

                cursor.close()
                confirmed_cursor.close()
                test_conn.close()
                confirmed_conn.close()

                return jsonify({
                    "success": True,
                    "data": result[0] if result else None,
                    "is_dirty": is_dirty,
                    "confirmed_data": confirmed_result[0] if confirmed_result else None,
                    "note": "Lectura sucia detectada" if is_dirty else "Datos consistentes"
                })
            except Error as e:
                return jsonify({"success": False, "message": str(e)})
            finally:
                if 'cursor' in locals(): cursor.close()
                if 'confirmed_cursor' in locals(): confirmed_cursor.close()
                if 'test_conn' in locals() and test_conn.is_connected(): test_conn.close()
                if 'confirmed_conn' in locals() and confirmed_conn.is_connected(): confirmed_conn.close()

        elif action == "test_non_repeatable_read":
            connection = get_session_connection()
            if not connection:
                return jsonify({"success": False, "message": "No hay conexión"})

            try:
                cursor = connection.cursor()

                # Primera lectura
                cursor.execute("SELECT nombre FROM persona WHERE id = %s", (request.values.get("id"),))
                first_read = cursor.fetchone()

                # Esperar un momento para que otro proceso pueda modificar el dato
                time.sleep(2)

                # Segunda lectura
                cursor.execute("SELECT nombre FROM persona WHERE id = %s", (request.values.get("id"),))
                second_read = cursor.fetchone()

                is_different = first_read != second_read

                cursor.close()

                return jsonify({
                    "success": True,
                    "first_read": first_read[0] if first_read else None,
                    "second_read": second_read[0] if second_read else None,
                    "is_different": is_different,
                    "note": "Lectura no repetible detectada" if is_different else "Lecturas consistentes"
                })
            except Error as e:
                return jsonify({"success": False, "message": str(e)})

        elif action == "test_phantom_read":
            connection = get_session_connection()
            if not connection:
                return jsonify({"success": False, "message": "No hay conexión"})

            try:
                cursor = connection.cursor()

                # Primera lectura
                cursor.execute("SELECT COUNT(*) FROM persona WHERE nombre LIKE %s",
                               ('%' + request.values.get("filter", "") + '%',))
                first_count = cursor.fetchone()[0]

                # Esperar un momento para que otro proceso pueda insertar datos
                time.sleep(2)

                # Segunda lectura
                cursor.execute("SELECT COUNT(*) FROM persona WHERE nombre LIKE %s",
                               ('%' + request.values.get("filter", "") + '%',))
                second_count = cursor.fetchone()[0]

                has_phantoms = first_count != second_count

                cursor.close()

                return jsonify({
                    "success": True,
                    "first_count": first_count,
                    "second_count": second_count,
                    "has_phantoms": has_phantoms,
                    "note": "Lectura fantasma detectada" if has_phantoms else "No se detectaron lecturas fantasma"
                })
            except Error as e:
                return jsonify({"success": False, "message": str(e)})

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


@app.teardown_appcontext
def close_db(error):
    if error:
        print(f"[DEBUG] Error en aplicación: {error}")


if __name__ == "__main__":
    try:
        print("Iniciando servidor Flask con TRANSACCIONES REALES de MySQL")
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