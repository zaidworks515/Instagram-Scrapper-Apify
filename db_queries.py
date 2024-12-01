import sqlite3

def create_connection():
    db_path = "db/database.sqlite"
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print('Database is connected')
        return conn, cursor
    except sqlite3.Error as e:
        print(f"Error: {e}")
        return None, None


def insert_data(session_id, file_path, creation_status):
    conn, cursor = create_connection()
    if not conn or not cursor:
        return

    insert_query = """
    INSERT INTO scrapped_data_path (session_id, file_path, creation_status)
    VALUES (?, ?, ?);
    """

    try:
        cursor.execute(insert_query, (session_id, file_path, creation_status))
        conn.commit()
        print(f"Data inserted successfully: {session_id}, {file_path}, {creation_status}")
    except sqlite3.Error as e:
        print(f"Error: {e}")
    finally:
        conn.close()

    
# insert_data('123', '/path/to/file.txt', 0)

def update_status(session_id, new_status, cloudinary_url):
    conn, cursor = create_connection()
    if not conn or not cursor:
        return

    update_query = """
    UPDATE scrapped_data_path
    SET creation_status = ?, cloudinary_url = ?
    WHERE session_id = ?;
    """

    try:
        cursor.execute(update_query, (new_status, cloudinary_url, session_id))
        conn.commit()

        if cursor.rowcount > 0:
            print(f"Status updated successfully for session_id '{session_id}' to '{new_status}'.")
        else:
            print(f"No record found with session_id '{session_id}'.")
    except sqlite3.Error as e:
        print(f"Error: {e}")
    finally:
        conn.close()


# update_status('123', 1, 'abcd.com')


def check_status(session_id):
    try:
        conn, cursor = create_connection()

        query = """
            SELECT creation_status, cloudinary_url 
            FROM scrapped_data_path 
            WHERE session_id = ? 
            ORDER BY created_at DESC 
            LIMIT 1
            """
        
        cursor.execute(query, (session_id,))

        result = cursor.fetchone()

        if result is not None:
            return {"session_id": session_id, "creation_status": result[0], "cloudinary_url": result[1]}
        else:
            return {"error": "Session ID not found"}

    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# status =check_status(session_id='123')
# print(status['creation_status'])

"""===============================================x==============================================="""

# def scrapped_data_path():
#     conn, cursor = create_connection()
#     if not conn or not cursor:
#         return

#     create_table_query = """
#     CREATE TABLE IF NOT EXISTS scrapped_data_path (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         session_id TEXT NOT NULL,
#         file_path TEXT NOT NULL,
#         cloudinary_url TEXT NULL,
#         creation_status BINARY NOT NULL, 
#         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#     );"""

#     try:
#         cursor.execute(create_table_query)
#         conn.commit()
#         print('Table is created')
#     except sqlite3.Error as e:
#         print(f"Error: {e}")
#     finally:
#         conn.close()

# scrapped_data_path()


# def delete_record(table_name):
#     conn, cursor = create_connection()
#     if not conn or not cursor:
#         return

#     delete_query = f"DELETE FROM {table_name};"

#     try:
#         cursor.execute(delete_query)
#         conn.commit()
#         print(f"All records deleted from table '{table_name}'.")
#     except sqlite3.Error as e:
#         print(f"Error: {e}")
#     finally:
#         conn.close()

# delete_record('scrapped_data_path')

