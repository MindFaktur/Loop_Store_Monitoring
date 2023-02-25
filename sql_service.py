import sqlite3


DB_CONN = sqlite3.connect("stores.db", check_same_thread=False)
CURSOR = DB_CONN.cursor()


def execute_sql(query, values=[], cursor_query=False):
    response = ""
    if not cursor_query:
        try:
            response = DB_CONN.execute(query)
        except Exception as e:
            print(e)
    else:
        try:
            response = CURSOR.executemany(query, values)
        except Exception as e:
            print(e)
    DB_CONN.commit()
    return response
