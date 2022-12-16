import pyodbc
def test():
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                        'Server=172.31.8.25;'
                        'Database=Voxtron_Callcenter;'
                        'Trusted_Connection=yes;')

    cursor = conn.cursor()
    cursor.execute('SELECT * FROM table_name')

    for i in cursor:
        print(i)