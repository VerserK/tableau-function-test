import pyodbc
def test():
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                        'Server=172.31.8.25;'
                        'Database=Voxtron_Callcenter;'
                        'UID=boon;'
                        'PWD=Boon@DA123;'
                        'Trusted_Connection=yes;')

    cursor = conn.cursor()
    cursor.execute('SELECT * FROM VCC_logs')

    for i in cursor:
        print(i)