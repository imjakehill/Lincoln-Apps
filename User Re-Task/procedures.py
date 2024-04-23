import pyodbc, logging, threading

from datetime import datetime

class SQLServer(object):
    
    def __init__(self) -> None:
        txt = open("config_connection.txt", "r")
        configaux=[]
        self.finalconfig=[]

        for i in txt.readlines():
            if i!="\n":
                configaux.append(i.split("=").pop(1))

        for i in configaux:
            self.finalconfig.append(i.split(";").pop(0))

        self.logFile = logging.getLogger('database_information')
        self.logFile.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        file_handler = logging.FileHandler('database_information.log')
        file_handler.setFormatter(formatter)

        self.logFile.addHandler(file_handler)

    def llenarComboBoxUsers(self):
        try:
            if self.finalconfig[3].lower()=="yes":
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE={self.finalconfig[2]};Trusted_Connection={self.finalconfig[3]};')

            else:
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE={self.finalconfig[2]};UID={self.finalconfig[4]};PWD={self.finalconfig[5]}')

            cursor = conn.cursor()
            self.logFile.info(f"Database connection succeeded")

        except pyodbc.Error as e:
            self.logFile.error(f"Database connection failed (func:llenarComboboxUsers): {e}")

        query = """select USERNAME, NOMBRE from USUARIOS a where a.USERNAME in 
                    (select usuario1 from CMPAUTORIZACION where usuario1=a.username union all 
                    select USUARIO2 from CMPAUTORIZACION where usuario2=a.username union all select USUARIO3 from CMPAUTORIZACION where usuario3=a.username)"""

        try:
            cursor.execute(query)
            usersCombobox = []

            for i in cursor.fetchall():
                usersCombobox.append(i[0].strip( ))
            
            rows_affected = cursor.rowcount
            self.logFile.info(f"SQL statement executed: {query} (Row count affected: {rows_affected})")

            query = "select username from usuarios"
            
            cursor.execute(query)
            newUsersCombobox = []

            for i in cursor.fetchall():
                newUsersCombobox.append(i[0].strip( ))
            
            rows_affected = cursor.rowcount
            self.logFile.info(f"SQL statement executed: {query} (Row count affected: {rows_affected})")

            return usersCombobox, newUsersCombobox

        except pyodbc.Error as e:
            self.logFile.error(f"SQL statement executed (FAILED): {query} ({e})")

        finally:
            cursor.close()
            conn.close()
            self.logFile.info(f"Database connection closed")

    def llenarComboBoxNewUser(self):
        try:
            if self.finalconfig[3].lower()=="yes":
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE={self.finalconfig[2]};Trusted_Connection={self.finalconfig[3]};')

            else:
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE={self.finalconfig[2]};UID={self.finalconfig[4]};PWD={self.finalconfig[5]}')

            cursor = conn.cursor()
            self.logFile.info(f"Database connection succeeded")

        except pyodbc.Error as e:
            self.logFile.error(f"Database connection failed (func:llenarComboBoxNewUser): {e}")

        query = "Select username from usuarios"

        try:
            cursor.execute(query)
            usersCombobox = []

            for i in cursor.fetchall():
                usersCombobox.append(i[0].strip( ))
            
            rows_affected = cursor.rowcount
            self.logFile.info(f"SQL statement executed: {query} (Row count affected: {rows_affected})")
            return usersCombobox

        except pyodbc.Error as e:
            self.logFile.error(f"SQL statement executed (FAILED): {query} ({e})")

        finally:
            cursor.close()
            conn.close()
            self.logFile.info(f"Database connection closed")

    def selectUserNotif(self, userComboboxText):
        try:
            if self.finalconfig[3].lower()=="yes":
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE={self.finalconfig[2]};Trusted_Connection={self.finalconfig[3]};')

            else:
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE={self.finalconfig[2]};UID={self.finalconfig[4]};PWD={self.finalconfig[5]}')

            cursor = conn.cursor()
            self.logFile.info(f"Database connection succeeded")

        except pyodbc.Error as e:
            self.logFile.error(f"Database connection failed (func:selectUserNotif): {e}")

        query = f"SELECT IDNOTIF FROM NOTIFICACIONESUSU WHERE USUARIO='{userComboboxText}' and completada=0"

        try:
            
            cursor.execute(query)
            idnotif = cursor.fetchall()

            rows_affected = cursor.rowcount
            self.logFile.info(f"SQL statement executed: {query} (Row count affected: {rows_affected})")
            return idnotif

        except pyodbc.Error as e:
            self.logFile.error(f"SQL statement executed (FAILED): {query} ({e})")

        finally:
            cursor.close()
            conn.close()
            self.logFile.info(f"Database connection closed")

    def updateUserTask(self, userComboboxText, newUserComboboxText, dateUser, dateNewUser):   
        try:
            if self.finalconfig[3].lower()=="yes":
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE={self.finalconfig[2]};Trusted_Connection={self.finalconfig[3]};')

            else:
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE={self.finalconfig[2]};UID={self.finalconfig[4]};PWD={self.finalconfig[5]}')

            cursor = conn.cursor()
            self.logFile.info(f"Database connection succeeded")

        except pyodbc.Error as e:
            self.logFile.error(f"Database connection failed (func:updateUserTask): {e}")

        try:
            Error=False

            cursor.execute("SELECT SUSER_SNAME() AS CurrentUser")
            currentUser = cursor.fetchone()[0]

            dateUser_obj = datetime.strptime(dateUser, '%d/%m/%Y').strftime('%Y-%m-%d')
            dateNewUser_obj = datetime.strptime(dateNewUser, '%d/%m/%Y').strftime('%Y-%m-%d')

            cursor.execute("SELECT CONVERT(DATE, ?) AS fecha", dateUser_obj)
            finalDateUser = cursor.fetchone().fecha
            cursor.execute("SELECT CONVERT(DATE, ?) AS fecha", dateNewUser_obj)
            finalDateNewUser = cursor.fetchone().fecha

            query = """insert into CMPAUTORIZACION_AUXAHS (USUARIOCARGABACKUP,USUARIOACAMBIAR,USUARIOBACKUP,FECHADESDE,FECHAHASTA) 
                    values (?, ?, ?, ?, ?)"""

            params = [currentUser, userComboboxText, newUserComboboxText, finalDateUser, finalDateNewUser]

            cursor.execute(query, params)
                
            rows_affected = cursor.rowcount
            self.logFile.info(f"SQL statement executed: {query} with params {params} (Row count affected: {rows_affected})")
            
            conn.commit()
            
            query = "exec SP_Servicio_Backup_Autorizantes ?, ?, ?, ?"
            params = [userComboboxText, newUserComboboxText, finalDateUser, finalDateNewUser]

            cursor.execute(query, params)

            rows_affected = cursor.rowcount
            self.logFile.info(f"SQL statement executed: {query} with params {params} (Row count affected: {rows_affected})")

            conn.commit()

        except pyodbc.Error as e:
            self.logFile.error(f"SQL statement executed (FAILED): {query} with params {params} ({e})")
            Error=True

        cursor.close()
        conn.close()
        self.logFile.info(f"Database connection closed")

        if Error:
            return False
        else:
            return True      

"""
CREATE PROCEDURE getUsers
AS
BEGIN
	SET NOCOUNT ON;
	-- Primera consulta para obtener usuarios de usuario1
    SELECT DISTINCT usuario1
    FROM CMPAUTORIZACION
    WHERE usuario1 IS NOT NULL
    GROUP BY usuario1

    UNION ALL

    -- Segunda consulta para obtener usuarios de usuario2
    SELECT DISTINCT usuario2
    FROM CMPAUTORIZACION
    WHERE usuario2 IS NOT NULL
    GROUP BY usuario2

    UNION ALL

    -- Tercera consulta para obtener usuarios de usuario3
    SELECT DISTINCT usuario3
    FROM CMPAUTORIZACION
    WHERE usuario3 IS NOT NULL
    GROUP BY usuario3
END;
"""