import pyodbc, logging, threading, datetime

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

    def isTxtCorrect(self, txtFile:str) -> bool:
        with open(f"{txtFile}", "r") as file: # File reading ------------------------------------------
            lineas = file.readlines()
            if "|" in lineas[-1]:
                self.logFile.info(f"File: '{txtFile}' was loaded.")
                return True
            else:
                self.logFile.error(f"File: '{txtFile}' is not valid.")
                return False

    def validateTxt(self, txtFile:str):
        try: # Database connection --------------------------------------------------------------------
            if self.finalconfig[3].lower()=="yes":
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE={self.finalconfig[2]};Trusted_Connection={self.finalconfig[3]};')

            else:
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE={self.finalconfig[2]};UID={self.finalconfig[4]};PWD={self.finalconfig[5]}')

            cursor = conn.cursor()
            self.logFile.info(f"Database connection succeeded")

        except pyodbc.Error as e:
            self.logFile.error(f"Database connection failed (func:validateTxt): {e}")

        with open(f"{txtFile}", "r") as file: # File reading ------------------------------------------
            lineas = file.read().strip().split('\n')
            fileArray = []

            for linea in lineas[2:]:
                valores = linea.split('|')
                fileArray.append(valores)

            fileDate = fileArray[0][0]
            fileArray.pop(0)
            
        # SQL sentences -------------------------------------------------------------------------------
            
        notFoundQuery1 = []
        notFoundQuery2 = []

        try: 
            query1 = "select codcue, nombre from CUENTAS where codcue=?"

            for i in range(len(fileArray)):
                try:
                    if fileArray[i][2]!='':
#                       actualQuery = query1 + fileArray[i][2]
                        
                        cursor.execute(query1, (fileArray[i][2],))

                        if cursor.fetchall()==[]:
                            notFoundQuery1.append(fileArray[i][2])
                            self.logFile.error(f"SQL statement executed (NOT FOUND): {query1} with params: {fileArray[i][2]}")

                        else:
                            self.logFile.info(f"SQL statement executed: {query1} with params: {fileArray[i][2]}")

                except pyodbc.Error as e:
                    self.logFile.error(f"SQL statement executed (ERROR): {query1} with params: {fileArray[i][2]} ({e})")
                    notFoundQuery1.append(fileArray[i][2])
                    continue
        
            query2 = "select codcen, nombre from CENTROSAP where codcen=?"

            for i in range(len(fileArray)):
                try:
                    if fileArray[i][10]!='':
#                       actualQuery = query2 + fileArray[i][10]
                        
                        cursor.execute(query2, (fileArray[i][10],))

                        if cursor.fetchall()==[]:
                            notFoundQuery2.append(fileArray[i][10])
                            self.logFile.error(f"SQL statement executed (NOT FOUND): {query2} with params: {fileArray[i][10]}")

                        else:
                            self.logFile.info(f"SQL statement executed: {query2} with params: {fileArray[i][10]}")

                except pyodbc.Error as e:
                    self.logFile.error(f"SQL statement executed (ERROR): {query2} with params: {fileArray[i][10]} ({e})")
                    notFoundQuery2.append(fileArray[i][10])
                    continue

            return notFoundQuery1==[], notFoundQuery2==[], notFoundQuery1, notFoundQuery2, fileDate

        except pyodbc.Error as e:
            pass
        finally:
            cursor.close()
            conn.close()
            self.logFile.info("Database connection closed.")

    def importarICR(self, txtFile:str, observacion:str) -> bool:
        try: # ICR Database connection --------------------------------------------------------------------
            if self.finalconfig[3].lower()=="yes":
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE=ICR;Trusted_Connection={self.finalconfig[3]};')

            else:
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE=ICR;UID={self.finalconfig[4]};PWD={self.finalconfig[5]}')

            cursor = conn.cursor()
            self.logFile.info(f"Database connection succeeded")

        except pyodbc.Error as e:
            self.logFile.error(f"Database connection failed (func:importarICR): {e}")

        procedureStatus = False

        try:
            fileElementsArray = []
            with open(txtFile, "r") as file:
                lineas = file.read().strip().split('\n')

                for linea in lineas[3:]:
                    valores = linea.split('|')
                    fileElementsArray.append(valores)

                if observacion!='' and fileElementsArray[-1][-1]!='':
                    for i in fileElementsArray:
                        i[-1] = ''

            fechaRow = ''
            with open(txtFile, "r") as readFile:
                fechaRow = readFile.readlines()[2]

            with open(txtFile, "w") as file:
                file.seek(0)
                file.write(f"[REGISTRO]\nASIENTO\n{fechaRow}")

                # Unificar el array y convertirlo en el formato del txt
                lines = '\n'.join(['|'.join(row) for row in fileElementsArray])

                file.write(lines)

                file.close()

            with open(txtFile, 'r') as file:
                fileArray = file.readlines()

                fileArray.pop(0);fileArray.pop(0)

                fileArray[0] = fileArray[0].replace('\n', '\r\n')

                content = ""
                
                if fileElementsArray[-1][-1]=='':
                    for i in range(1, len(fileArray)):
                        fileArray[i] = fileArray[i].replace("\n", f"{observacion}\r\n")
                    
                    fileArray[-1] += observacion+'\r\n'

                fileArray[-1] = fileArray[-1].replace(' ', '\xa0')

                for i in fileArray:
                    content+=i

                query = f"insert into ICR.DBO.ENTRADA (codemp,codsuc,etiqueta,cuerpo) values (1,1,'ASIENTO','{content}')"
                cursor.execute(query)
                
            conn.commit()

            procedureStatus = True
            self.logFile.info(f"SQL statement executed: {query} with params: Txt File: '{txtFile}'")
            self.logFile.info(f"'{txtFile}' Successfully loaded to ICR")

        except pyodbc.Error as e:
            self.logFile.error(f"SQL statement executed (FAILED): {query} with params: Txt File: '{txtFile}' {e}")

        finally:
            cursor.close()
            conn.close()
            self.logFile.info("Database connection closed.")
            
            return procedureStatus
        
    def importarBAS(self, observacion, currentDatetime) -> str:
        try: # ICR Database connection --------------------------------------------------------------------
            if self.finalconfig[3].lower()=="yes":
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE={self.finalconfig[2]};Trusted_Connection={self.finalconfig[3]};')

            else:
                conn = pyodbc.connect(f'DRIVER={self.finalconfig[0]};SERVER={self.finalconfig[1]};DATABASE={self.finalconfig[2]};UID={self.finalconfig[4]};PWD={self.finalconfig[5]}')

            cursor = conn.cursor()
            self.logFile.info(f"Database connection succeeded")

        except pyodbc.Error as e:
            self.logFile.error(f"Database connection failed (func:importarICR): {e}")

        selectResult = None

        try:
            cursor.execute("SELECT CONVERT(DATETIME, ?) AS fecha", currentDatetime[0:-3])
            params = [datetime.datetime.strftime(cursor.fetchone().fecha,'%Y-%m-%d %H:%M:%S.%f')[0:-3]]

            cursor.execute("SELECT SUSER_SNAME() AS CurrentUser")
            params.append(cursor.fetchone()[0])

            query = """select  top 1 MAX(fechareg), nrotrans, NUMERO from TRANSAC where FECHAREG >= ? and FUENTE='DISCV' and username=? and NROTRANSELIM is null
                       group by NROTRANS,NUMERO
                       order by 1 desc"""

            cursor.execute(query, params)

            selectResult = cursor.fetchall()

            self.logFile.info(f"SQL statement executed: {query} with params: {params}")

            params = (observacion, selectResult[0][1])

            query2 = "update transac set observacion=? WHERE nrotrans = ?"

            cursor.execute(query2, params)

            conn.commit()

            self.logFile.info(f"SQL statement executed: {query2} with params: {params}")
            self.logFile.info(f"Successfully loaded to BAS")

        except pyodbc.Error as e:
            self.logFile.error(f"SQL statement executed (FAILED): {query} with params: {params} {e}")

        finally:
            cursor.close()
            conn.close()
            self.logFile.info("Database connection closed.")

            if selectResult==[]:
                selectResult = None
                self.logFile.error(f"SQL statement executed (NOT FOUND): {query} with params: {params}")
                return str(selectResult)

            return str(selectResult[0][2])

#♥