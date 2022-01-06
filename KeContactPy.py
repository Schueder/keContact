from pyModbusTCP.client import ModbusClient
import time
import sqlite3

class keContact:
     """
     The keContact class helps you, to connect to KEBA Wallbox of the type KC-P30 x-series and c-series
     via Modbus TCP.
     even in this class, some helpfull methods to save the received data to an SQLite db-File.
     
     By define the class object, the IP-Adress of your KEBA Wallbox is nessecarry.
     optional parameters:
          sqlFileName which defines the name of the file, which is keConnect.db in default
          port of tcp-host, which is 502 in default
          
     code sample:
          from KeContactPy import keContact
          keContact = keContact("192.168.1.42","myDbFile.db");
     
     NEEDED SIDE PACKAGES: time, sqlite3, pyModbusTCP     
     """
     def __init__(self, hostIp, sqlFileName = 'keConnect.db',  hostPort = 502):
          self.SERVER_HOST = hostIp
          self.SERVER_PORT = hostPort
          self.sqlFile = sqlFileName
          self.cntSessions = 0;
          self.cntLogs = 0;
          
          self.Phase1 = {'Voltage':-1.0,'Current':-1.0}
          self.Phase2 = {'Voltage':-1.0,'Current':-1.0}
          self.Phase3 = {'Voltage':-1.0,'Current':-1.0}
          self.States = {'Charging':-1,'Cable':-1,'Error':-1}
          self.Data = {'Serial':-1,'ProductType':-1,'Firmware':-1,'rfid':-1}
          self.Power={'TotalEnergy':-1,'Active':-1,'Factor':-1,'Charged':-1}
          self.Maximum = {'ChargingCurrent':-1,'SupportedCurrent':-1}
          
     def initModbusClient(self):
          '''
          the initModbusClient mehtod, will connect to the given ip-address and port.
          No parameters needed.
          in case of the connection fail, an error will be printed to the shell.
          code sample:
               keContact.initModbusClient()
          '''
          self.client = ModbusClient()
          # uncomment this line to see debug message
          # self.client.debug(True)
          self.client.host(self.SERVER_HOST)
          self.client .port(self.SERVER_PORT)
          
          if not self.client.is_open():
               if not self.client.open():
                    print("ERROR: Could not open Modbus connection");

     def createSqlFile(self, filePrefix=""):
          '''
          the Method createSqlFile creates a new db-file which will be used in the class until the next new
          file will be created.
          A parameter can be given to the method, which is a prefix of the file name.
          This prefix will be set infront of the given sqlFileNamewhile init the Class. (default empty string)
          code sample:
               keContact.createSqlFile("prefix_");
               # db-file : prefix_myDbFile.db will be created and tables will be defined and insert to the file.
          '''
          self.sqlConnect = sqlite3.connect(str(filePrefix) + self.sqlFile)
          self.cursor = self.sqlConnect.cursor()
          # TODO: Check if tables exists
          self.cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='idx' ''')

          if self.cursor.fetchone()[0]==1 : 
               print("INFO createSqlFile(): Tables already existing")
          
          else:
               self.cursor.execute('''CREATE TABLE idx(date text, idx INTEGER PRIMARY KEY)''')
               
               self.cursor.execute('''CREATE TABLE parmeter(
                                                  idx INTEGER PRIMARY KEY,
                                                  Date TEXT,
                                                  SerialNumber TEXT,
                                                  ProductType TEXT,
                                                  Firmware TEXT,
                                                  MaxSupportedCurrent REAL,
                                                  RFID INTEGER)''')
               
               self.cursor.execute('''CREATE TABLE states(
                                                  idx INTEGER PRIMARY KEY,
                                                  loadSession INTEGER,
                                                  Date TEXT,
                                                  Charging INTEGER,
                                                  CableState INTEGER,
                                                  ErrorState INTEGER)''')
                                                  
               self.cursor.execute('''CREATE TABLE phases(
                                                  idx INTEGER PRIMARY KEY,
                                                  loadSession INTEGER,
                                                  Date TEXT,
                                                  Phase1Voltage INTEGER,
                                                  Phase1Current REAL,
                                                  Phase2Voltage INTEGER,
                                                  Phase2Current REAL,
                                                  Phase3Voltage INTEGER,
                                                  Phase3Current REAL)''')
                                                  
               self.cursor.execute('''CREATE TABLE power(
                                                  idx INTEGER PRIMARY KEY,
                                                  loadSession INTEGER,
                                                  Date TEXT,
                                                  PowerFactor REAL,
                                                  PowerActive REAL,
                                                  PowerTotal REAL,
                                                  PowerCharged REAL,
                                                  MaxChargingCurrent REAL)''')
               
          self.__getCounts()

     def __countLogUp(self):
          self.cntLogs = self.cntLogs + 1
          
     def __getCounts(self):
          self.cursor.execute("SELECT * FROM idx WHERE idx=(SELECT max(idx) FROM idx)")
          for dat in self.cursor:
               self.cntSessions = dat[1]

     def newLoadSession(self):
          """
          the index (count) of the sessions is stored in the db-file.
          after init the number of sessione will be read.
          This method can be used, to increase the number of sessions by 1.
          After increase the number of sessions, it will be written to the db-file.
          """
          self.cntSessions = self.cntSessions + 1
          self.cursor.execute("INSERT INTO idx VALUES ('"+self.__now()+"',"+str(self.cntSessions)+")")
          self.sqlConnect.commit()
          
     def getParameters(self):
          """
          This method read following parameters of the wallbox:
               Name                          register     length    unit
               -------------------------------------------------------------------
               serialNumber               1014           2             -
               ProductType                 1016           2             -
               Firmware                      1018           2             -
               RFID                              1500           2             -
               SupportedCurrent        1110           2            A
               
          if the data are received, they will be stored to the class object and can be used in your code.
          if the connection to the wallbox is not opened, -1 will be returned and an error will be
          printed to the shell.
          code sample:
                keContact.getParameters()
                print(keContact.Data['Serial'])
                print(keContact.Data['ProductType'])
                print(keContact.Data['Firmware'])
                print(keContact.Data['rfid'])
                print(keContact.Maximum['SupportedCurrent'])
          """  
          if not self.client.is_open():
               print("ERROR getStaticData(): Modbus TCP client not connected");
               return -1
          self.Data['Serial'] = self.client.read_holding_registers(1014, 2)[1]
          self.Data['ProductType']  = self.client.read_holding_registers(1016, 2)[1]
          self.Data['Firmware'] = self.client.read_holding_registers(1018, 2)[1]
          self.Data['rfid'] = self.client.read_holding_registers(1500, 2)[1]
          self.Maximum['SupportedCurrent'] = self.client.read_holding_registers(1110, 2)[1]/1000

     def getStates(self):
          """
          This method read following state-variables of the wallbox:
               Name                          register     length    unit
               -------------------------------------------------------------------
               Charging State             1000           2             -
               Cable State                   1004           2             -
               Error State                    1006           2             -
               
          if the data are received, they will be stored to the class object and can be used in your code.
          if the connection to the wallbox is not opened, -1 will be returned and an error will be
          printed to the shell.
          code sample:
                keContact.getStates()
                print(keContact.States['Charging'])
                print(keContact.States['Cable'])
                print(keContact.States['Error'])
          """  
          if not self.client.is_open():
               print("ERROR getStates(): Modbus TCP client not connected");
               return -1
          self.States["Charging"] = self.client.read_holding_registers(1000, 2)[1]
          self.States['Cable'] = self.client.read_holding_registers(1004, 2)[1]
          self.States['Error'] = self.client.read_holding_registers(1006, 2)[1]

     def getPhases(self):
          """
          This method read following phase-variables for all three phases of the wallbox:
               Name                          register     length    unit
               -------------------------------------------------------------------
               phase1 Current              1000           2             A
               phase1 Voltage              1040           2             V
               phase2 Current              1008           2             A
               phase2 Voltage              1042           2             V
               phase3 Current              1010           2             A
               phase3 Voltage              1044           2             V
               
          if the data are received, they will be stored to the class object and can be used in your code.
          if the connection to the wallbox is not opened, -1 will be returned and an error will be
          printed to the shell.
          code sample:
                keContact.getPhases()
                print(keContact.Phase1['Current'])
                print(keContact.Phase1['Voltage'])
                print(keContact.Phase2['Current'])
                print(keContact.Phase2['Voltage'])
                print(keContact.Phase3['Current'])
                print(keContact.Phase3['Voltage'])
          """  
          if not self.client.is_open():
               print("ERROR getPhases(): Modbus TCP client not connected");
               return -1
          self.Phase1['Current'] = self.client.read_holding_registers(1006, 2)[1]/1000
          self.Phase2['Current'] = self.client.read_holding_registers(1008, 2)[1]/1000
          self.Phase3['Current'] = self.client.read_holding_registers(1010, 2)[1]/1000
          self.Phase1['Voltage'] = self.client.read_holding_registers(1040, 2)[1]
          self.Phase2['Voltage'] = self.client.read_holding_registers(1042, 2)[1]
          self.Phase3['Voltage'] = self.client.read_holding_registers(1044, 2)[1]

     def getPower(self):
          """
          This method read following power informatins of the wallbox:
               Name                             register     length    unit
               -------------------------------------------------------------------
               Power factor                   1000           2             %
               Active power                   1040           2             W
               Total energy                    1008           2             Wh
               Charged energy               1042          2             Wh
               Max. charging Current     1013         2             A
               
          if the data are received, they will be stored to the class object and can be used in your code.
          if the connection to the wallbox is not opened, -1 will be returned and an error will be
          printed to the shell.
          code sample:
                keContact.getPower()
                print(keContact.Power['Factor'])
                print(keContact.Power['Active'])
                print(keContact.Power['TotalEnergy'])
                print(keContact.Power['Charged'])
                print(keContact.Maximum['ChargingCurrent'])
          """  
          if not self.client.is_open():
               print("ERROR getPower(): Modbus TCP client not connected");
               return -1
          self.Power['Factor'] = self.client.read_holding_registers(1046, 2)[1]/10
          self.Power['Active'] = self.client.read_holding_registers(1020, 2)[1]/1000
          self.Power['TotalEnergy'] = self.client.read_holding_registers(1036, 2)[1]/1000
          self.Power['Charged'] = self.client.read_holding_registers(1502, 2)[1]
          self.Maximum['ChargingCurrent'] = self.client.read_holding_registers(1100, 2)[1]/1000

     def getModbusData(self):
          """
          This method will call all three classes, which will read all not static values from
          the modbus TCP.
          After run the method, the states, phases, and power datas will be updated.

          code sample:
                keContact.getModbusData()
               
          """
          self.getStates()
          self.getPhases()
          self.getPower()
          
     def __sqlWriteParameters(self):
          self.cursor.execute(f'''INSERT INTO parmeter VALUES(
                                             {self.cntLogs},
                                             "{self.__now()}",
                                             {self.Data['Serial']},
                                             {self.Data['ProductType']},
                                             {self.Data['Firmware']},
                                             {self.Maximum['SupportedCurrent']},
                                             {self.Data['rfid']})''')
          self.sqlConnect.commit()
          
     def __sqlWriteState(self):
       self.cursor.execute(f'''INSERT INTO states VALUES(
                                             {self.cntLogs},
                                             {self.cntSessions},
                                              "{self.__now()}",
                                              {self.States['Charging']},
                                              {self.States['Cable']},
                                              {self.States['Error']})''')
       self.sqlConnect.commit()

     def __sqlWritePhases(self):
          self.cursor.execute(f'''INSERT INTO phases VALUES(
                                             {self.cntLogs},
                                             {self.cntSessions},
                                              "{self.__now()}",
                                             {self.Phase1['Voltage']},{self.Phase1['Current']},
                                             {self.Phase2['Voltage']},{self.Phase1['Current']},
                                             {self.Phase3['Voltage']},{self.Phase1['Current']})''')
          self.sqlConnect.commit()
          
     def __sqlWritePower(self):
          self.cursor.execute(f'''INSERT INTO power VALUES(
                                             {self.cntLogs},
                                             {self.cntSessions},
                                              "{self.__now()}",
                                              {self.Power['Factor']},
                                              {self.Power['Active']},
                                              {self.Power['TotalEnergy']},
                                              {self.Power['Charged']},
                                              {self.Maximum['ChargingCurrent']})''')
          self.sqlConnect.commit()
          
     def sqlWriteData(self):
          """
          This method will increase the data index by 1, and will write all modbus data to the
          sql-db-file.
          """
          self.__countLogUp()
          self.__sqlWriteParameters()
          self.__sqlWriteState()
          self.__sqlWritePhases()
          self.__sqlWritePower()

     def __now(self):
          return time.ctime()

     def sqlComplete(self):
          """
          Call this method to close the connection to the sql-db-file
          """
          self.sqlConnect.commit()
          self.sqlConnect.close()
    
