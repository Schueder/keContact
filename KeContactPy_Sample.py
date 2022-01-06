import time
from KeContactPy import keContact

# Create new keContact
keContact = keContact("192.168.188.93");
# Create sql-file with prefix KC-P30_ -> result will be KC-P30_keContact.db
keContact.createSqlFile('KC-P30_');
# Create new (first) LoadSession
keContact.newLoadSession();
# Init and connect Modbus TCP
keContact.initModbusClient();
# read paramteers of the Wallbox
keContact.getParameters();

cnt = 0;

# always run this code or stop after e.g. cnt>1000 -> Change line to while cnt > 1000:
while True:
     # Read states to start or stop data collection
     keContact.getStates()
     
     # Run data collection if charging is in progress
     if keContact.States["Charging"] >=3:
          # Create new (next) loading session
          keContact.newLoadSession();
          # Read all data and write to SQL-file
          keContact.getModbusData()
          keContact.sqlWriteData()
     
     time.sleep(1)    # acc. manuel, don't sample faster than 500ms
     cnt = cnt + 1

# Complete sql connection
keContact.sqlComplete()     
