from flask import Flask
app = Flask(__name__)
@app.route("/home")
def home():
   return "Hello Himanshu"

@app.route("/check")
def check():
    '''
    The aim of this function is to check if the API is up
    '''
    return 'http 200 - I did it'

@app.route("/get_journal_data")
def get_journal_data():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == 'asoidewfoef':  
           
       db="MJK2018_2019"
       user="shsa"
       server="13.127.124.84,6016"
       password="Easeprint#021"
       port = "80"
       try:
           #conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db +';UID=' + user + ';PWD=' + password)
            conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)

       query = "select * from t_journal"

       abc = pd.read_sql(query, conn)    
       json_final_data = abc.to_json(orient='records', date_format = 'iso')

       conn.close()

       return json_final_data
    else:
       return jsonify({"message": "ERROR: Unauthorized Access"}), 401

@app.route("/get_all_ledger_head")
def get_journal_data():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == 'asoidewfoef':  
           
       db="MJK2018_2019"
       user="shsa"
       server="13.127.124.84,6016"
       password="Easeprint#021"
       port = "80"
       try:
           conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)

       query = "select MainLeadgerHeadId,MainLedgerHeads,LedgerHeadId,OrderNo from m_main_ledger_head"
       abc = pd.read_sql(query, conn)    
       json_final_data = abc.to_json(orient='records', date_format = 'iso')

       conn.close()
       return json_final_data
    else:
       return jsonify({"message": "ERROR: Unauthorized Access"}), 401
   
   
@app.route("/get_ledgers_basis_ledgerhead")
def get_journal_data(ledger_head_id):
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == 'asoidewfoef':  
           
       db="MJK2018_2019"
       user="shsa"
       server="13.127.124.84,6016"
       password="Easeprint#021"
       port = "80"
       try:
           conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)

       query = "select * from m_ledgermaster where LedgerHead = %s"%ledger_head_id
      
       abc = pd.read_sql(query, conn)    
       json_final_data = abc.to_json(orient='records', date_format = 'iso')

       conn.close()
       return json_final_data
    else:
       return jsonify({"message": "ERROR: Unauthorized Access"}), 401
   
