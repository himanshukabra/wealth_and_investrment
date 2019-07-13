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

# @app.route("/get_journal_data")
# def get_journal_data():
    
#     import pyodbc
#     import pandas as pd
#     import pandas.io.sql as psql
#     from flask import Flask, request, jsonify
      
#     headers = request.headers
#     auth = headers.get("X-Api-Key")
#     if auth == 'asoidewfoef':  
           
#        db="MJK2018_2019"
#        user="shsa"
#        server="13.127.124.84,6016"
#        password="Easeprint#021"
#        port = "80"
#        try:
#            #conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db +';UID=' + user + ';PWD=' + password)
#             conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
#        except Exception as e:
#            print(e)

#        query = "select * from t_journal"

#        abc = pd.read_sql(query, conn)    
#        json_final_data = abc.to_json(orient='records', date_format = 'iso')

#        conn.close()
   
#     else:
   
#        json_final_data = jsonify({"message": "ERROR: Unauthorized Access"}), 401

#     return json_final_data

@app.route("/get_all_ledger_heads")
def get_all_ledger_heads():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == 'asoidewfoef':  
       
       data = []
       data = {'dbname':request.json['dbname']}
         
       db=data['dbname']
       user="shsa"
       server="13.127.124.84,6016"
       password="Easeprint#021"
       port = "80"
       try:
           conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)

       query = "select * from m_main_ledger_head"

       abc = pd.read_sql(query, conn)    
       json_final_data = abc.to_json(orient='records', date_format = 'iso')

       conn.close()
   
    else:
   
       json_final_data = jsonify({"message": "ERROR: Unauthorized Access"}), 401

    return json_final_data

@app.route("/get_ledger_master")
def get_ledger_master():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == 'asoidewfoef':  
       
       data = []
       data = {'dbname':request.json['dbname']
               'ledgerhead'request.json['ledgerhead']}
         
       db=data['dbname']
       user="shsa"
       server="13.127.124.84,6016"
       password="Easeprint#021"
       port = "80"
       try:
           conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)

       query = "select tableid,ledgername,ledgerhead from m_ledgermaster where ledgerhead = "%data['ledgerhead']

       abc = pd.read_sql(query, conn)    
       json_final_data = abc.to_json(orient='records', date_format = 'iso')

       conn.close()
   
    else:
   
       json_final_data = jsonify({"message": "ERROR: Unauthorized Access"}), 401

    return json_final_data
