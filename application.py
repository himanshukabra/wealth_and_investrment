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

@app.route('/get_all_ledger_heads', methods=['POST'])
def get_all_ledger_heads():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
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

    #query = "select * from m_main_ledger_head"
    query = "select 0 as MainLeadgerHeadId,'--Select One--' as MainLedgerHeads,0 as LedgerHeadId,0 as OrderNo,'false' as IsReserved union select * from m_main_ledger_head"

    abc = pd.read_sql(query, conn)    
    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    conn.close() 

    return json_final_data

@app.route('/get_ledger_master', methods=['POST'])
def get_ledger_master():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == 'asoidewfoef':  
       
       data = []
       data = {'dbname':request.json['dbname'],
               'ledgerhead':request.json['ledgerhead']}
         
       db=data['dbname']
       user="shsa"
       server="13.127.124.84,6016"
       password="Easeprint#021"
       port = "80"
       try:
           conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)

       query = "select 0 as tableid,'--Select One--' as ledgername," + str(data['ledgerhead']) + " as ledgerhead union select tableid,ledgername,ledgerhead from m_ledgermaster where ledgerhead = %s"%data['ledgerhead']

       abc = pd.read_sql(query, conn)    
       json_final_data = abc.to_json(orient='records', date_format = 'iso')

       conn.close()
   
    else:
   
       json_final_data = jsonify({"message": "ERROR: Unauthorized Access"}), 401

    return json_final_data

@app.route('/get_ledger', methods=['POST'])
def get_ledger():
    
       import pyodbc
       import pandas as pd
       import pandas.io.sql as psql
       from flask import Flask, request, jsonify

       headers = request.headers
       auth = headers.get("X-Api-Key")
       if auth == 'asoidewfoef':  

          data = []
          data = {'dbname':request.json['dbname'],
                  'ledgerhead':request.json['ledgerhead'],
                  'ledgeraccount':request.json['ledgeraccount'],
                  'from_date':request.json['from_date'],
                  'to_date':request.json['to_date']}

          db=data['dbname']
          user="shsa"
          server="13.127.124.84,6016"
          password="Easeprint#021"
          port = "80"
          try:
              conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
          except Exception as e:
              print(e)

          query = "exec USP_R_Ledger 1,%s,%s,%s,%s"%(data['ledgerhead'],data['ledgeraccount'],data['from_date'],data['to_date'])

          abc = pd.read_sql(query, conn)    
          json_final_data = abc.to_json(orient='records', date_format = 'iso')

          conn.close()

       else:

          json_final_data = jsonify({"message": "ERROR: Unauthorized Access"}), 401

       return json_final_data

@app.route('/get_trial_balance', methods=['POST'])
def get_trial_balance():
    
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

          query = "exec Usp_R_TrialBalaceNav 0"

          abc = pd.read_sql(query, conn)    
          json_final_data = abc.to_json(orient='records', date_format = 'iso')

          conn.close()

       else:

          json_final_data = jsonify({"message": "ERROR: Unauthorized Access"}), 401

       return json_final_data
