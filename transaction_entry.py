def get_product_list():
    
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

       #query = "select * from m_main_ledger_head"
       query = "select id,products_offered from m_product"

       abc = pd.read_sql(query, conn)
       
       json_final_data = abc.to_json(orient='records', date_format = 'iso')

       conn.close() 

       return json_final_data

    else:
   
       json_final_data = jsonify({"message": "ERROR: Unauthorized Access"}), 401

    return json_final_data 
