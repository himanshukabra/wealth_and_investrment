def get_product(dbname):
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    db=dbname
    user="shsa"
    server="13.127.124.84,6016"
    password="Easeprint#021"
    port = "80"
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)

    except Exception as e:
        print(e)

    query = "select id,products_offered from m_product"

    abc = pd.read_sql(query, conn)

    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    conn.close() 

    return json_final_data

def get_scheme(dbname,product_id):
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    db=dbname
    user="shsa"
    server="13.127.124.84,6016"
    password="Easeprint#021"
    port = "80"
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)

    except Exception as e:
        print(e)

    query = "select * from (select Table_Id,ScripName,Product_Id from M_Equity union select id as tableid,bond_name as scripname,3 as Product_Id from M_Bonds union select Table_Id,Scheme_Name as scripname,Product_Id from M_Mutual_Funds) t where Product_Id = %s"%product_id

    abc = pd.read_sql(query, conn)

    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    conn.close() 

    return json_final_data

def get_broker(dbname):
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    db=dbname
    user="shsa"
    server="13.127.124.84,6016"
    password="Easeprint#021"
    port = "80"
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)

    except Exception as e:
        print(e)

    query = "select id,broker_name,isnull(account_ledger,0) as account_ledger,isnull(demat_id,0) as demat_id from m_broker"

    abc = pd.read_sql(query, conn)

    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    conn.close() 

    return json_final_data

def get_demat(dbname,broker_id):
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    db=dbname
    user="shsa"
    server="13.127.124.84,6016"
    password="Easeprint#021"
    port = "80"
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)

    except Exception as e:
        print(e)

    query = "select broker_id,id,demat_account_number from m_demat_accounts where broker_id = %s union all select 0 as broker_id,0 as id,0  as demat_Account_number"%broker_id

    abc = pd.read_sql(query, conn)

    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    conn.close() 

    return json_final_data
