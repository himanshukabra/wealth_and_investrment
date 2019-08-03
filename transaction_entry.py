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

    query = "select id,products_offered from m_product union select 0,'--Select One--' "

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

    query = "select top 100 * from (select Table_Id,ScripName,Product_Id from M_Equity union select id as tableid,bond_name as scripname,3 as Product_Id from M_Bonds union select Table_Id,Scheme_Name as scripname,Product_Id from M_Mutual_Funds union select 0 as TableId, '--Select One--',%s as product_id ) t where Product_Id = %s"%(product_id,product_id)

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

    query = "select id,broker_name,isnull(account_ledger,0) as account_ledger,isnull(demat_id,0) as demat_id from m_broker union select 0 as id, '--Select One--' as broker_name, 0 as account_ledger, 0 as demat_id"

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

    query = "Select top 1 broker_id,id,demat_account_number from (select broker_id,id,demat_account_number from m_demat_accounts where broker_id = %s union all select 0 as broker_id,0 as id,0  as demat_Account_number) t order by t.broker_id desc"%broker_id

    abc = pd.read_sql(query, conn)

    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    conn.close() 

    return json_final_data

def get_scrip_opening(dbname,scrip_id,product_id,folio):
    
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

    query = "select top 1 * from (select product_id,scrip_Id,folio_number,sum(quantity) as closing_quanity from ( select product_id,script_id as scrip_Id,isnull(folio_number,0) as folio_number, sum(case when transaction_type = 'Buy' then quantity when transaction_type = 'Sell' then -quantity end) as quantity from t_transaction_register where script_id = %s and product_id = %s and folio_number = '%s' group by product_id,script_id,isnull(folio_number,0) union select product_id,scrip_Id,isnull(folio_number,'0') as folio_number,sum(quantity) as quantity from t_opening_balance_products where scrip_id = %s and product_id = %s and folio_number = '%s' group by product_id,scrip_Id,isnull(folio_number,'0') ) t group by product_id,scrip_Id,folio_number union select 0 as product_id,0 as script_id,'0' as folio_number,0 as closing_quantity) t order by scrip_Id desc"%(scrip_id,product_id,folio,scrip_id,product_id,folio)

    abc = pd.read_sql(query, conn)

    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    conn.close() 

    return json_final_data

def get_folios(dbname,scrip_id,product_id):
    
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

    query = "select distinct product_id,scrip_id,folio_number from (select product_id,scrip_id,folio_number from T_Opening_Balance_Products union select product_id,script_id as scrip_id,folio_number from t_transaction_register ) t where folio_number is not null and product_id = %s and scrip_id = %s union select %s as product_id,%s as scrip_id,'--Select One--' as folio_number"%(product_id,scrip_id,product_id,scrip_id)

    
    abc = pd.read_sql(query, conn)

    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    conn.close() 

    return json_final_data

def insert_temp_transaction_register(dbname,product_id,scrip_id,folio_number,transaction_type,quantity,gross_rate,gross_amount,brokerage,stt,net_rate,user_name,computer_name):
    
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
    cur = conn.cursor()
    query = "INSERT INTO [t_transaction_api_temp] ([product_id],[script_id],[folio_number],[rate],[transaction_type],[quantity],[gross_rate],[gross_amount],[brokerage],[stt],[net_rate],[user],[computer_name]) VALUES (%s,%s,'%s',%s,'%s',%s,%s,%s,%s,%s,%s,'%s','%s')"%(product_id,scrip_id,folio_number,gross_rate,transaction_type,quantity,gross_rate,gross_amount,brokerage,stt,net_rate,user_name,computer_name)

    a = cur.execute(query)
    cur.commit()

    check_e = a.rowcount
    if check_e>=1:
       val = "Saved Successfully"
    else:
       val = "Data not saved"

    conn.close() 
    return jsonify({"response": val}), 200

def get_temp_transaction(dbname,user_name,computer_name):
    
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

    query = "select tableid, product_id, script_id as scrip_id, folio_number,rate,transaction_type,quantity,gross_rate,gross_amount,brokerage,stt,net_rate,[user] as user_name,computer_name from t_transaction_api_temp where [user] = '%s' and computer_name = '%s'"%(user_name,computer_name)

    abc = pd.read_sql(query, conn)

    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    conn.close() 

    return json_final_data

def delete_temp_transaction(dbname,tableid):
    
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
    cur = conn.cursor()
    query = "delete from t_transaction_api_temp where tableid = %s"%(tableid)

    a = cur.execute(query)
    cur.commit()

    check_e = a.rowcount
    if check_e>=1:
       val = "Deleted Successfully"
    else:
       val = "Data not Deleted"

    conn.close() 
    return jsonify({"response": val}), 200

def get_total_for_temp_transaction(dbname,user_name,computer_name):
    
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
    cur = conn.cursor()
    query = "select isnull(sum(isnull(gross_amount,0)),0) as gross_amount,isnull(sum(isnull(brokerage,0)),0) as total_brokerage, isnull(sum(isnull(stt,0)),0) as stt_amount from t_transaction_api_temp where [user] = '%s' and computer_name = '%s'"%(user_name,computer_name)

    abc = pd.read_sql(query, conn)

    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    conn.close() 

    return json_final_data

def get_product_ledger_list(dbname):
    
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
    cur = conn.cursor()
    query = "select ledgername,tableid as ledger_id,ledgerhead from m_ledgermaster where ledgername in ('INVESTMENT IN EQUITY','INTEREST FROM BONDS/DEBENTURES (TAX FREE)','INVESTMENT IN BONDS/DEBENTURES','INVESTMENT IN MUTUAL FUND') and ledgerhead is not null"

    abc = pd.read_sql(query, conn)

    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    conn.close() 

    return json_final_data

def insert_data_in_at_from_transaction_entry(dbname,sp_type,date_of_transaction,account_head,account_ledger,DRCR,voucher_number,standard_description,amount,ledgerhead1,ledger_account,computer_name,created_by):
    
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
    cur = conn.cursor()
    query = "exec Usp_T_Insert_in_Account_Transaction 1,%s,'%s',%s,%s,'%s','%s',4,'%s','%s','%s','%s',%s,%s,%s,%s,'&s','%s','%s'"%(sp_type,date_of_transaction,account_head,account_ledger,DRCR,voucher_number,standard_description,standard_description,standard_description,standard_description,amount,ledgerhead1,ledger_account,ledger_account,computer_name,created_by)

    a = cur.execute(query)
    cur.commit()

    check_e = a.rowcount
    if check_e>=1:
       val = "Saved Successfully"
    else:
       val = "Data not saved"

    conn.close() 
    return val

def get_temp_data_from_transaction_register(dbname,user_name,computer_name):

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

    query = "select product_id, script_id as scrip_id, folio_number,rate,transaction_type,quantity,gross_rate,gross_amount,brokerage,stt,net_rate,[user] as user_name,computer_name from t_transaction_api_temp where [user] = '%s' and computer_name = '%s'"%(user_name,computer_name)

    abc = pd.read_sql(query, conn)

    conn.close()  

    return abc

def insert_final_data_in_transaction_register(dbname,date_of_transaction,broker_id,demat_id,contract_number,reference_number,product_id,scrip_id,folio_number,transaction_type,quantity,gross_rate,gross_amount,brokerage,stt,net_rate,net_amount,entry_from_web,created_by,remarks):
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    import json
    from flask import Flask, request, jsonify
    pd.options.mode.chained_assignment = None    

    db=dbname
    user="shsa"
    server="13.127.124.84,6016"
    password="Easeprint#021"
    port = "80"
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
    except Exception as e:
        print(e)

    cur = conn.cursor()

    query = "exec Usp_T_Insert_in_Transaction_Register 0,%s,%s,'%s','%s','%s',%s,%s,'%s',%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,'%s',%s,'%s','%s','%s'"%(broker_id,demat_id,contract_number,reference_number,date_of_transaction,product_id,scrip_id,folio_number,gross_rate,transaction_type,quantity,gross_rate,gross_amount,brokerage,stt,net_rate,0,net_amount,date_of_transaction,gross_rate,'web',created_by,remarks)
    a = cur.execute(query)
    cur.commit()

    check_e = a.rowcount
    if check_e>=1:
        val = "Saved Successfully"
    else:
        val = "Data not saved"

    conn.close() 

    return val

def delete_temp_transaction_permanently_post_entry(dbname,user_name,computer_name):
    
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
    cur = conn.cursor()
    query = "delete from t_transaction_api_temp where [user] = '%s' and computer_name = '%s'"%(user_name,computer_name)

    a = cur.execute(query)
    cur.commit()

    conn.close() 
    return None

