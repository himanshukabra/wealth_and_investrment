def get_auto_debit_transaction_data(dbname,from_date,to_date):

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

    query = "select t1.tableid,t1.product_id,t2.products_offered as product_name,t1.scrip_id,t.scripname,[date],t1.amount_registered,t1.investment_ledger,t1.bank_ledger_registered,lm.ledgername as bank_registered,t1.folio_number from t_auto_debit_transactions t1 left join (select Table_Id,ScripName,Product_Id from M_Equity union select id as tableid,bond_name as scripname,3 as Product_Id from M_Bonds union select Table_Id,Scheme_Name as scripname,Product_Id from M_Mutual_Funds union select 0 as TableId, '--Select One--',1 as product_id ) t on t.Table_Id = t1.scrip_id and t.product_id = t1.product_id left join m_product t2 on t1.product_id = t2.id left join m_ledgermaster lm on t1.bank_ledger_registered = lm.tableid where registration_status = 'active' and date >= convert(date,'%s',103) and date<=convert(date,'%s',103) and isnull(entry_processed_status,'No')='No'"%(from_date,to_date)

    abc = pd.read_sql(query, conn)

    conn.close()  

    json_final_data = abc.to_json(orient='records', date_format = 'iso')
    
    return json_final_data

def update_auto_debit_transaction(dbname,amount_paid,bank_ledger,paid_on,table_id):
    
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
    query = "update t_auto_debit_transactions set actual_amount_paid = %s, bank_ledger_paid = %s,paid_on_date = convert(date,'%s',103),entry_processed_status = 'yes', entry_processed_date = convert(date, getdate(),103) where tableid=%s"%(amount_paid,bank_ledger,paid_on,table_id)

    a = cur.execute(query)
    cur.commit()

    check_e = a.rowcount
    if check_e>=1:
       val = "Saved Successfully"
    else:
       val = "Data not saved"

    conn.close() 
    return val

def get_bank_names(dbname):

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

    query = "select ledgername,tableid from m_ledgermaster where ledgerhead = 17 union select '--Select One--' as ledgername, 0 as tableid"

    abc = pd.read_sql(query, conn)

    conn.close()  

    json_final_data = abc.to_json(orient='records', date_format = 'iso')
    
    return json_final_data
