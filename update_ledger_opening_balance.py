def get_ledger_opening(dbname,ledger_id):

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

    query = "Select ISNULL(OpCR,0) as CumCR,ISNULL(OpDR,0) as CumDB  From M_LedgerMaster Where TableId=%s"%(ledger_id)

    abc = pd.read_sql(query, conn)

    conn.close()  

    json_final_data = abc.to_json(orient='records', date_format = 'iso')
    
    return json_final_data

def update_new_opening_balance_for_ledger(dbname,amount_paid,bank_ledger,paid_on,table_id):
    
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
