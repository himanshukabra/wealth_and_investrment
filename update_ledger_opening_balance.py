def get_ledger_opening(dbname,ledger_id):

    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    db=dbname
    user="shsa"
    server="103.212.121.67"
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

def update_new_opening_balance_for_ledger(dbname,account_ledger,debit_amount,credit_amount):
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    db=dbname
    user="shsa"
    server="103.212.121.67"
    password="Easeprint#021"
    port = "80"
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)

    except Exception as e:
        print(e)
    cur = conn.cursor()
    query = "exec Usp_LedgerOpeningBalance 1,0,'',0,%s,%s,0,%s,0,'',0,''"%(account_ledger,debit_amount,credit_amount)

    a = cur.execute(query)
    cur.commit()

    check_e = a.rowcount
    if check_e>=1:
       val = "Saved Successfully"
    else:
       val = "Data not saved"

    conn.close() 
    return val
