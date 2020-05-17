def get_bank_book_data(dbname,from_date,to_date):
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    import json
    from flask import Flask, request, jsonify
    pd.options.mode.chained_assignment = None    

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

    query = "exec Usp_T_BankBook 0,3,'',%s,%s"%(from_date,to_date)    
    abc = pd.read_sql(query, conn)
    conn.close() 
    
    json_final_data = abc.to_json(orient='records', date_format = 'iso')
    
    return json_final_data
    
def delete_bank_book_entry(dbname,table_id,auto_serial_number):
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    import json
    from flask import Flask, request, jsonify
    pd.options.mode.chained_assignment = None    

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

    query = "exec Usp_T_BankBook %s,2,'','','','','','',%s"%(table_id,auto_serial_number)    
    a = cur.execute(query)
    cur.commit()

    check_e = a.rowcount
    if check_e>=1:
        val = "Deleted Successfully"
    else:
        val = "Data not Deleted"

    conn.close() 
    return val      
    
def delete_cash_book_entry(dbname,table_id,auto_serial_number):
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    import json
    from flask import Flask, request, jsonify
    pd.options.mode.chained_assignment = None    

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

    query = "exec Usp_T_CashBook %s,2,'','','','','','','',%s"%(table_id,auto_serial_number)    
    a = cur.execute(query)
    cur.commit()

    check_e = a.rowcount
    if check_e>=1:
        val = "Deleted Successfully"
    else:
        val = "Data not deleted"

    conn.close() 
    return val
    #return jsonify({"message": val}), 200   

def get_bank_book_data_for_auto_serial_number(dbname,auto_serial_number):
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    import json
    from flask import Flask, request, jsonify
    pd.options.mode.chained_assignment = None    

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

    query = "select t1.tableid,t1.autoserialnumber,t1.date,t1.accounthead,t1.accountledger,t2.ledgername as account_ledger_name,t3.mainledgerheads as account_head_name,t1.drcr,t1.vouchernumber,t1.standarddescription1,t1.amount,t1.ledgerhead1,t1.ledger1,t4.ledgername as bank_ledger_name,t5.mainledgerheads as bank_head_name from t_bankbook t1 left join m_ledgermaster t2 on t1.accountledger = t2.tableid left join M_Main_Ledger_Head t3 on t1.accounthead = t3.mainleadgerheadid left join m_ledgermaster t4 on t1.ledger1 = t4.tableid left join M_Main_Ledger_Head t5 on t1.ledgerhead1 = t5.mainleadgerheadid where autoserialnumber = %s"%(auto_serial_number)    
    abc = pd.read_sql(query, conn)
    conn.close() 
    
    json_final_data = abc.to_json(orient='records', date_format = 'iso')
    
    return json_final_data 
