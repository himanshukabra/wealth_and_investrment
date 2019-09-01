def get_auto_serial_number(dbname):
    
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

    query = "Select MAX(isnull(AutoSerialNumber,0))+1 as srno from T_Journal"
    
    abc = pd.read_sql(query, conn)  
    
    conn.close() 
    
    return abc
    
def insert_data_in_temp_journal(dbname,date_of_transaction,account_head,account_ledger,DRCR,voucher_number,standard_description,amount,computer_name,created_by):
    
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
    query = "insert into T_Temp_Journal([Date], AccountHead, AccountLedger, DRCR, VoucherNumber, StandardDescription1, Amount,ComputerName,CreatedBy,CreatedDate) values (convert(date, '%s',103),%s,%s,'%s','%s','%s',%s,'%s','%s',convert(date,GetDate(),103))"%(date_of_transaction,account_head,account_ledger,DRCR,voucher_number,standard_description,amount,computer_name,created_by)

    a = cur.execute(query)
    cur.commit()

    check_e = a.rowcount
    if check_e>=1:
       val = "Saved Successfully"
    else:
       val = "Data not saved"

    conn.close() 
    return val
