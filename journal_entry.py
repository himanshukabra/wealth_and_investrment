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

def delete_temp_journal_entry(dbname,tableid):
    
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
    query = "delete from T_Temp_Journal where tableid = %s"%(tableid)

    a = cur.execute(query)
    cur.commit()

    check_e = a.rowcount
    if check_e>=1:
       val = "Deleted Successfully"
    else:
       val = "Data not Deleted"

    conn.close() 
    return jsonify({"response": val}), 200

def get_temp_journal_transaction(dbname,user_name,computer_name):
    
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

    query = "select tableid,date,AccountHead,AccountLedger,DRCR,VoucherNumber,StandardDescription1,Amount from t_temp_journal where ComputerName = '%s' and CreatedBy = '%s'"%(computer_name,user_name)

    abc = pd.read_sql(query, conn)

    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    conn.close() 

    return json_final_data

def insert_journal(dbname,voucher_number,auto_serial_number,user_name,computer_name):
    
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

    query = "insert into T_Journal (date,AccountHead,accountledger,drcr,autoserailnumber,voucher_number,standarddescription1, standarddescription2,amount,computername,createdby,createddate) values ('%s',%s,%s,'%s','%s','%s','%s','%s',%s,'%s','%s','%s','%s')"%(i[2],i[3],i[4],i[5],auto_serial_number,voucher_number,i[7],i[7],i[8],computer_name,created_by,convert(date,getdate(),103))

    a = cur.execute(query)
    cur.commit()

    check_e = a.rowcount
    if check_e>=1:
       val = "Saved Successfully"
    else:
       val = "Data not Saved"

    conn.close() 
    return jsonify({"response": val}), 200

