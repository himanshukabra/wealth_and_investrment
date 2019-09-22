def get_bank_book_data(dbname,from_date,to_date):
    
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

    query = "exec Usp_T_BankBook 0,3,'','%s','%s'"(from_date,to_date)    
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
    server="13.127.124.84,6016"
    password="Easeprint#021"
    port = "80"
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
    except Exception as e:
        print(e)

    cur = conn.cursor()

    query = "exec Usp_T_BankBook %s,2,'','','','','','',%s"(table_id,auto_serial_number)    
    abc = pd.read_sql(query, conn)
    a = cur.execute(query)
    cur.commit()

    check_e = a.rowcount
    if check_e>=1:
        val = "Saved Successfully"
    else:
        val = "Data not saved"

    conn.close() 
    return jsonify({"message": val}), 200     
    
def delete_cash_book_entry(dbname,table_id,auto_serial_number):
    
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

    query = "exec Usp_T_CashBook %s,2,'','','','','','','',%s"(table_id,auto_serial_number)    
    a = cur.execute(query)
    cur.commit()

    check_e = a.rowcount
    if check_e>=1:
        val = "Saved Successfully"
    else:
        val = "Data not saved"

    conn.close() 
    return jsonify({"message": val}), 200   
