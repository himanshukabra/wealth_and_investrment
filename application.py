from flask import Flask
app = Flask(__name__)
@app.route("/home")
def home():
   return "Hello Himanshu"

@app.route("/check")
def check():
    '''
    The aim of this function is to check if the API is up
    '''
    return 'http 200 - I did it'

@app.route("/get_journal_data")
def get_journal_data():
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    db="MJK2018_2019"
    user="shsa"
    server="13.127.124.84,6016"
    password="Easeprint#021"
    port = "80"
    try:
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db +';UID=' + user + ';PWD=' + password)
    except Exception as e:
        print(e)

    cur = conn.cursor()
    #query = "select m_product.products_offered as product_name,income.scrip_id,case when income.product_id = 1 then m_equity.scripname when income.product_id = 2 then m_bonds.Bond_Name end as particulars,income.transaction_date,income.amount from t_income_data income left join m_product on income.product_id = m_product.id left join m_equity on income.product_id = m_product.id and income.scrip_id = m_equity.table_id left join m_bonds on income.product_id = m_product.id and income.scrip_id = m_bonds.id "    
    query = "select * from t_journal"
    
    abc = pd.read_sql(query, conn)    
    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    cur.close()
    conn.close()
    
    return json_final_data

