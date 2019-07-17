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

# @app.route("/get_journal_data")
# def get_journal_data():
    
#     import pyodbc
#     import pandas as pd
#     import pandas.io.sql as psql
#     from flask import Flask, request, jsonify
      
#     headers = request.headers
#     auth = headers.get("X-Api-Key")
#     if auth == 'asoidewfoef':  
           
#        db="MJK2018_2019"
#        user="shsa"
#        server="13.127.124.84,6016"
#        password="Easeprint#021"
#        port = "80"
#        try:
#            #conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db +';UID=' + user + ';PWD=' + password)
#             conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
#        except Exception as e:
#            print(e)

#        query = "select * from t_journal"

#        abc = pd.read_sql(query, conn)    
#        json_final_data = abc.to_json(orient='records', date_format = 'iso')

#        conn.close()
   
#     else:
   
#        json_final_data = jsonify({"message": "ERROR: Unauthorized Access"}), 401

#     return json_final_data

@app.route('/get_all_ledger_heads', methods=['POST'])
def get_all_ledger_heads():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    data = []
    data = {'dbname':request.json['dbname']}

    db=data['dbname']
    user="shsa"
    server="13.127.124.84,6016"
    password="Easeprint#021"
    port = "80"
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
    except Exception as e:
        print(e)

    #query = "select * from m_main_ledger_head"
    query = "select 0 as MainLeadgerHeadId,'--Select One--' as MainLedgerHeads,0 as LedgerHeadId,0 as OrderNo,'false' as IsReserved union select * from m_main_ledger_head"

    abc = pd.read_sql(query, conn)    
    json_final_data = abc.to_json(orient='records', date_format = 'iso')

    conn.close() 

    return json_final_data

@app.route('/get_ledger_master', methods=['POST'])
def get_ledger_master():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
      
    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == 'asoidewfoef':  
       
       data = []
       data = {'dbname':request.json['dbname'],
               'ledgerhead':request.json['ledgerhead']}
         
       db=data['dbname']
       user="shsa"
       server="13.127.124.84,6016"
       password="Easeprint#021"
       port = "80"
       try:
           conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)

       query = "select 0 as tableid,'--Select One--' as ledgername," + str(data['ledgerhead']) + " as ledgerhead union select tableid,ledgername,ledgerhead from m_ledgermaster where ledgerhead = %s"%data['ledgerhead']

       abc = pd.read_sql(query, conn)    
       json_final_data = abc.to_json(orient='records', date_format = 'iso')

       conn.close()
   
    else:
   
       json_final_data = jsonify({"message": "ERROR: Unauthorized Access"}), 401

    return json_final_data

@app.route('/get_ledger', methods=['POST'])
def get_ledger():
    
       import pyodbc
       import pandas as pd
       import pandas.io.sql as psql
       from flask import Flask, request, jsonify

       headers = request.headers
       auth = headers.get("X-Api-Key")
       if auth == 'asoidewfoef':  

          data = []
          data = {'dbname':request.json['dbname'],
                  'ledgerhead':request.json['ledgerhead'],
                  'ledgeraccount':request.json['ledgeraccount'],
                  'from_date':request.json['from_date'],
                  'to_date':request.json['to_date']}

          db=data['dbname']
          user="shsa"
          server="13.127.124.84,6016"
          password="Easeprint#021"
          port = "80"
          try:
              conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
          except Exception as e:
              print(e)

          query = "exec USP_R_Ledger 1,%s,%s,%s,%s"%(data['ledgerhead'],data['ledgeraccount'],data['from_date'],data['to_date'])

          abc = pd.read_sql(query, conn)    
          json_final_data = abc.to_json(orient='records', date_format = 'iso')

          conn.close()

       else:

          json_final_data = jsonify({"message": "ERROR: Unauthorized Access"}), 401

       return json_final_data

@app.route('/get_trial_balance', methods=['POST'])
def get_trial_balance():
    
       import pyodbc
       import pandas as pd
       import pandas.io.sql as psql
       from flask import Flask, request, jsonify

       headers = request.headers
       auth = headers.get("X-Api-Key")
       if auth == 'asoidewfoef':  

          data = []
          data = {'dbname':request.json['dbname']}

          db=data['dbname']
          user="shsa"
          server="13.127.124.84,6016"
          password="Easeprint#021"
          port = "80"
          try:
              conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
          except Exception as e:
              print(e)

          query = "exec Usp_R_TrialBalaceNav 0"

          abc = pd.read_sql(query, conn)    
          json_final_data = abc.to_json(orient='records', date_format = 'iso')

          conn.close()

       else:

          json_final_data = jsonify({"message": "ERROR: Unauthorized Access"}), 401

       return json_final_data

@app.route('/get_holding_with_gain_loss', methods=['POST'])
def get_holding_with_gain_loss(): 
        import pyodbc
        import pandas as pd
        import pandas.io.sql as psql
        import json
        import datetime as dt
        from datetime import datetime
        from datetime import timedelta
        from bsedata.bse import BSE
        pd.options.mode.chained_assignment = None    

       headers = request.headers
       auth = headers.get("X-Api-Key")
       if auth == 'asoidewfoef':  
          data = []
          data = {'dbname':request.json['dbname']}      

           db=data['dbname']
           user="shsa"
           server="13.127.124.84,6016"
           password="Easeprint#021"
           port = "80"
           try:
               conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
           except Exception as e:
               print(e)

           cur = conn.cursor()

           query = "exec usp_r_holding 0"

           abc = pd.read_sql(query, conn)    

           cur.close()
           conn.close()

           def replace_last(source_string, replace_what, replace_with):
               head, _sep, tail = source_string.rpartition(replace_what)
               return head + replace_with + tail

           def get_mutual_fund_nav(mf_code_string):
               import requests
               import os
               import json

               url = "https://mutual-fund-info.p.rapidapi.com/ri/v1/investment/scheme/latestNav"
               param = {
                   "X-RapidAPI-Host": "mutual-fund-info.p.rapidapi.com",
                   "X-RapidAPI-Key": "826618dea1msh8638b9c6973968ep101467jsnf068791f2bf0",
                   "Content-Type": "application/json"
               }

               r = requests.post(url,data=(mf_code_string), headers=param)

               abc =r.content

               return abc

           def calculate_current_value(row):

               if row['nav']==0:
                   val = row['closing_shares']*row['cost_price']
               else:
                   val = row['closing_shares']*row['nav']        
               return val

           def calculate_cost(row):

               if row['nav']==0:
                   val =0
               else:
                   val = row['total_amount']/row['closing_shares']        
               return val

           def calculate_gain_loss(row):

               val = (row['current_value']-row['total_amount'])
               return val

           def calculate_absolute_gain(row):

               if row['total_amount']==0:
                   val = 0
               else:
                   val = (row['gain/loss']/row['total_amount'])*100
               return val

           def get_nse_price(scrip_name):

               from nsetools import Nse
               nse = Nse()
               import json
               from pandas.io.json import json_normalize
               a = nse.is_valid_code(scrip_name)
               if a == False:
                   return None
               else:
                   q = nse.get_quote(scrip_name)
                   f = json.dumps(q)
                   json_f = json.loads(f)
                   filesdata = json_normalize(json_f)
                   return filesdata    


           data = abc

           #### mutual fund scheme calculation

           mutual_fund_data = data.loc[data['product_name']=='Mutual Fund']    
           scehme_codes = mutual_fund_data['scrip_code']
           scheme_code_stirng = '{'+ '"' + 'schemeCodes' + '"' + ":["
           for i in scehme_codes:
               scheme_code_stirng = scheme_code_stirng + '"' + str(i) + '"' + ","
           mf_code_string = replace_last(scheme_code_stirng, ',', ']}')  
           a = json.loads(get_mutual_fund_nav(mf_code_string))
           ab = a['data']
           mf_nav_from_site = pd.DataFrame.from_dict(ab)
           mf_nav_from_site["schemeCode"] = pd.to_numeric(mf_nav_from_site["schemeCode"])
           mutual_fund_data["scrip_code"] = pd.to_numeric(mutual_fund_data["scrip_code"])
           mutual_fund_data["total_amount"] = pd.to_numeric(mutual_fund_data["total_amount"])
           mutual_fund_data["closing_shares"] = pd.to_numeric(mutual_fund_data["closing_shares"])
           mf_final_data = pd.merge(mutual_fund_data,mf_nav_from_site,left_on='scrip_code',right_on='schemeCode',how='left')
           mf_final_data = mf_final_data[['product_name','scrip_code','Particulars','closing_shares','total_amount','nav','date']]
           mf_final_data['cost_price'] = mf_final_data.apply(calculate_cost, axis =1) 
           mf_final_data['current_value'] = mf_final_data.apply(calculate_current_value, axis =1) 
           mf_final_data['gain/loss'] = mf_final_data.apply(calculate_gain_loss, axis =1) 
           mf_final_data['absolute_gain(%)'] = mf_final_data.apply(calculate_absolute_gain, axis =1) 
           mf_final_data = mf_final_data.round({'total_amount' : 2,'current_value' : 2,'gain/loss' : 2,'absolute_gain(%)':2})

           #### equity scheme calculation

           date = (datetime.now() + timedelta(minutes=330)).strftime('%d-%b-%Y')
           overall_data = pd.DataFrame()    
           equity_data = data.loc[data['product_name']=='Equity'] 
           for x in equity_data.itertuples():
               price = get_nse_price(str(x[4]))
               overall_data = overall_data.append(price) 
           site_data = overall_data[['symbol','lastPrice']]
           site_data["lastPrice"] = pd.to_numeric(site_data["lastPrice"])
           site_data.symbol = site_data.symbol.apply(str)
           equity_data.scrip_code = equity_data.scrip_code.apply(str)
           equity_data["closing_shares"] = pd.to_numeric(equity_data["closing_shares"])
           equity_data["total_amount"] = pd.to_numeric(equity_data["total_amount"])
           final_data = pd.merge(equity_data,site_data,left_on='scrip_code',right_on='symbol',how='left')
           final_data['lastPrice'] = final_data['lastPrice'].fillna(0)
           final_data['date'] = date
           equity_final_data = final_data[['product_name','scrip_code','Particulars','closing_shares','total_amount','lastPrice','date']]
           equity_final_data = equity_final_data.rename(columns={'lastPrice': 'nav'})
           equity_final_data['cost_price'] = equity_final_data.apply(calculate_cost, axis =1) 
           equity_final_data['current_value'] = equity_final_data.apply(calculate_current_value, axis =1) 
           equity_final_data['gain/loss'] = equity_final_data.apply(calculate_gain_loss, axis =1) 
           equity_final_data['absolute_gain(%)'] = equity_final_data.apply(calculate_absolute_gain, axis =1) 
           equity_final_data = equity_final_data.round({'total_amount' : 2,'current_value' : 2,'gain/loss' : 2,'absolute_gain(%)':2})    


           bond_data = data.loc[data['product_name']=='Bonds'] 
           bond_scehme_codes = bond_data['scrip_code']
           abc = bond_scehme_codes.to_list()
           b = BSE()
           dfObj1 = pd.DataFrame()
           codelist = abc
           for code in codelist:
               quote = b.getQuote(code)
               dfObj = pd.DataFrame(quote)
               dfObj1 = dfObj1.append(dfObj)

           dfObj1 = dfObj1.drop_duplicates(subset='scripCode', keep='first', inplace=False)
           bond_prices_from_site = dfObj1[['scripCode','currentValue','updatedOn']]        

           bond_prices_from_site["scripCode"] = pd.to_numeric(bond_prices_from_site["scripCode"])
           bond_prices_from_site["currentValue"] = pd.to_numeric(bond_prices_from_site["currentValue"])
           bond_data["scrip_code"] = pd.to_numeric(bond_data["scrip_code"])
           bond_data["total_amount"] = pd.to_numeric(bond_data["total_amount"])
           bond_data["closing_shares"] = pd.to_numeric(bond_data["closing_shares"])
           bond_final_data = pd.merge(bond_data,bond_prices_from_site,left_on='scrip_code',right_on='scripCode',how='left')
           bond_final_data = bond_final_data[['product_name','scrip_code','Particulars','closing_shares','total_amount','currentValue','updatedOn']]
           bond_final_data['currentValue'] = bond_final_data['currentValue'].fillna(0)
           bond_final_data = bond_final_data.rename(columns={'currentValue': 'nav'})
           bond_final_data = bond_final_data.rename(columns={'updatedOn': 'date'})
           bond_final_data['cost_price'] = bond_final_data.apply(calculate_cost, axis =1) 
           bond_final_data['current_value'] = bond_final_data.apply(calculate_current_value, axis =1) 
           bond_final_data['gain/loss'] = bond_final_data.apply(calculate_gain_loss, axis =1) 
           bond_final_data['absolute_gain(%)'] = bond_final_data.apply(calculate_absolute_gain, axis =1) 
           bond_final_data = bond_final_data.round({'total_amount' : 2,'current_value' : 2,'gain/loss' : 2,'absolute_gain(%)':2})    

           frames = [equity_final_data, mf_final_data, bond_final_data]
           combined_holding_data = pd.concat(frames)

           combined_holding_data = combined_holding_data.rename(columns={'closing_shares': 'quantity'}) 
           combined_holding_data = combined_holding_data.rename(columns={'total_amount': 'total_cost'}) 
           combined_holding_data = combined_holding_data.rename(columns={'nav': 'current_price'})
           combined_holding_data = combined_holding_data.rename(columns={'date': 'valuation_date'})                  
           combined_holding_data = combined_holding_data[['product_name','scrip_code','Particulars','quantity','cost_price','total_cost','current_price','valuation_date','current_value','gain/loss','absolute_gain(%)']]
           combined_holding_data = combined_holding_data.round({'quantity' : 4,'cost_price' : 2,'gain/loss' : 2,'absolute_gain(%)':2,'total_cost':2,'current_price':2,'current_value':2})    

           json_final_data = combined_holding_data.to_json(orient='records', date_format = 'iso')

           return json_final_data   
      else:
           
           json_final_data = jsonify({"message": "ERROR: Unauthorized Access"}), 401
           return json_final_data   
