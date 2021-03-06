from transaction_entry import get_product
from transaction_entry import get_scheme
from transaction_entry import get_demat
from transaction_entry import get_broker
from transaction_entry import get_scrip_opening
from transaction_entry import get_folios
from transaction_entry import insert_temp_transaction_register
from transaction_entry import get_temp_transaction
from transaction_entry import delete_temp_transaction
from transaction_entry import get_total_for_temp_transaction
from transaction_entry import get_product_ledger_list
from transaction_entry import get_broker_id_ledger
from transaction_entry import insert_data_in_at_from_transaction_entry
from transaction_entry import insert_final_data_in_transaction_register
from transaction_entry import get_temp_data_from_transaction_register
from transaction_entry import delete_temp_transaction_permanently_post_entry
from auto_transaction_entry import get_auto_debit_transaction_data
from auto_transaction_entry import update_auto_debit_transaction
from auto_transaction_entry import get_bank_names
from update_ledger_opening_balance import get_ledger_opening
from update_ledger_opening_balance import update_new_opening_balance_for_ledger
from journal_entry import get_auto_serial_number
from journal_entry import insert_data_in_temp_journal
from journal_entry import delete_temp_journal_entry
from journal_entry import get_temp_journal_transaction
from journal_entry import insert_journal
from journal_entry import get_temp_journal_transaction_as_dataframe
from journal_entry import delete_temp_transaction_permanently_post_entry_for_journal
from journal_entry import insert_data_in_account_transaction
from cash_book import get_bank_book_data
from cash_book import delete_cash_book_entry
from cash_book import delete_bank_book_entry
from cash_book import get_bank_book_data_for_auto_serial_number
from journal_entry import delete_journal_book_entry
from create_graphs import get_investment_graph

from flask import Flask
app = Flask(__name__)

user="shsa"
server="103.212.121.67"
password="Easeprint#021"

@app.route("/home")
def home():
   return "Hello Himanshu"

@app.route("/check")
def check():
    '''
    The aim of this function is to check if the API is up
    '''
    return 'http 200 - I did it'

@app.route('/get_all_ledger_heads', methods=['POST'])
def get_all_ledger_heads():
    
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

    else:
   
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401

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
       try:
           conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)

       query = "select 0 as tableid,'--Select One--' as ledgername," + str(data['ledgerhead']) + " as ledgerhead union select tableid,ledgername,ledgerhead from m_ledgermaster where ledgerhead = %s"%data['ledgerhead']

       abc = pd.read_sql(query, conn)    
       json_final_data = abc.to_json(orient='records', date_format = 'iso')

       conn.close()
   
    else:
   
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401

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
          try:
              conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
          except Exception as e:
              print(e)

          query = "exec USP_R_Ledger 1,%s,%s,'%s','%s'"%(data['ledgerhead'],data['ledgeraccount'],data['from_date'],data['to_date'])
          abc = pd.read_sql(query, conn)    
          json_final_data = abc.to_json(orient='records', date_format = 'iso')

          conn.close()

       else:

          json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401

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
          try:
              conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
          except Exception as e:
              print(e)

          #query = "exec Usp_R_TrialBalaceNav 0"
          query = "select convert(varchar,lm.LedgerHead) as LedgerHead,convert(varchar,lm.TableId) as TableId,(select mlh.MainLedgerHeads from M_Main_Ledger_Head MLH where lm.LedgerHead=MLH.MainLeadgerHeadId) as MainLedgerHeads ,lm.LedgerName as ledgername,(select mlh.OrderNo from M_Main_Ledger_Head MLH where lm.LedgerHead=MLH.MainLeadgerHeadId) as sequence ,(select case when isnull((isnull(lm.OpDR,0)-ISNULL(lm.OpCR,0)+ABS(isnull(lm.CumDB,0))-isnull(lm.CumCR,0)),0)>0 then (isnull(lm.OpDR,0)-isnull(lm.OpCr,0)+ABS(isnull(lm.CumDb,0))-isnull(lm.CumCr,0)) else 0 end) as DR,(select case when isnull((isnull(lm.OpDR,0)-ISNULL(lm.OpCR,0)+ABS(isnull(lm.CumDB,0))-isnull(lm.CumCR,0)),0)<0 then (isnull(lm.OpCR,0)-isnull(lm.OpDr,0)-ABS(isnull(lm.CumDb,0))+isnull(lm.CumCr,0)) else 0 end) as CR from M_LedgerMaster lm where Isnull((isnull(lm.OpDR,0)-ISNULL(lm.OpCR,0)+ABS(isnull(lm.CumDB,0))-isnull(lm.CumCR,0)),0)>0 or Isnull((isnull(lm.OpCR,0)-ISNULL(lm.OpDR,0)-ABS(isnull(lm.CumDB,0))+isnull(lm.CumCR,0)),0)>0"
          abc = pd.read_sql(query, conn)    
          abc = abc.sort_values(by=['sequence','ledgername'], ascending=True)
         
          json_final_data = abc.to_json(orient='records', date_format = 'iso')

          conn.close()

       else:

          json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401

       return json_final_data

@app.route('/get_holding_with_gain_loss', methods=['POST'])
def get_holding_with_gain_loss(): 
        import pyodbc
        import pandas as pd
        import pandas.io.sql as psql
        import json
        import datetime as dt
        from pandas.io.json import json_normalize
        from datetime import datetime
        from datetime import timedelta
        from bsedata.bse import BSE
        from flask import Flask, request, jsonify
        pd.options.mode.chained_assignment = None    

        headers = request.headers
        auth = headers.get("X-Api-Key")
        if auth == 'asoidewfoef':  
           data = []
           data = {'dbname':request.json['dbname']}      

           db=data['dbname']
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
               mf_nav_data =r.content
               return mf_nav_data

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
               from pandas.io.json import json_normalize
               from nsetools import Nse
               import json
               nse = Nse()
               import json
               from pandas.io.json import json_normalize
               a = nse.is_valid_code(scrip_name)
               b = str(a)
               if b == 'False':
                   filesd = []
                   filesd=  [[scrip_name,0]]
                   filesdata = pd.DataFrame(filesd,columns = ['symbol','lastPrice'])
               if b == 'True':
                   q = nse.get_quote(str(scrip_name))
                   f = json.dumps(q)
                   json_f = json.loads(f)
                   filesdata = json_normalize(json_f)
                  
               return filesdata

           data1 = abc

           #### mutual fund scheme calculation
           mutual_fund_data = data1.loc[data1['product_name']=='Mutual Fund']    
           scehme_codes = mutual_fund_data['scrip_code']
           scheme_code_stirng = '{'+ '"' + 'schemeCodes' + '"' + ":["
           for i in scehme_codes:
               scheme_code_stirng = scheme_code_stirng + '"' + str(i) + '"' + ","
           mf_code_string = replace_last(scheme_code_stirng, ',', ']}')  
           a = json.loads(get_mutual_fund_nav(mf_code_string))
           del a['meta']
           my_json_string = json.dumps(a)
           df = pd.read_json(my_json_string)
           mf_data = df.data.apply(pd.Series)         
           mf_data['schemeCode'] = pd.to_numeric(mf_data['schemeCode'])
           mutual_fund_data['scrip_code'] = pd.to_numeric(mutual_fund_data['scrip_code'])
           mutual_fund_data['total_amount'] = pd.to_numeric(mutual_fund_data['total_amount'])
           mutual_fund_data['closing_shares'] = pd.to_numeric(mutual_fund_data['closing_shares'])
           mf_final_data = pd.merge(mutual_fund_data,mf_data,left_on='scrip_code',right_on='schemeCode',how='left')
           mf_final_data = mf_final_data[['product_name','scrip_code','Particulars','closing_shares','total_amount','nav','date']]
           mf_final_data['cost_price'] = mf_final_data.apply(calculate_cost, axis =1) 
           mf_final_data['current_value'] = mf_final_data.apply(calculate_current_value, axis =1) 
           mf_final_data['gain/loss'] = mf_final_data.apply(calculate_gain_loss, axis =1) 
           mf_final_data['absolute_gain(%)'] = mf_final_data.apply(calculate_absolute_gain, axis =1) 
           mf_final_data = mf_final_data.round({'total_amount' : 2,'current_value' : 2,'gain/loss' : 2,'absolute_gain(%)':2})

           #### equity scheme calculation

           date = (datetime.now() + timedelta(minutes=330)).strftime('%d-%b-%Y')
           overall_data = pd.DataFrame()    
           equity_data = data1.loc[data1['product_name']=='Equity'] 
           if not equity_data.empty:
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

#            bond_data = data1.loc[data1['product_name']=='Bonds'] 
#            bond_scehme_codes = bond_data['scrip_code']
#            abc = bond_scehme_codes.to_list()
#            b = BSE()
#            dfObj1 = pd.DataFrame()
#            codelist = abc
#            for code in codelist:
#                quote = b.getQuote(code)
#                dfObj = pd.DataFrame(quote)
#                dfObj1 = dfObj1.append(dfObj)

#            dfObj1 = dfObj1.drop_duplicates(subset='scripCode', keep='first', inplace=False)
#            bond_prices_from_site = dfObj1[['scripCode','currentValue','updatedOn']]        

#            bond_prices_from_site["scripCode"] = pd.to_numeric(bond_prices_from_site["scripCode"])
#            bond_prices_from_site["currentValue"] = pd.to_numeric(bond_prices_from_site["currentValue"])
#            bond_data["scrip_code"] = pd.to_numeric(bond_data["scrip_code"])
#            bond_data["total_amount"] = pd.to_numeric(bond_data["total_amount"])
#            bond_data["closing_shares"] = pd.to_numeric(bond_data["closing_shares"])
#            bond_final_data = pd.merge(bond_data,bond_prices_from_site,left_on='scrip_code',right_on='scripCode',how='left')
#            bond_final_data = bond_final_data[['product_name','scrip_code','Particulars','closing_shares','total_amount','currentValue','updatedOn']]
#            bond_final_data['currentValue'] = bond_final_data['currentValue'].fillna(0)
#            bond_final_data = bond_final_data.rename(columns={'currentValue': 'nav'})
#            bond_final_data = bond_final_data.rename(columns={'updatedOn': 'date'})
#            bond_final_data['cost_price'] = bond_final_data.apply(calculate_cost, axis =1) 
#            bond_final_data['current_value'] = bond_final_data.apply(calculate_current_value, axis =1) 
#            bond_final_data['gain/loss'] = bond_final_data.apply(calculate_gain_loss, axis =1) 
#            bond_final_data['absolute_gain(%)'] = bond_final_data.apply(calculate_absolute_gain, axis =1) 
#            bond_final_data = bond_final_data.round({'total_amount' : 2,'current_value' : 2,'gain/loss' : 2,'absolute_gain(%)':2})    

           if equity_data.empty and mutual_fund_data.empty:
               exit()
           elif equity_data.empty:
               frames = [mf_final_data]
           elif mutual_fund_data.empty:    
               frames = [equity_final_data]
           else:
               frames = [equity_final_data,mf_final_data]            
           
           combined_holding_data = pd.concat(frames)

           combined_holding_data = combined_holding_data.rename(columns={'closing_shares': 'quantity'}) 
           combined_holding_data = combined_holding_data.rename(columns={'total_amount': 'total_cost'}) 
           combined_holding_data = combined_holding_data.rename(columns={'nav': 'current_price'})
           combined_holding_data = combined_holding_data.rename(columns={'date': 'valuation_date'}) 
           combined_holding_data = combined_holding_data.rename(columns={'gain/loss': 'gainloss'}) 
           combined_holding_data = combined_holding_data.rename(columns={'absolute_gain(%)': 'absolute_gain'})  
           combined_holding_data = combined_holding_data[['product_name','scrip_code','Particulars','quantity','cost_price','total_cost','current_price','valuation_date','current_value','gainloss','absolute_gain']]
           combined_holding_data = combined_holding_data.round({'quantity' : 4,'cost_price' : 2,'gainloss' : 2,'absolute_gain':2,'total_cost':2,'current_price':2,'current_value':2})    

           json_final_data = combined_holding_data.to_json(orient='records', date_format = 'iso')

           return json_final_data   
        else:           
           json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401
           return json_final_data   

@app.route('/get_gp_data_cash_book', methods=['POST'])
def get_gp_data_cash_book():
    
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
       try:
           conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)

       #query = "select * from m_main_ledger_head"
       query = "select LedgerHeadId,LedgerId from GlobalPreffrences where UsedHead='CashBook'"

       abc = pd.read_sql(query, conn)    
       json_final_data = abc.to_json(orient='records', date_format = 'iso')

       conn.close() 
       return json_final_data  
    else:           
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401
       return json_final_data 
  
@app.route('/get_cash_opening_balance', methods=['POST'])
def get_cash_opening_balance():
    
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
       try:
           conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)

       query = "select case when isnull((isnull(lm.OpDR,0)-ISNULL(lm.OpCR,0)+isnull(t.CumDB,0)-isnull(t.CumCR,0)),0)>0 then Convert(varchar, abs((isnull(lm.OpDR,0)-isnull(lm.OpCr,0)+isnull(t.CumDb,0)-isnull(t.CumCr,0))))+' DR' else Convert(varchar, abs((isnull(lm.OpDR,0)-isnull(lm.OpCr,0)+isnull(t.CumDb,0)-isnull(t.CumCr,0))))+' CR' end as CashBalanceOpening from (select AccountLedger,sum(case when DRCR = 'D' then Amount end) as CumDb,sum(case when DRCR = 'C' then Amount end) as CumCr from T_Account_Transaction where AccountLedger= (select LedgerId from GlobalPreffrences where UsedFor='CB') group by AccountLedger) t inner join m_ledgermaster lm on t.AccountLedger = lm.tableid"

       abc = pd.read_sql(query, conn)    
       json_final_data = abc.to_json(orient='records', date_format = 'iso')

       conn.close() 
       return json_final_data  
    else:           
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401
       return json_final_data

@app.route('/get_ledger_opening_balance', methods=['POST'])
def get_ledger_opening_balance():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
     
    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == 'asoidewfoef':  
  
       data = []
       data = {'dbname':request.json['dbname'],
               'account_ledger':request.json['account_ledger']}

       db=data['dbname']
       try:
           conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)
    
       query = "select case when isnull((isnull(lm.OpDR,0)-ISNULL(lm.OpCR,0)+isnull(t.CumDB,0)-isnull(t.CumCR,0)),0)>0 then Convert(varchar, abs((isnull(lm.OpDR,0)-isnull(lm.OpCr,0)+isnull(t.CumDb,0)-isnull(t.CumCr,0))))+' DR' else Convert(varchar, abs((isnull(lm.OpDR,0)-isnull(lm.OpCr,0)+isnull(t.CumDb,0)-isnull(t.CumCr,0))))+' CR' end as CashBalanceOpening from (Select AccountLedger,sum(CumDb) as CumDb,sum(CumCr) as CumCr from (select AccountLedger,sum(case when DRCR = 'D' then Amount else 0 end) as CumDb,sum(case when DRCR = 'C' then Amount else 0 end) as CumCr from T_Account_Transaction where AccountLedger= %s  group by AccountLedger union all Select %s as AccountLedger,0 as CumDb, 0 as CumCr) f group by AccountLedger ) t inner join m_ledgermaster lm on t.AccountLedger = lm.tableid"%(data['account_ledger'],data['account_ledger'])

       abc = pd.read_sql(query, conn)    
       json_final_data = abc.to_json(orient='records', date_format = 'iso')

       conn.close() 
       return json_final_data  
    else:           
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401
       return json_final_data   

@app.route('/insert_data_cash_book', methods=['POST'])
def insert_data_cash_book():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    from flask import Flask, request, jsonify
     
    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == 'asoidewfoef':  
  
       data = []
       data = {'dbname':request.json['dbname'],
               'date':request.json['date'],
               'account_ledger':request.json['account_ledger'],
               'account_head':request.json['account_head'],
               'drcr':request.json['drcr'],
               'vouchernumber':request.json['vouchernumber'],
               'standard_description':request.json['standard_description'],
               'amount':request.json['amount'],
               'ledger1':request.json['ledger1'],
               'ledgerhead':request.json['ledgerhead'],
               'computername':request.json['computername'],
               'createdby':request.json['createdby'],
               'entry_type':request.json['entry_type']}

       db=data['dbname']
       try:
           conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)
       cur = conn.cursor()
      
       if data['entry_type']=='CashBook':                                 
           query = "exec Usp_T_CashBook 1,0,'%s','','',%s,%s,'','%s',4,'%s','%s','%s','%s','%s',%s,%s,%s,'%s','%s','','','',''"%(data['date'],data['account_head'],data['account_ledger'],data['drcr'],data['vouchernumber'],data['standard_description'],data['standard_description'],data['standard_description'],data['standard_description'],data['amount'],data['ledgerhead'],data['ledger1'],data['computername'],data['createdby'])
       elif data['entry_type']=='BankBook':
           if data['drcr']=="P":
               data['drcr']=="I"
           elif data['drcr']=="R":
               data['drcr']=="R"
           query = "exec Usp_T_BankBook 1,0,'%s','','',%s,%s,'%s',4,'%s','%s','%s','%s','%s',%s,%s,%s,'','%s','%s','','','',''"%(data['date'],data['account_head'],data['account_ledger'],data['drcr'],data['vouchernumber'],data['standard_description'],data['standard_description'],data['standard_description'],data['standard_description'],data['amount'],data['ledgerhead'],data['ledger1'],data['computername'],data['createdby'])      
       
       #query = "exec Usp_T_CashBook 1,0,'2019-03-31','','',1,7125,'','D',4,'','test now','test now','test now','test now',1.00,21,7119,'HIMANSHU','HIMANSHU','2019-03-31','','',''"
       print(query)
       a = cur.execute(query)
       cur.commit()
       
       check_e = a.rowcount
       if check_e>=1:
           val = "Saved Successfully"
       else:
           val = "Data not saved"
       
       conn.close() 
       return jsonify({"response": val}), 200
  
    else:           
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401
       return json_final_data       

@app.route("/get_product_list", methods=['POST'])
def get_product_list():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname']}   
       json_final_data = get_product(data['dbname'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route("/get_scheme_list", methods=['POST'])
def get_scheme_list():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'product_id':request.json['product_id']}   
       json_final_data = get_scheme(data['dbname'],data['product_id'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route("/get_broker_list", methods=['POST'])
def get_broker_list():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname']}   
       json_final_data = get_broker(data['dbname'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route("/get_demat_list", methods=['POST'])
def get_demat_list():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'broker_id':request.json['broker_id']}   
       json_final_data = get_demat(data['dbname'],data['broker_id'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route("/get_scrip_opening_balance", methods=['POST'])
def get_scrip_opening_balance():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'scrip_id':request.json['scrip_id'],
               'product_id':request.json['product_id'],
               'folio_number':request.json['folio_number'],}   
       json_final_data = get_scrip_opening(data['dbname'],data['scrip_id'],data['product_id'],data['folio_number'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route("/get_scrip_folio_list", methods=['POST'])
def get_scrip_folio_list():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'scrip_id':request.json['scrip_id'],
               'product_id':request.json['product_id']}   
       json_final_data = get_folios(data['dbname'],data['scrip_id'],data['product_id'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route("/insert_temp_transactions", methods=['POST'])
def insert_temp_transactions():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'scrip_id':request.json['scrip_id'],
               'product_id':request.json['product_id'],
               'folio_number':request.json['folio_number'],
               'transaction_type':request.json['transaction_type'],
               'quantity':request.json['quantity'],
               'gross_rate':request.json['gross_rate'],
               'gross_amount':request.json['gross_amount'],
               'brokerage':request.json['brokerage'],
               'stt':request.json['stt'],
               'net_rate':request.json['net_rate'],
               'user_name':request.json['user_name'],
               'computer_name':request.json['computer_name']}   
       json_final_data = insert_temp_transaction_register(data['dbname'],data['product_id'],data['scrip_id'],data['folio_number'],data['transaction_type'],data['quantity'],data['gross_rate'],data['gross_amount'],data['brokerage'],data['stt'],data['net_rate'],data['user_name'],data['computer_name'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route("/get_temp_transaction_data", methods=['POST'])
def get_temp_transaction_data():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'user_name':request.json['user_name'],
               'computer_name':request.json['computer_name']}   
       json_final_data = get_temp_transaction(data['dbname'],data['user_name'],data['computer_name'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route("/delete_temp_transaction_data", methods=['POST'])
def delete_temp_transaction_data():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'tableid':request.json['tableid']}   
       json_final_data = delete_temp_transaction(data['dbname'],data['tableid'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route("/get_total_for_temp_tran", methods=['POST'])
def get_total_for_temp_tran():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'user_name':request.json['user_name'],
               'computer_name':request.json['computer_name']}   
       json_final_data = get_total_for_temp_transaction(data['dbname'],data['user_name'],data['computer_name'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route("/get_product_ledger_data", methods=['POST'])
def get_product_ledger_data():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname']}   
       json_final_data = get_product_ledger_list(data['dbname'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route('/update_account_transaction_for_transaction_entry', methods=['POST'])
def update_account_transaction_for_transaction_entry():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    pd.options.mode.chained_assignment = None      
    from flask import Flask, request, jsonify
    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == 'asoidewfoef':  
  
       data = []
       data = {'dbname':request.json['dbname'],
               'date':request.json['date'],
               'broker_ledger':request.json['broker_ledger'],
               'broker_head':request.json['broker_head'],
               'contract_number':request.json['contract_number'],
               'net_amount':request.json['net_amount'],
               'gross_amount':request.json['gross_amount'],
               'investment_in_ledger':request.json['investment_in_ledger'],
               'investment_in_ledger_head':request.json['investment_in_ledger_head'],
               'createdby':request.json['createdby'],
               'computer_name':request.json['computer_name'],
               'stt_amount':request.json['stt_amount'],
               'other_charges_amount':request.json['other_charges_amount'],
               'round_off_amount':request.json['round_off_amount'],               
               'broker_id':request.json['broker_id'],
               'demat_id':request.json['demat_id'],
               'reference_number':request.json['reference_number'],
               'remarks':request.json['remarks'],
               'user_name':request.json['user_name']}
       
       if float(data['gross_amount'])<0:
               json_final_data = insert_data_in_at_from_transaction_entry(data['dbname'],1,data['date'],data['broker_head'],data['broker_ledger'],'',data['contract_number'],'Transaction done for contract number '+str(data['contract_number']),abs(float(data['gross_amount'])),data['investment_in_ledger_head'],data['investment_in_ledger'],data['computer_name'],data['createdby'])
       elif float(data['gross_amount'])>0:
            json_final_data = insert_data_in_at_from_transaction_entry(data['dbname'],0,data['date'],data['investment_in_ledger_head'],data['investment_in_ledger'],'',data['contract_number'],'Transaction done for contract number '+str(data['contract_number']),abs(float(data['gross_amount'])),data['broker_head'],data['broker_ledger'],data['computer_name'],data['createdby'])
       elif float(data['gross_amount'])==0:
            json_final_data = jsonify({"response": "ERROR: net amount cannot be zero "}), 200

       if float(data['stt_amount'])>0:
            json_final_data = insert_data_in_at_from_transaction_entry(data['dbname'],2,data['date'],2,8119,'',data['contract_number'],'Securities Transaction Tax on contract number '+str(data['contract_number']),abs(float(data['stt_amount'])),data['broker_head'],data['broker_ledger'],data['computer_name'],data['createdby'])
            
       if float(data['other_charges_amount'])>0:
            json_final_data = insert_data_in_at_from_transaction_entry(data['dbname'],3,data['date'],2,8120,'',data['contract_number'],'Other Transaction Tax on contract number '+str(data['contract_number']),abs(float(data['other_charges_amount'])),data['broker_head'],data['broker_ledger'],data['computer_name'],data['createdby'])
            
       data_from_db = pd.DataFrame()
       data_from_db = get_temp_data_from_transaction_register(data['dbname'],data['user_name'],data['computer_name'])
            
       for i in data_from_db.itertuples():
           json_final_data = insert_final_data_in_transaction_register(data['dbname'],data['date'],data['broker_id'],data['demat_id'],data['contract_number'],data['reference_number'],i[1],i[2],i[3],i[5],i[6],i[7],i[8],i[9],i[10],i[11],i[8]+i[9]+i[10],'web',data['createdby'],data['remarks'])
       
       delete_temp_transaction_permanently_post_entry(data['dbname'],data['user_name'],data['computer_name'])      
      
    else:           
       json_final_data = "ERROR: Unauthorized Access"
         
       
    return jsonify({"response": json_final_data})

@app.route("/get_auto_debit_transactions_list", methods=['POST'])
def get_auto_debit_transactions_list():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'from_date':request.json['from_date'],
               'to_date':request.json['to_date']}   
       json_final_data = get_auto_debit_transaction_data(data['dbname'],data['from_date'],data['to_date'])

   else:
      
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
         
   return json_final_data

@app.route('/post_auto_transaction_entry', methods=['POST'])
def post_auto_transaction_entry():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    pd.options.mode.chained_assignment = None      
    from flask import Flask, request, jsonify
    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == 'asoidewfoef':  
  
       data_json = {'dbname':request.json['dbname'],
               'auto_debit_table_id':request.json['auto_debit_table_id'],
               'units':request.json['units'],
               'gross_rate':request.json['gross_rate'],
               'gross_amount':request.json['gross_amount'],
               'investment_in_ledger':request.json['investment_in_ledger'],
               'bank_ledger':request.json['bank_ledger'],               
               'date':request.json['date'],
               'scrip_name':request.json['scrip_name'],
               'computer_name':request.json['computer_name'],
               'broker_id':request.json['broker_id'],
               'created_by':request.json['created_by'],
               'product_id':request.json['product_id'],
               'scrip_id':request.json['scrip_id'],
               'folio_number':request.json['folio_number']}
       
       data = data_json
       json_final_data1 = insert_data_in_at_from_transaction_entry(data['dbname'],0,data['date'],0,data['investment_in_ledger'],'',data['auto_debit_table_id'],'PURCHASE OF ' + str(data['scrip_name'])+' - UNITS - '+str(data['units']),abs(float(data['gross_amount'])),0,data['bank_ledger'],data['computer_name'],data['created_by'])   
       json_final_data = insert_final_data_in_transaction_register(data['dbname'],data['date'],data['broker_id'],0,data['auto_debit_table_id'],'auto_debit_transaction',data['product_id'],data['scrip_id'],data['folio_number'],'Buy',data['units'],data['gross_rate'],data['gross_amount'],0,0,data['gross_rate'],data['gross_amount'],'web',data['created_by'],'auto_debit_transaction')
       update_auto_debit_transaction(data['dbname'],data['gross_amount'],data['bank_ledger'],data['date'],data['auto_debit_table_id'])      

    else:           
       json_final_data = "ERROR: Unauthorized Access"
         
       
    return jsonify({"response": json_final_data})

@app.route("/get_bank_list", methods=['POST'])
def get_bank_list():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname']}   
      
       json_final_data = get_bank_names(data['dbname'])
   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
         
   return json_final_data

@app.route("/get_ledger_opening_from_master", methods=['POST'])
def get_ledger_opening_from_master():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'account_ledger':request.json['account_ledger']}   
      
       json_final_data = get_ledger_opening(data['dbname'],data['account_ledger'])
   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
         
   return json_final_data

@app.route("/update_opening_to_master", methods=['POST'])
def update_opening_to_master():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'account_ledger':request.json['account_ledger'],
               'debit_amount':request.json['debit_amount'],
               'credit_amount':request.json['credit_amount']}   
      
       json_final_data = update_new_opening_balance_for_ledger(data['dbname'],data['account_ledger'],data['debit_amount'],data['credit_amount'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
         
   return json_final_data

@app.route("/insert_temp_journal", methods=['POST'])
def insert_temp_journal():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':     
       data = []
       data = {'dbname':request.json['dbname'],
               'date':request.json['date'],
               'account_ledger':request.json['account_ledger'],
               'account_head':request.json['account_head'],
               'drcr':request.json['drcr'],
               'vouchernumber':request.json['vouchernumber'],
               'standard_description':request.json['standard_description'],
               'amount':request.json['amount'],
               'computername':request.json['computername'],
               'createdby':request.json['createdby']}               
      
       json_final_data = insert_data_in_temp_journal(data['dbname'],data['date'],data['account_head'],data['account_ledger'],data['drcr'],data['vouchernumber'],data['standard_description'],data['amount'],data['computername'],data['createdby'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
         
   return json_final_data

@app.route("/delete_temp_journal_transaction", methods=['POST'])
def delete_temp_journal_transaction():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'tableid':request.json['tableid']}   
      
       json_final_data = delete_temp_journal_entry(data['dbname'],data['tableid'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route("/get_temp_journal_transaction_data", methods=['POST'])
def get_temp_journal_transaction_data():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'createdby':request.json['createdby'],
               'computername':request.json['computername']}   
       json_final_data = get_temp_journal_transaction(data['dbname'],data['createdby'],data['computername'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route('/insert_in_journal_entry', methods=['POST'])
def insert_in_journal_entry():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    pd.options.mode.chained_assignment = None      
    from flask import Flask, request, jsonify
    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == 'asoidewfoef':  
  
       data = []
       data = {'dbname':request.json['dbname'],
               'voucher_number':request.json['voucher_number'],
               'createdby':request.json['createdby'],
               'computername':request.json['computername']}
 
       db=data['dbname']
       try:
           conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)
       
       cur = conn.cursor()
       data_from_db = pd.DataFrame()
       data_from_db = get_temp_journal_transaction_as_dataframe(data['dbname'],data['createdby'],data['computername'])
       
       asn = get_auto_serial_number(data['dbname'])
       for i in asn.itertuples():  
           sn = i[1]  
         
       for i in data_from_db.itertuples():
           query = "exec Usp_T_Insert_in_Journal_Web 0,'%s',%s,%s,'%s','%s','%s','%s',%s,'%s','%s'"%(i[2],i[3],i[4],i[5],data['voucher_number'],str(sn),i[7],i[8],data['computername'],data['createdby'])
           print(query)
           a = cur.execute(query)
           cur.commit()

           check_e = a.rowcount
           if check_e>=1:
               json_final_data = "Saved Successfully"
           else:
               json_final_data = "Data not Saved"       
      
       #delete_temp_transaction_permanently_post_entry_for_journal(data['dbname'],data['createdby'],data['computername'])      
      
    else:           
       json_final_data = "ERROR: Unauthorized Access"
         
       
    return jsonify({"response": json_final_data})

@app.route("/get_cash_and_bank_book_entry_date", methods=['POST'])
def get_cash_and_bank_book_entry_date():
       import pyodbc
       import pandas as pd
       import pandas.io.sql as psql
       from flask import Flask, request, jsonify

       headers = request.headers
       auth = headers.get("X-Api-Key")
       if auth == 'asoidewfoef':  

          data = []
          data = {'dbname':request.json['dbname'],
                  'from_date':request.json['from_date'],
                  'to_date':request.json['to_date']}

          db=data['dbname']
          try:
              conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
          except Exception as e:
              print(e)

          query = "exec Usp_T_BankBook 0,3,'','%s','%s'"%(data['from_date'],data['to_date'])

          abc = pd.read_sql(query, conn)    
          json_final_data = abc.to_json(orient='records', date_format = 'iso')
          conn.close()

       else:

          json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401
            
       return json_final_data

@app.route("/delete_cash_or_bank_book_entry_data", methods=['POST'])
def delete_cash_or_bank_book_entry_data():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'entry_type':request.json['entry_type'],
               'tableid':request.json['tableid'],
               'auto_serial_number':request.json['auto_serial_number']}   
       
       if (data['dbname']=='cash_book'):
           json_final_data = delete_cash_book_entry(data['dbname'],data['tableid'],data['auto_serial_number'])
       else:
           json_final_data = delete_bank_book_entry(data['dbname'],data['tableid'],data['auto_serial_number']) 

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route("/get_journal_book_entry_data", methods=['POST'])
def get_journal_book_entry_data():
       import pyodbc
       import pandas as pd
       import pandas.io.sql as psql
       from flask import Flask, request, jsonify

       headers = request.headers
       auth = headers.get("X-Api-Key")
       if auth == 'asoidewfoef':  

          data = []
          data = {'dbname':request.json['dbname'],
                  'from_date':request.json['from_date'],
                  'to_date':request.json['to_date']}

          db=data['dbname']
          try:
              conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
          except Exception as e:
              print(e)

          query = "Select Distinct Amount,StandardDescription1 as [Desciption],convert(varchar,Date,103) as Date,AutoSerialNumber,VoucherNumber From T_Journal WHERE [Date] BETWEEN convert(varchar,%s,101) and convert(varchar,%s,101)"%(data['from_date'],data['to_date'])

          abc = pd.read_sql(query, conn)    
          json_final_data = abc.to_json(orient='records', date_format = 'iso')
          conn.close()

       else:

          json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401

@app.route('/get_equity_holding', methods=['POST'])
def get_equity_holding(): 
        import pyodbc
        import pandas as pd
        import pandas.io.sql as psql
        import json
        import datetime as dt
        from pandas.io.json import json_normalize
        from datetime import datetime
        from datetime import timedelta
        from bsedata.bse import BSE
        from flask import Flask, request, jsonify
        pd.options.mode.chained_assignment = None    

        headers = request.headers
        auth = headers.get("X-Api-Key")
        if auth == 'asoidewfoef':  
           data = []
           data = {'dbname':request.json['dbname']}      

           db=data['dbname']
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
               from pandas.io.json import json_normalize
               from nsetools import Nse
               import json
               nse = Nse()
               import json
               from pandas.io.json import json_normalize
               a = nse.is_valid_code(scrip_name)
               b = str(a)
               if b == 'False':
                   filesd = []
                   filesd=  [[scrip_name,0]]
                   filesdata = pd.DataFrame(filesd,columns = ['symbol','lastPrice'])
               if b == 'True':
                   q = nse.get_quote(str(scrip_name))
                   f = json.dumps(q)
                   json_f = json.loads(f)
                   filesdata = json_normalize(json_f)
                  
               return filesdata

           data1 = abc

           #### equity scheme calculation

           date = (datetime.now() + timedelta(minutes=330)).strftime('%d-%b-%Y')
           overall_data = pd.DataFrame()    
           equity_data = data1.loc[data1['product_name']=='Equity'] 
           equity_data = equity_data[equity_data['scrip_code']!='']
           equity_data = equity_data.fillna('not_available')
           equity_data = equity_data.loc[equity_data['scrip_code']!='not_available']
           if not equity_data.empty:
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

           if equity_data.empty:
               json_final_data = jsonify({"response": "no data to show"})
               exit()  
           else:
               frames = [equity_final_data]            
           
           combined_holding_data = pd.concat(frames)

           combined_holding_data = combined_holding_data.rename(columns={'closing_shares': 'quantity'}) 
           combined_holding_data = combined_holding_data.rename(columns={'total_amount': 'total_cost'}) 
           combined_holding_data = combined_holding_data.rename(columns={'nav': 'current_price'})
           combined_holding_data = combined_holding_data.rename(columns={'date': 'valuation_date'}) 
           combined_holding_data = combined_holding_data.rename(columns={'gain/loss': 'gainloss'}) 
           combined_holding_data = combined_holding_data.rename(columns={'absolute_gain(%)': 'absolute_gain'})  
           combined_holding_data = combined_holding_data[['product_name','scrip_code','Particulars','quantity','cost_price','total_cost','current_price','valuation_date','current_value','gainloss','absolute_gain']]
           combined_holding_data = combined_holding_data.round({'quantity' : 4,'cost_price' : 2,'gainloss' : 2,'absolute_gain':2,'total_cost':2,'current_price':2,'current_value':2})    

           json_final_data = combined_holding_data.to_json(orient='records', date_format = 'iso')
         
        else:           
           json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401          
             
        return json_final_data

@app.route("/delete_journal_entry_data", methods=['POST'])
def delete_journal_entry_data():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'auto_serial_number':request.json['auto_serial_number']}   
       json_final_data = delete_journal_book_entry(data['dbname'],data['auto_serial_number'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data 

@app.route("/edit_bank_data", methods=['POST'])
def edit_bank_data():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'auto_serial_number':request.json['auto_serial_number']}   
       json_final_data = get_bank_book_data_for_auto_serial_number(data['dbname'],data['auto_serial_number'])
   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data 

@app.route('/get_mutual_fund_holdings', methods=['POST'])
def get_mutual_fund_holdings(): 
        import pyodbc
        import pandas as pd
        import pandas.io.sql as psql
        import json
        import datetime as dt
        from pandas.io.json import json_normalize
        from datetime import datetime
        from datetime import timedelta
        from bsedata.bse import BSE
        from flask import Flask, request, jsonify
        pd.options.mode.chained_assignment = None    

        headers = request.headers
        auth = headers.get("X-Api-Key")
        if auth == 'asoidewfoef':  
           data = []
           data = {'dbname':request.json['dbname']}      

           db=data['dbname']
           try:
               conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
           except Exception as e:
               print(e)

           cur = conn.cursor()

           query = "exec usp_r_holding 2"

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
               mf_nav_data =r.content
               return mf_nav_data

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

           data1 = abc

           #### mutual fund scheme calculation
           mutual_fund_data = data1.loc[data1['product_name']=='mutual fund']    
           scehme_codes = mutual_fund_data['scrip_code']
           scheme_code_stirng = '{'+ '"' + 'schemeCodes' + '"' + ":["
           for i in scehme_codes:
               scheme_code_stirng = scheme_code_stirng + '"' + str(i[:6]) + '"' + ","
           mf_code_string = replace_last(scheme_code_stirng, ',', ']}')  
           a = json.loads(get_mutual_fund_nav(mf_code_string))
           del a['meta']
           my_json_string = json.dumps(a)
           df = pd.read_json(my_json_string)
           mf_data = df.data.apply(pd.Series)         
           mf_data['schemeCode'] = pd.to_numeric(mf_data['schemeCode'])
           mutual_fund_data['scrip_code'] = pd.to_numeric(mutual_fund_data['scrip_code'])
           mutual_fund_data['total_amount'] = pd.to_numeric(mutual_fund_data['total_amount'])
           mutual_fund_data['closing_shares'] = pd.to_numeric(mutual_fund_data['closing_shares'])
           mf_final_data = pd.merge(mutual_fund_data,mf_data,left_on='scrip_code',right_on='schemeCode',how='left')
           mf_final_data = mf_final_data[['product_name','scrip_code','Particulars','closing_shares','total_amount','nav','date']]
           mf_final_data['cost_price'] = mf_final_data.apply(calculate_cost, axis =1) 
           mf_final_data['current_value'] = mf_final_data.apply(calculate_current_value, axis =1) 
           mf_final_data['gain/loss'] = mf_final_data.apply(calculate_gain_loss, axis =1) 
           mf_final_data['absolute_gain(%)'] = mf_final_data.apply(calculate_absolute_gain, axis =1) 
           mf_final_data = mf_final_data.round({'total_amount' : 2,'current_value' : 2,'gain/loss' : 2,'absolute_gain(%)':2})

           combined_holding_data = pd.DataFrame(mf_final_data)

           combined_holding_data = combined_holding_data.rename(columns={'closing_shares': 'quantity'}) 
           combined_holding_data = combined_holding_data.rename(columns={'total_amount': 'total_cost'}) 
           combined_holding_data = combined_holding_data.rename(columns={'nav': 'current_price'})
           combined_holding_data = combined_holding_data.rename(columns={'date': 'valuation_date'}) 
           combined_holding_data = combined_holding_data.rename(columns={'gain/loss': 'gainloss'}) 
           combined_holding_data = combined_holding_data.rename(columns={'absolute_gain(%)': 'absolute_gain'})  
           combined_holding_data = combined_holding_data[['product_name','scrip_code','Particulars','quantity','cost_price','total_cost','current_price','valuation_date','current_value','gainloss','absolute_gain']]
           combined_holding_data = combined_holding_data.round({'quantity' : 4,'cost_price' : 2,'gainloss' : 2,'absolute_gain':2,'total_cost':2,'current_price':2,'current_value':2})    

           json_final_data = combined_holding_data.to_json(orient='records', date_format = 'iso')

           return json_final_data  
         
        else:           
           json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401
           return json_final_data

@app.route("/get_broker_ledger_number", methods=['GET'])
def get_broker_ledger_number():
   from flask import Flask, request, jsonify
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname'],
               'broker_id':request.json['broker_id']}   
       json_final_data = get_broker_id_ledger(data['dbname'],data['broker_id'])

   else:
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401   
   return json_final_data

@app.route("/get_investment_pie_graph", methods=['POST'])
def get_investment_pie_graph():
   import pyodbc
   import io
   import random
   import pandas as pd
   import pandas.io.sql as psql
   import json
   import datetime as dt
   from pandas.io.json import json_normalize
   from flask import Flask, request, jsonify
   pd.options.mode.chained_assignment = None
   import matplotlib.pyplot as plt
   from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
   from matplotlib.figure import Figure
   from flask import Response
   import warnings
   import base64
   warnings.filterwarnings("ignore")
   
   headers = request.headers
   auth = headers.get("X-Api-Key")
   if auth == 'asoidewfoef':       
       data = []
       data = {'dbname':request.json['dbname']}
       print(data)
       db=data['dbname']

       try:
           conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db+';UID='+user+';PWD='+ password)
       except Exception as e:
           print(e)
            
       query = "exec Usp_R_Queries_for_Charts 0"

       abc = pd.read_sql(query, conn)
       conn.close() 

       img = io.BytesIO()
       ax1 = plt.subplot(121, aspect='equal')
       plot = abc.plot(kind='pie', y='percentage_investment', ax=ax1, figsize=(20,20),autopct='%1.0f%%', 
             startangle=180, shadow=False, labels=abc['investment_type'], legend = False, fontsize=9)
#        plot = plt.legend(bbox_to_anchor=(1.2, 1), loc=1, borderaxespad=0)
       plot = plt.axis('off')
       plt.savefig(img, format='png')
       img.seek(0)
       plot_url = base64.b64encode(img.getvalue()).decode()
       return '<img src="data:image/png;base64,{}">'.format(plot_url)
   else:           
       json_final_data = jsonify({"response": "ERROR: Unauthorized Access"}), 401
       return json_final_data               

@app.route('/update_salary_entries', methods=['POST'])
def update_salary_entries():
    
    import pyodbc
    import pandas as pd
    import pandas.io.sql as psql
    pd.options.mode.chained_assignment = None      
    from flask import Flask, request, jsonify
    headers = request.headers
    auth = headers.get("X-Api-Key")
    if auth == 'asoidewfoef':  
  
       data = []
       data = {'dbname':request.json['dbname'],
               'date':request.json['date'],
               'account_ledger':request.json['account_ledger'],
               'account_head':request.json['account_head'],
               'amount':request.json['amount'],
               'salary_from_ledger_head':request.json['salary_from_ledger_head'],
               'salary_from_ledger':request.json['salary_from_ledger'],
               'DRCR':request.json['DRCR'],
               'createdby':request.json['createdby'],
               'computer_name':request.json['computer_name'],
               'voucher_number':request.json['voucher_number'],
               'description':request.json['description']}
       

       json_final_data = insert_data_in_account_transaction(data['dbname'],4,data['date'],data['account_head'],data['account_ledger'],data['DRCR'],data['voucher_number'],data['description'],abs(float(data['amount'])),data['salary_from_ledger_head'],data['salary_from_ledger'],data['computer_name'],data['createdby'])
      
    else:           
       json_final_data = "ERROR: Unauthorized Access"
         
       
    return jsonify({"response": json_final_data})   
