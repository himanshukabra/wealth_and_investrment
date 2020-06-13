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
import warnings
warnings.filterwarnings("ignore")

user="shsa"
server="103.212.121.67"
password="Easeprint#021"

def get_investment_graph(db_name):

    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db_name+';UID='+user+';PWD='+ password)
    except Exception as e:
        print(e)

    query = "exec Usp_R_Queries_for_Charts 0"

    abc = pd.read_sql(query, conn)

    conn.close() 
    
    def create_figure(abc):
        fig = Figure()
        axis = fig.add_subplot(1, 1, 1)
        ax1 = plt.subplot(121, aspect='equal')
        plot = abc.plot(kind='pie', y='percentage_investment', ax=ax1, figsize=(20,20),autopct='%1.0f%%', 
               startangle=180, shadow=False, labels=abc['investment_type'], legend = False, fontsize=9)
        plt.legend(bbox_to_anchor=(1.2, 1), loc=1, borderaxespad=0)
        plt.axis('off')
        return fig
    
    fig = create_figure(abc)
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')    
