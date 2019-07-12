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
