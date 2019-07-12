from flask import Flask
from flask_restful import Resource, Api 
app = Flask(__name__)
api = Api(app) 
@app.route("/")
#def hello():
#    return "Hello Himanshu"

class api_status(Resource):
    def get(self):
        '''
        The aim of this function is to check if the API is up
        '''
        return 'http 200'

api.add_resource(api_status, '/healthcheck')
