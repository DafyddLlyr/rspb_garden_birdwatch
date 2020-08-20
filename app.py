from flask import Flask, jsonify
from flasgger import Swagger
from flask_script import Manager
from flask_restful import Api

app = Flask(__name__)
swagger = Swagger(app)

def bootstrap_app():
    initialise_flask_restful_routes(app)

def initialise_flask_restful_routes(app):
    """ Define the routes the API exposes using Flask-Restful """
    api = Api(app, default_mediatype='application/json')
    from server.api.county_api import CountyAPI
    from server.api.bird_api import BirdAPI

    api.add_resource(CountyAPI, '/api/v1/counties')
    api.add_resource(BirdAPI, '/api/v1/birds')

application = bootstrap_app()
manager = Manager(application)

if __name__ == "__main__":
    manager.run()