from flask_restful import Resource

class BirdAPI(Resource):

    def get(self):
        """ Get RSPB Garden Birdwatch results by bird
        ---
        tags:
          - bird
        responses:
          200:
            description: Request successful
          500:
            description: Internal Server Error
        """
        try:
            return {
                    "msg": "hello world"
                   }, 200
        except Exception as e:
            error_msg = f'Unhandled exception: {str(e)}'
            return {"errorMessage": error_msg}, 500
