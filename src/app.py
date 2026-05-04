from flask import Flask
from flask_pymongo import PyMongo
from threads import close_running_threads, initThreads
import atexit
from routes import api_blueprint 

# We start the Flask application
app = Flask(__name__)

#Add swagger

# Register the function to be called on exit
atexit.register(close_running_threads)
# We start the threads that should be active
initThreads()
# Import API routes
app.register_blueprint(api_blueprint, url_prefix="/api")

if __name__ == '__main__':
    app.run(debug=True)