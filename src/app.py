from flask import Flask
from database import initDatabase
from threads import close_running_threads, initThreads
import atexit

# We start the Flask application
app = Flask(__name__)
#Init database
initDatabase(app)
# Register the function to be called on exit
atexit.register(close_running_threads)
# We start the threads that should be active
initThreads()
# Import API routes
import routes