# app/__init__.py

from .create_app import create_app  # Import the create_app function
from flask_socketio import SocketIO

# Module-level SocketIO instance. It will be initialized with the Flask app
# in create_app() via socketio.init_app(app, ...).
socketio = SocketIO()


# The create_app function is called in run.py to initialize the Flask app
