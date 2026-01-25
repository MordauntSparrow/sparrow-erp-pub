# In app/plugins/medical_records_module/__init__.py
from .routes import public_bp  # Import the blueprint from your routes file
# from .objects import staffUser  # Import the user class (unchanged)

# Expose the login attributes for dynamic registration with the updated prefix only
login_prefix = "event_medical_manager"
# get_user_by_id = StaffUser.get_user_by_id
