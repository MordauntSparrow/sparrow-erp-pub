# In app/plugins/medical_records_module/__init__.py
from .routes import public_bp  # Import the blueprint from your routes file
from .objects import CareCompanyUser  # Import the user class (unchanged)

# Expose the login attributes for dynamic registration with the updated prefix only
login_prefix = "Vita-Care-Portal"
get_user_by_id = CareCompanyUser.get_user_by_id