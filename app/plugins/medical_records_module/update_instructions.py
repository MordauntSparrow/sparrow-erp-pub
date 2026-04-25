"""
Run: python app/plugins/medical_records_module/update_instructions.py
"""
import install as installer

if __name__ == "__main__":
    installer.upgrade()
    print("Medical Records / Cura schema upgrade complete.")
