from flask_login import UserMixin
from itsdangerous import URLSafeTimedSerializer
from flask import current_app, url_for
import json
import logging
from datetime import datetime
from app.objects import get_db_connection, EmailManager, AuthManager

logger = logging.getLogger("medical_records_module.objects")


def _patients_table_exists(cursor) -> bool:
    cursor.execute(
        "SELECT 1 FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
        ("patients",),
    )
    return cursor.fetchone() is not None


class Patient:
    @staticmethod
    def search_by_dob_and_postcode(dob, postcode):
        """
        Search for patients matching the given date of birth and postcode.
        Returns a list of dictionaries representing patient records.
        """
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        if not _patients_table_exists(cursor):
            cursor.close()
            conn.close()
            return []
        query = """
            SELECT id, first_name, middle_name, last_name, address, date_of_birth, gender, postcode, package_type, care_company_id, access_requirements, notes
            FROM patients
            WHERE date_of_birth = %s AND postcode = %s
        """
        cursor.execute(query, (dob, postcode))
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results

    @staticmethod
    def get_all(search=None, company_id=None):
        """
        Retrieves patients.
        If company_id is provided, only returns records for that company.
        If search is provided, filters by first name, last name, or id.
        """
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        if not _patients_table_exists(cursor):
            cursor.close()
            conn.close()
            return []
        if company_id is not None:
            if search:
                search_param = "%" + search + "%"
                query = """
                    SELECT * FROM patients
                    WHERE care_company_id = %s 
                      AND (first_name LIKE %s OR last_name LIKE %s OR CAST(id AS CHAR) LIKE %s)
                """
                cursor.execute(query, (company_id, search_param, search_param, search_param))
            else:
                query = "SELECT * FROM patients WHERE care_company_id = %s"
                cursor.execute(query, (company_id,))
        else:
            if search:
                search_param = "%" + search + "%"
                query = """
                    SELECT * FROM patients
                    WHERE first_name LIKE %s OR last_name LIKE %s OR CAST(id AS CHAR) LIKE %s
                """
                cursor.execute(query, (search_param, search_param, search_param))
            else:
                query = "SELECT * FROM patients"
                cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results

    @staticmethod
    def get_by_id(id):
        """
        Retrieves a patient by id.
        """
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        if not _patients_table_exists(cursor):
            cursor.close()
            conn.close()
            return None
        query = "SELECT * FROM patients WHERE id = %s"
        cursor.execute(query, (id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result

    @staticmethod
    def add_patient(nhs_number, first_name, middle_name, last_name, address,
                    gp_details, medical_conditions, allergies, medications, previous_visit_records,
                    package_type, notes, message_log, access_requirements, payment_details,
                    next_of_kin_details, lpa_details, resuscitation_directive, documents, date_of_birth,
                    weight, gender, postcode, care_company_id, contact_number):
        """
        Inserts a new patient record. The auto-incremented 'id' is used as the primary key.
        """
        if care_company_id == '':
            care_company_id = None

        conn = get_db_connection()
        cursor = conn.cursor()
        if not _patients_table_exists(cursor):
            cursor.close()
            conn.close()
            logger.warning("Patient.add_patient skipped: patients table not present")
            return
        query = """
            INSERT INTO patients (
                nhs_number, first_name, middle_name, last_name, address, contact_number,
                gp_details, medical_conditions, allergies, medications, previous_visit_records,
                package_type, notes, message_log, access_requirements, payment_details,
                next_of_kin_details, lpa_details, resuscitation_directive, documents,
                date_of_birth, weight, gender, postcode, care_company_id, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
        cursor.execute(query, (
            nhs_number, first_name, middle_name, last_name, address, contact_number,
            gp_details, medical_conditions, allergies, medications, previous_visit_records,
            package_type, notes, message_log, access_requirements, payment_details,
            next_of_kin_details, lpa_details, resuscitation_directive, documents,
            date_of_birth, weight, gender, postcode, care_company_id
        ))
        conn.commit()
        cursor.close()
        conn.close()

    @staticmethod
    def update_patient(id, **kwargs):
        """
        Updates the patient record identified by id.
        Accepts keyword arguments corresponding to the patient table columns.
        For example:
            Patient.update_patient(1, first_name='John', last_name='Doe', weight=75.50)
        This method automatically updates the 'updated_at' column.
        """
        if not kwargs:
            return
        conn = get_db_connection()
        cursor = conn.cursor()
        if not _patients_table_exists(cursor):
            cursor.close()
            conn.close()
            logger.warning("Patient.update_patient skipped: patients table not present")
            return
        fields = []
        values = []
        for key, value in kwargs.items():
            fields.append(f"{key} = %s")
            values.append(value)
        values.append(id)
        query = f"UPDATE patients SET {', '.join(fields)}, updated_at = CURRENT_TIMESTAMP WHERE id = %s"
        cursor.execute(query, tuple(values))
        conn.commit()
        cursor.close()
        conn.close()

    @staticmethod
    def delete_patient(id):
        """
        Deletes the patient record identified by id.
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        if not _patients_table_exists(cursor):
            cursor.close()
            conn.close()
            logger.warning("Patient.delete_patient skipped: patients table not present")
            return
        query = "DELETE FROM patients WHERE id = %s"
        cursor.execute(query, (id,))
        conn.commit()
        cursor.close()
        conn.close()


# -----------------------------------------------------------------------------
# AuditLog: raw MySQL implementation for logging audit entries.
# -----------------------------------------------------------------------------
class AuditLog:
    @staticmethod
    def insert_log(
        user,
        action,
        patient_id=None,
        *,
        case_id=None,
        principal_role=None,
        route=None,
        ip=None,
        user_agent=None,
        reason=None,
    ):
        """
        Write an audit log entry.

        Backwards-compatible: will fall back to legacy schema if new columns are absent.
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        timestamp = datetime.utcnow()

        # Preferred (case-first) schema.
        try:
            query = """
                INSERT INTO audit_logs
                  (user, principal_role, action, case_id, patient_id, route, ip, user_agent, reason, timestamp)
                VALUES
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(
                query,
                (
                    user,
                    principal_role,
                    action,
                    case_id,
                    patient_id,
                    route,
                    ip,
                    user_agent,
                    reason,
                    timestamp,
                ),
            )
            conn.commit()
            return
        except Exception:
            # Legacy table (user, action, patient_id, timestamp).
            conn.rollback()
            query = """
                INSERT INTO audit_logs (user, action, patient_id, timestamp)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(query, (user, action, patient_id, timestamp))
            conn.commit()
        finally:
            cursor.close()
            conn.close()


# -----------------------------------------------------------------------------
# Prescription: raw MySQL implementation for prescription records.
# -----------------------------------------------------------------------------
class Prescription:
    @staticmethod
    def insert_prescription(patient_id, prescribed_by, prescription_text):
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO prescriptions (patient_id, prescribed_by, prescription, created_at)
            VALUES (%s, %s, %s, %s)
        """
        created_at = datetime.utcnow()
        cursor.execute(query, (patient_id, prescribed_by, prescription_text, created_at))
        conn.commit()
        cursor.close()
        conn.close()


# -----------------------------------------------------------------------------
# CareCompanyUser: raw MySQL implementation for care company user management.
# -----------------------------------------------------------------------------
class CareCompanyUser(UserMixin):
    def __init__(self, id, username, email, password_hash, company_name,
                 contact_phone, contact_address, company_pin_hash=None):
        self.id = id
        self.username = username
        self.email = email
        self.password_hash = password_hash
        self.company_name = company_name
        self.contact_phone = contact_phone
        self.contact_address = contact_address
        self.company_pin_hash = company_pin_hash

    @staticmethod
    def get_all(search=None):
        """
        Returns a list of care companies matching the 'search' text,
        searching by company_name or id, limited to 50 results.
        """
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        base_query = "SELECT * FROM care_company_users"
        params = []

        if search:
            # We'll search partial match on company_name or ID as string
            base_query += " WHERE company_name LIKE %s OR CAST(id AS CHAR) LIKE %s"
            like_pattern = f"%{search}%"
            params.extend([like_pattern, like_pattern])

        # Limit to 50 results (or whatever suits your use case)
        base_query += " LIMIT 50"

        cursor.execute(base_query, params)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        # Convert rows to a list of dicts with only the fields you care about
        results = []
        for row in rows:
            results.append({
                "id": row["id"],
                "company_name": row["company_name"]
            })
        return results


    def get_id(self):
        return f"Vita-Care-Portal:{self.id}"

    def set_password(self, password):
        self.password_hash = AuthManager.hash_password(password)

    def check_password(self, password):
        return AuthManager.verify_password(self.password_hash, password)

    def set_company_pin(self, pin):
        self.company_pin_hash = AuthManager.hash_password(pin)

    def check_company_pin(self, pin):
        if not self.company_pin_hash or self.company_pin_hash.strip() == "":
            return False
        return AuthManager.verify_password(self.company_pin_hash, pin)

    def generate_reset_token(self, expires_in=600):
        s = URLSafeTimedSerializer(current_app.config.get("SECRET_KEY"))
        return s.dumps({'user_id': self.id})

    @staticmethod
    def verify_reset_token(token, expires_in=600):
        s = URLSafeTimedSerializer(current_app.config.get("SECRET_KEY"))
        try:
            data = s.loads(token, max_age=expires_in)
        except Exception:
            return None
        return CareCompanyUser.get_user_by_id(data.get('user_id'))

    @staticmethod
    def get_user_by_id(user_id):
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM care_company_users WHERE id = %s"
        cursor.execute(query, (user_id,))
        user_data = cursor.fetchone()
        cursor.close()
        conn.close()
        if user_data:
            return CareCompanyUser(
                user_data["id"],
                user_data["username"],
                user_data["email"],
                user_data["password_hash"],
                user_data["company_name"],
                user_data.get("contact_phone"),
                user_data.get("contact_address"),
                user_data.get("company_pin_hash")
            )
        return None

    @staticmethod
    def get_user_by_username(username):
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM care_company_users WHERE username = %s"
        cursor.execute(query, (username,))
        user_data = cursor.fetchone()
        cursor.close()
        conn.close()
        if user_data:
            return CareCompanyUser(
                user_data["id"],
                user_data["username"],
                user_data["email"],
                user_data["password_hash"],
                user_data["company_name"],
                user_data.get("contact_phone"),
                user_data.get("contact_address"),
                user_data.get("company_pin_hash")
            )
        return None

    @staticmethod
    def get_user_by_email(email):
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM care_company_users WHERE email = %s"
        cursor.execute(query, (email,))
        user_data = cursor.fetchone()
        cursor.close()
        conn.close()
        if user_data:
            return CareCompanyUser(
                user_data["id"],
                user_data["username"],
                user_data["email"],
                user_data["password_hash"],
                user_data["company_name"],
                user_data.get("contact_phone"),
                user_data.get("contact_address"),
                user_data.get("company_pin_hash")
            )
        return None

    @staticmethod
    def update_password(user_id, new_hash):
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "UPDATE care_company_users SET password_hash = %s WHERE id = %s"
        cursor.execute(query, (new_hash, user_id))
        conn.commit()
        cursor.close()
        conn.close()

    @staticmethod
    def update_company_pin(user_id, new_pin_hash):
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "UPDATE care_company_users SET company_pin_hash = %s WHERE id = %s"
        cursor.execute(query, (new_pin_hash, user_id))
        conn.commit()
        cursor.close()
        conn.close()

    def send_initial_reset_email(self):
        token = self.generate_reset_token()
        subject = "Set Your Password for the Vita Care Portal"
        reset_url = url_for('care_company.reset_password', token=token, _external=True)
        text_body = f"""Dear {self.company_name},

An account has been created for you on the Vita Care Portal.
Please set your password by clicking the following link:
{reset_url}

If you did not expect this email, please contact support.

Kind regards,
The Support Team
"""
        html_body = f"""<p>Dear {self.company_name},</p>
<p>An account has been created for you on the Vita Care Portal.</p>
<p>Please set your password by clicking the following link:</p>
<p><a href="{reset_url}">{reset_url}</a></p>
<p>If you did not expect this email, please contact support.</p>
<p>Kind regards,<br>The Support Team</p>"""
        email_manager = EmailManager()
        email_manager.send_email(
            subject=subject,
            body=text_body,
            recipients=[self.email]
        )

    @staticmethod
    def add_user(username, email, password, company_name, contact_phone, contact_address, company_pin=""):
        """
        Inserts a new care company user into the database.
        Since the 'id' column is an auto-increment integer, we do not include it in the INSERT.
        """
        new_password_hash = AuthManager.hash_password(password)
        new_company_pin_hash = None
        if company_pin and company_pin.strip() != "":
            new_company_pin_hash = AuthManager.hash_password(company_pin)
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO care_company_users 
            (username, email, password_hash, company_name, contact_phone, contact_address, company_pin_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (username, email, new_password_hash, company_name, contact_phone, contact_address, new_company_pin_hash))
        conn.commit()
        cursor.close()
        conn.close()
