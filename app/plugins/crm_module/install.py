"""
CRM module install/upgrade: CRM core, quotes, activities, medical event planner (R2/R3).
Run from repo root: python app/plugins/crm_module/install.py install
Or: python app/plugins/crm_module/install.py upgrade
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
PLUGIN_DIR = HERE.parent
PLUGINS_DIR = PLUGIN_DIR.parent
APP_ROOT = PLUGINS_DIR.parent
PROJECT_ROOT = APP_ROOT.parent
for p in (str(PROJECT_ROOT), str(APP_ROOT), str(PLUGIN_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from app.objects import get_db_connection  # noqa: E402


def _create_table(conn, name: str, columns_sql: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{name}` (
                {columns_sql}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        conn.commit()
        print(f"[crm_module] Ensured table: {name}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _alter_add_column(conn, table: str, col_def: str) -> None:
    parts = col_def.strip().split(None, 1)
    col_name = parts[0]
    rest = parts[1] if len(parts) > 1 else ""
    cur = conn.cursor()
    try:
        cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{col_name}` {rest}")
        conn.commit()
        print(f"[crm_module] Added column {table}.{col_name}")
    except Exception as e:
        if "Duplicate column" not in str(e):
            print(f"[crm_module] ALTER {table}.{col_name}: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _seed_event_plan_questions(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM crm_event_plan_questions")
        n = cur.fetchone()[0]
        if n > 0:
            return
        seeds = [
            (
                "Crowd & setting",
                "Is the event primarily seated, standing, or mixed?",
                "text",
                10,
            ),
            (
                "Crowd & setting",
                "Approximate maximum crowd density expected in any single area?",
                "text",
                20,
            ),
            (
                "Environment",
                "Are significant water features or swimming present on site?",
                "yes_no",
                30,
            ),
            (
                "Environment",
                "Are crowd movement routes clearly defined and stewarded?",
                "yes_no",
                40,
            ),
            (
                "Planning",
                "Brief summary of medical escalation / handover arrangements?",
                "text",
                50,
            ),
        ]
        for group_name, qtext, atype, so in seeds:
            cur.execute(
                """INSERT INTO crm_event_plan_questions
                (group_name, question_text, answer_type, sort_order, is_active, help_url)
                VALUES (%s,%s,%s,%s,1,NULL)""",
                (group_name, qtext, atype, so),
            )
        conn.commit()
        print("[crm_module] Seeded default event plan checklist questions")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _migrate_crm_extensions(conn) -> None:
    _alter_add_column(conn, "crm_event_plans", "wizard_step TINYINT NOT NULL DEFAULT 1")
    _alter_add_column(conn, "crm_event_plans", "handoff_status VARCHAR(32) DEFAULT NULL")
    _alter_add_column(conn, "crm_event_plans", "handoff_external_ref VARCHAR(128) DEFAULT NULL")
    _alter_add_column(conn, "crm_event_plans", "handoff_at DATETIME DEFAULT NULL")
    _alter_add_column(conn, "crm_event_plans", "handoff_error TEXT DEFAULT NULL")
    _alter_add_column(conn, "crm_event_plans", "cura_operational_event_id BIGINT NULL")
    _alter_add_column(
        conn,
        "crm_event_plans",
        "clinical_handover_json JSON DEFAULT NULL",
    )
    _alter_add_column(conn, "crm_opportunities", "lead_meta_json JSON NULL")
    _alter_add_column(conn, "crm_quote_rules", "conditions_json JSON NULL")
    _alter_add_column(conn, "crm_quotes", "quote_group_id INT NULL")
    _alter_add_column(conn, "crm_quotes", "parent_quote_id INT NULL")
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_SCHEMA = DATABASE()
              AND TABLE_NAME = 'crm_quotes'
              AND CONSTRAINT_NAME = 'fk_crm_quote_parent'
            """
        )
        if cur.fetchone()[0] == 0:
            try:
                cur.execute(
                    """
                    ALTER TABLE crm_quotes
                    ADD CONSTRAINT fk_crm_quote_parent
                    FOREIGN KEY (parent_quote_id) REFERENCES crm_quotes(id) ON DELETE SET NULL
                    """
                )
                conn.commit()
                print("[crm_module] Added fk_crm_quote_parent")
            except Exception as e:
                if "Duplicate" not in str(e) and "exists" not in str(e).lower():
                    print(f"[crm_module] fk_crm_quote_parent: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass

    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE crm_quotes SET quote_group_id = id WHERE quote_group_id IS NULL"
        )
        conn.commit()
    except Exception as e:
        print(f"[crm_module] backfill quote_group_id: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass


def install():
    conn = get_db_connection()
    try:
        _create_table(
            conn,
            "crm_accounts",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            website VARCHAR(512) DEFAULT NULL,
            phone VARCHAR(64) DEFAULT NULL,
            notes TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_crm_accounts_name (name)
            """,
        )
        _create_table(
            conn,
            "crm_contacts",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            account_id INT DEFAULT NULL,
            first_name VARCHAR(128) NOT NULL DEFAULT '',
            last_name VARCHAR(128) NOT NULL DEFAULT '',
            email VARCHAR(255) DEFAULT NULL,
            phone VARCHAR(64) DEFAULT NULL,
            job_title VARCHAR(128) DEFAULT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_crm_contacts_account (account_id),
            KEY idx_crm_contacts_email (email(191)),
            CONSTRAINT fk_crm_contacts_account
                FOREIGN KEY (account_id) REFERENCES crm_accounts(id) ON DELETE SET NULL
            """,
        )
        _create_table(
            conn,
            "crm_opportunities",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            account_id INT NOT NULL,
            name VARCHAR(255) NOT NULL,
            stage ENUM(
                'prospecting','qualification','proposal','negotiation','won','lost'
            ) NOT NULL DEFAULT 'prospecting',
            amount DECIMAL(12,2) DEFAULT NULL,
            notes TEXT,
            lead_meta_json JSON NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_crm_opp_account (account_id),
            KEY idx_crm_opp_stage (stage),
            CONSTRAINT fk_crm_opp_account
                FOREIGN KEY (account_id) REFERENCES crm_accounts(id) ON DELETE CASCADE
            """,
        )
        _create_table(
            conn,
            "crm_opportunity_stage_history",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            opportunity_id INT NOT NULL,
            from_stage VARCHAR(32) DEFAULT NULL,
            to_stage VARCHAR(32) NOT NULL,
            changed_by VARCHAR(64) DEFAULT NULL,
            changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_crm_osh_opp (opportunity_id, changed_at),
            KEY idx_crm_osh_to (to_stage, changed_at),
            CONSTRAINT fk_crm_osh_opp
                FOREIGN KEY (opportunity_id) REFERENCES crm_opportunities(id) ON DELETE CASCADE
            """,
        )
        _create_table(
            conn,
            "crm_quote_rule_sets",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_crm_qrs_active (is_active)
            """,
        )
        _create_table(
            conn,
            "crm_quote_rules",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            rule_set_id INT NOT NULL,
            sort_order INT NOT NULL DEFAULT 0,
            rule_type ENUM(
                'fixed_add','per_head','per_hour','percent_surcharge','minimum_charge','vat'
            ) NOT NULL,
            amount DECIMAL(12,2) DEFAULT NULL,
            rate DECIMAL(12,4) DEFAULT NULL,
            percent DECIMAL(8,4) DEFAULT NULL,
            label VARCHAR(255) DEFAULT NULL,
            conditions_json JSON DEFAULT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_crm_qr_set_order (rule_set_id, sort_order),
            CONSTRAINT fk_crm_qr_rule_set
                FOREIGN KEY (rule_set_id) REFERENCES crm_quote_rule_sets(id) ON DELETE CASCADE
            """,
        )
        _create_table(
            conn,
            "crm_quotes",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255) NOT NULL DEFAULT '',
            account_id INT DEFAULT NULL,
            opportunity_id INT DEFAULT NULL,
            rule_set_id INT DEFAULT NULL,
            status ENUM('draft','sent','accepted','lost','rejected') NOT NULL DEFAULT 'draft',
            revision INT NOT NULL DEFAULT 1,
            quote_group_id INT DEFAULT NULL,
            parent_quote_id INT DEFAULT NULL,
            crowd_size INT DEFAULT NULL,
            duration_hours DECIMAL(8,2) DEFAULT NULL,
            internal_notes TEXT,
            total_amount DECIMAL(14,2) DEFAULT NULL,
            created_by VARCHAR(64) DEFAULT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_crm_quotes_status (status),
            KEY idx_crm_quotes_account (account_id),
            KEY idx_crm_quotes_group (quote_group_id),
            KEY idx_crm_quotes_parent (parent_quote_id),
            CONSTRAINT fk_crm_quote_account
                FOREIGN KEY (account_id) REFERENCES crm_accounts(id) ON DELETE SET NULL,
            CONSTRAINT fk_crm_quote_opp
                FOREIGN KEY (opportunity_id) REFERENCES crm_opportunities(id) ON DELETE SET NULL,
            CONSTRAINT fk_crm_quote_rs
                FOREIGN KEY (rule_set_id) REFERENCES crm_quote_rule_sets(id) ON DELETE SET NULL,
            CONSTRAINT fk_crm_quote_parent
                FOREIGN KEY (parent_quote_id) REFERENCES crm_quotes(id) ON DELETE SET NULL
            """,
        )
        _create_table(
            conn,
            "crm_quote_line_items",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            quote_id INT NOT NULL,
            sort_order INT NOT NULL DEFAULT 0,
            description VARCHAR(512) NOT NULL,
            quantity DECIMAL(12,4) NOT NULL DEFAULT 1.0000,
            unit_price DECIMAL(12,2) NOT NULL DEFAULT 0.00,
            KEY idx_crm_qli_quote (quote_id, sort_order),
            CONSTRAINT fk_crm_qli_quote
                FOREIGN KEY (quote_id) REFERENCES crm_quotes(id) ON DELETE CASCADE
            """,
        )
        _create_table(
            conn,
            "crm_quote_status_history",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            quote_id INT NOT NULL,
            previous_status VARCHAR(32) DEFAULT NULL,
            new_status VARCHAR(32) NOT NULL,
            changed_by VARCHAR(64) DEFAULT NULL,
            note VARCHAR(512) DEFAULT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_crm_qsh_quote (quote_id, created_at),
            CONSTRAINT fk_crm_qsh_quote
                FOREIGN KEY (quote_id) REFERENCES crm_quotes(id) ON DELETE CASCADE
            """,
        )
        _create_table(
            conn,
            "crm_activities",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            activity_type ENUM('call','meeting','email','task','note') NOT NULL DEFAULT 'note',
            subject VARCHAR(255) NOT NULL DEFAULT '',
            body TEXT,
            due_at DATETIME DEFAULT NULL,
            completed_at DATETIME DEFAULT NULL,
            account_id INT DEFAULT NULL,
            contact_id INT DEFAULT NULL,
            opportunity_id INT DEFAULT NULL,
            quote_id INT DEFAULT NULL,
            created_by VARCHAR(64) DEFAULT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_crm_act_account (account_id),
            KEY idx_crm_act_due (due_at),
            CONSTRAINT fk_crm_act_account
                FOREIGN KEY (account_id) REFERENCES crm_accounts(id) ON DELETE SET NULL,
            CONSTRAINT fk_crm_act_contact
                FOREIGN KEY (contact_id) REFERENCES crm_contacts(id) ON DELETE SET NULL,
            CONSTRAINT fk_crm_act_opp
                FOREIGN KEY (opportunity_id) REFERENCES crm_opportunities(id) ON DELETE SET NULL,
            CONSTRAINT fk_crm_act_quote
                FOREIGN KEY (quote_id) REFERENCES crm_quotes(id) ON DELETE SET NULL
            """,
        )
        _create_table(
            conn,
            "crm_event_plan_questions",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            group_name VARCHAR(128) NOT NULL DEFAULT '',
            question_text VARCHAR(512) NOT NULL,
            answer_type ENUM('yes_no','text','number') NOT NULL DEFAULT 'text',
            sort_order INT NOT NULL DEFAULT 0,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            help_url VARCHAR(512) DEFAULT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_crm_epq_sort (sort_order, id)
            """,
        )
        _create_table(
            conn,
            "crm_event_plan_question_audit",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            question_id INT NOT NULL,
            previous_text VARCHAR(512) NOT NULL,
            new_text VARCHAR(512) NOT NULL,
            changed_by VARCHAR(64) DEFAULT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_crm_epqa_q (question_id),
            CONSTRAINT fk_crm_epqa_q
                FOREIGN KEY (question_id) REFERENCES crm_event_plan_questions(id) ON DELETE CASCADE
            """,
        )
        _create_table(
            conn,
            "crm_event_plans",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            status ENUM('draft','completed','archived') NOT NULL DEFAULT 'draft',
            wizard_step TINYINT NOT NULL DEFAULT 1,
            quote_id INT DEFAULT NULL,
            opportunity_id INT DEFAULT NULL,
            account_id INT DEFAULT NULL,
            title VARCHAR(255) NOT NULL DEFAULT '',
            event_type VARCHAR(128) DEFAULT NULL,
            start_datetime DATETIME DEFAULT NULL,
            end_datetime DATETIME DEFAULT NULL,
            expected_attendance INT DEFAULT NULL,
            demographics_notes TEXT,
            environment_notes TEXT,
            address_line1 VARCHAR(255) DEFAULT NULL,
            postcode VARCHAR(32) DEFAULT NULL,
            what3words VARCHAR(64) DEFAULT NULL,
            location_notes TEXT,
            hospitals_notes TEXT,
            hospitals_json JSON DEFAULT NULL,
            clinical_handover_json JSON DEFAULT NULL,
            resources_medics VARCHAR(255) DEFAULT NULL,
            resources_vehicles TEXT,
            resources_comms TEXT,
            escalation_notes TEXT,
            risk_summary TEXT,
            risk_score INT DEFAULT NULL,
            signoff_name VARCHAR(255) DEFAULT NULL,
            signoff_role VARCHAR(255) DEFAULT NULL,
            signoff_at DATETIME DEFAULT NULL,
            handoff_status VARCHAR(32) DEFAULT NULL,
            handoff_external_ref VARCHAR(128) DEFAULT NULL,
            handoff_at DATETIME DEFAULT NULL,
            handoff_error TEXT DEFAULT NULL,
            cura_operational_event_id BIGINT NULL,
            checklist_answers_json JSON DEFAULT NULL,
            created_by VARCHAR(64) DEFAULT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_crm_ep_status (status),
            CONSTRAINT fk_crm_ep_quote
                FOREIGN KEY (quote_id) REFERENCES crm_quotes(id) ON DELETE SET NULL,
            CONSTRAINT fk_crm_ep_opp
                FOREIGN KEY (opportunity_id) REFERENCES crm_opportunities(id) ON DELETE SET NULL,
            CONSTRAINT fk_crm_ep_account
                FOREIGN KEY (account_id) REFERENCES crm_accounts(id) ON DELETE SET NULL
            """,
        )
        _create_table(
            conn,
            "crm_event_plan_pdfs",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            plan_id INT NOT NULL,
            file_path VARCHAR(512) NOT NULL,
            pdf_generated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            pdf_hash CHAR(64) DEFAULT NULL,
            CONSTRAINT fk_crm_epdf_plan
                FOREIGN KEY (plan_id) REFERENCES crm_event_plans(id) ON DELETE CASCADE,
            KEY idx_crm_epdf_plan (plan_id, pdf_generated_at)
            """,
        )
        _create_table(
            conn,
            "crm_guide_wage_job_map",
            """
            slot VARCHAR(32) NOT NULL PRIMARY KEY,
            job_type_id INT NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            """,
        )
        _create_table(
            conn,
            "crm_event_plan_handoff_log",
            """
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            plan_id INT NOT NULL,
            trigger_key VARCHAR(64) NOT NULL DEFAULT '',
            status VARCHAR(32) NOT NULL,
            detail TEXT,
            pdf_hash CHAR(64) DEFAULT NULL,
            external_ref VARCHAR(128) DEFAULT NULL,
            created_by VARCHAR(64) DEFAULT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_crm_ephh_plan (plan_id, created_at),
            CONSTRAINT fk_crm_ephh_plan
                FOREIGN KEY (plan_id) REFERENCES crm_event_plans(id) ON DELETE CASCADE
            """,
        )

        _migrate_crm_extensions(conn)
        _seed_event_plan_questions(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def upgrade():
    install()


if __name__ == "__main__":
    cmd = (sys.argv[1] if len(sys.argv) > 1 else "").strip().lower()
    if cmd == "upgrade":
        upgrade()
    else:
        install()
