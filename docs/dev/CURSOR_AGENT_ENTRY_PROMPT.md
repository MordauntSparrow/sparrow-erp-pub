# Cursor agent — entry prompt (copy below)

Paste into a **new chat** in the correct workspace (Sparrow ERP, Cura, or Ventus MDT). Fill in the two blanks.

---

```text
agent_id: <e.g. sparrow_crm | cura_clinical | sparrow_inventory — see AGENT_PRD_INDEX.md>

Task: <one sentence: what to implement or fix>

Instructions:
1. Open and follow docs/dev/AGENT_PRD_INDEX.md.
2. Find the row for my agent_id. Read every document listed in read_order, in order.
3. If the human gave extra IDs or phase (e.g. “CRM-HOSP-001 only”, “W0 hotfix”), stay within that scope.
4. Match existing code patterns in this repo (permissions, CSRF, install.py). Do not expand scope beyond the PRDs + task.
```

---

**Tip:** In Cursor, `@docs/dev/AGENT_PRD_INDEX.md` in the same message so the agent loads the index immediately.
