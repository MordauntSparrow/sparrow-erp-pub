"""
Build SNOMED CT condition lists for Cura (UK NHS ambulance / emergency care profile).

Uses Snowstorm's ``/snowstorm/snomed-ct/{branch}/concepts`` endpoint (not ``.../browser/...``),
which supports ECL + ``searchAfter`` pagination on public training instances.

Configure via environment (optional):
  SNOWSTORM_BASE_URL — default https://snowstorm-training.snomedtools.org
  SNOWSTORM_BRANCH — default MAIN
  SNOWSTORM_MAX_CONCEPTS — default 5000 (hard cap on merged list)
  SNOWSTORM_ECL_CHUNKS — optional newline-separated ECL list; if unset, common disorder seeds
    (mental health, cardiac, respiratory, endocrine, etc.) are fetched first, then broad ``<<`` body-system slices.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger("medical_records_module.cura_snomed")

# High-use disorders crews actually name (one SNOMED concept per line, ``conceptId |FSN|``).
# Fetched before the large ``<<`` body-system ECLs so they are never displaced by per-chunk caps.
_COMMON_AMBULANCE_DISORDER_SEED_ECLS: tuple[str, ...] = (
    # --- Mental health (common) ---
    "354890007 |Depressive disorder (disorder)|",
    "48694002 |Anxiety disorder (disorder)|",
    "231504006 |Mixed anxiety and depressive disorder (disorder)|",
    "371631005 |Panic disorder (disorder)|",
    "58214004 |Schizophrenia (disorder)|",
    "13746004 |Bipolar disorder (disorder)|",
    "47505003 |Posttraumatic stress disorder (disorder)|",
    "33449004 |Personality disorder (disorder)|",
    "72366004 |Eating disorder (disorder)|",
    "406506008 |Attention deficit hyperactivity disorder (disorder)|",
    # --- Cardiovascular (common) ---
    "59621000 |Essential hypertension (disorder)|",
    "38341003 |Hypertensive disorder, systemic arterial (disorder)|",
    "414545008 |Ischemic heart disease (disorder)|",
    "53741008 |Coronary arteriosclerosis (disorder)|",
    "22298006 |Myocardial infarction (disorder)|",
    "84114007 |Heart failure (disorder)|",
    "42343007 |Congestive heart failure (disorder)|",
    "49436004 |Atrial fibrillation (disorder)|",
    "44103008 |Ventricular arrhythmia (disorder)|",
    "55827005 |Left ventricular hypertrophy (disorder)|",
    "400047006 |Peripheral vascular disease (disorder)|",
    # --- Respiratory (common) ---
    "195967001 |Asthma (disorder)|",
    "13645005 |Chronic obstructive lung disease (disorder)|",
    "87433001 |Pulmonary emphysema (disorder)|",
    "59282003 |Pulmonary embolism (disorder)|",
    "233604007 |Pneumonia (disorder)|",
    "53084003 |Bacterial pneumonia (disorder)|",
    "275498002 |Respiratory tract infection (disorder)|",
    "10509002 |Acute bronchitis (disorder)|",
    # --- Endocrine & metabolic (common) ---
    "44054006 |Diabetes mellitus type 2 (disorder)|",
    "46635009 |Diabetes mellitus type 1 (disorder)|",
    "73211009 |Diabetes mellitus (disorder)|",
    "414916001 |Obesity (disorder)|",
    "34486009 |Hyperthyroidism (disorder)|",
    "40930008 |Hypothyroidism (disorder)|",
    # --- Renal & urogenital (common) ---
    "52448006 |Chronic kidney disease (disorder)|",
    "46177005 |End-stage renal disease (disorder)|",
    "95570007 |Kidney stone (disorder)|",
    "68566005 |Urinary tract infectious disease (disorder)|",
    "266569009 |Benign prostatic hyperplasia (disorder)|",
    # --- Gastrointestinal & hepatic (common) ---
    "19943007 |Cirrhosis of liver (disorder)|",
    "235595009 |Gastroesophageal reflux disease (disorder)|",
    "10743008 |Irritable bowel syndrome (disorder)|",
    "64766004 |Ulcerative colitis (disorder)|",
    "34000006 |Crohn's disease (disorder)|",
    "266474003 |Calculus in biliary tract (disorder)|",
    "75694006 |Pancreatitis (disorder)|",
    # --- Neurological (common) ---
    "230690007 |Cerebrovascular accident (disorder)|",
    "266257000 |Transient ischemic attack (disorder)|",
    "84757009 |Epilepsy (disorder)|",
    "26929004 |Alzheimer's disease (disorder)|",
    "24700007 |Multiple sclerosis (disorder)|",
    "49049000 |Parkinson's disease (disorder)|",
    # --- Musculoskeletal (common) ---
    "396275006 |Osteoarthritis (disorder)|",
    "69896004 |Rheumatoid arthritis (disorder)|",
    "64859006 |Osteoporosis (disorder)|",
    "128053003 |Deep venous thrombosis (disorder)|",
    # --- Skin, allergy, soft tissue (common) ---
    "128045006 |Cellulitis (disorder)|",
    "43116000 |Eczema (disorder)|",
    "9014002 |Psoriasis (disorder)|",
    "446096008 |Perennial allergic rhinitis (disorder)|",
    # --- Haematologic (common) ---
    "271737000 |Anemia (disorder)|",
    # --- Oncology (frequently seen) ---
    "254837009 |Malignant neoplasm of breast (disorder)|",
    # --- General, women’s health, sleep ---
    "77386006 |Pregnancy (disorder)|",
    "78275009 |Obstructive sleep apnea syndrome (disorder)|",
    "23986001 |Glaucoma (disorder)|",
    # --- Substance use (common) ---
    "7200002 |Alcoholism (disorder)|",
    "6525002 |Dependent drug abuse (disorder)|",
)

# Human clinical disorder slices for ambulance / emergency pickers (International SNOMED edition).
# Each ECL is queried separately via Snowstorm and merged (deduped). IDs verified against Snowstorm MAIN.
DEFAULT_UK_AMBULANCE_BODY_SYSTEM_ECL_CHUNKS: tuple[str, ...] = (
    "<<118654009 |Disorder of respiratory system (disorder)|",
    "<<19829001 |Disorder of cardiovascular system (disorder)|",
    "<<928000 |Disorder of musculoskeletal system (disorder)|",
    "<<118599009 |Disorder of nervous system (disorder)|",
    "<<53619000 |Disorder of digestive system (disorder)|",
    "<<362969004 |Disorder of endocrine system (disorder)|",
    "<<95320005 |Disorder of skin (disorder)|",
    "<<371405004 |Disorder of eye (disorder)|",
    "<<118938001 |Disorder of ear (disorder)|",
    "<<89488007 |Disorder of the nose (disorder)|",
    "<<128606002 |Disorder of the urinary system (disorder)|",
    "<<40733004 |Infectious disease (disorder)|",
    "<<363346000 |Malignant neoplastic disease (disorder)|",
    "<<74732009 |Mental disorder (disorder)|",
    "<<417746004 |Traumatic injury (disorder)|",
    "<<125605004 |Fracture of bone (disorder)|",
)


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def _snowstorm_base_url() -> str:
    return (os.environ.get("SNOWSTORM_BASE_URL") or "https://snowstorm-training.snomedtools.org").rstrip(
        "/"
    )


def _snowstorm_branch() -> str:
    return (os.environ.get("SNOWSTORM_BRANCH") or "MAIN").strip() or "MAIN"


def _max_concepts() -> int:
    return min(50_000, _env_int("SNOWSTORM_MAX_CONCEPTS", 5000))


def _page_limit() -> int:
    return min(500, _env_int("SNOWSTORM_PAGE_LIMIT", 200))


def _fetch_one_ecl(
    session: requests.Session,
    base: str,
    branch: str,
    ecl: str,
    *,
    max_for_this_ecl: int,
    timeout: int = 120,
) -> list[dict[str, Any]]:
    """Return normalised concept rows for one ECL (active concepts only)."""
    url = f"{base}/snowstorm/snomed-ct/{branch}/concepts"
    out: list[dict[str, Any]] = []
    params: dict[str, Any] = {
        "ecl": ecl.strip(),
        "limit": _page_limit(),
        "activeFilter": "true",
    }
    search_after: str | None = None
    while len(out) < max_for_this_ecl:
        if search_after:
            params["searchAfter"] = search_after
        try:
            r = session.get(url, params=params, timeout=timeout)
        except requests.RequestException as ex:
            logger.warning("SNOMED fetch network error: %s", ex)
            break
        if r.status_code != 200:
            logger.warning("SNOMED fetch HTTP %s: %s", r.status_code, r.text[:500])
            break
        try:
            data = r.json()
        except json.JSONDecodeError:
            logger.warning("SNOMED fetch invalid JSON")
            break
        items = data.get("items") if isinstance(data, dict) else None
        if not items:
            break
        for it in items:
            if not isinstance(it, dict):
                continue
            if not it.get("active"):
                continue
            cid = str(it.get("conceptId") or it.get("id") or "").strip()
            if not cid:
                continue
            pt = it.get("pt") if isinstance(it.get("pt"), dict) else {}
            fsn = it.get("fsn") if isinstance(it.get("fsn"), dict) else {}
            term = (pt.get("term") or fsn.get("term") or "").strip()
            if not term:
                continue
            out.append(
                {
                    "conceptId": cid,
                    "pt": term,
                    "fsn": (fsn.get("term") or "").strip() or None,
                }
            )
            if len(out) >= max_for_this_ecl:
                break
        sa = data.get("searchAfter") if isinstance(data, dict) else None
        if not sa or not items:
            break
        search_after = str(sa)
    return out


def fetch_uk_ambulance_snomed_payload() -> dict[str, Any]:
    """
    Pull SNOMED concepts from Snowstorm and build the dataset payload.

    Returns a dict: { schemaVersion, profile, generatedAt, source, concepts: [...] }
    Raises RuntimeError on total failure (no concepts).
    """
    base = _snowstorm_base_url()
    branch = _snowstorm_branch()
    cap = _max_concepts()
    raw_env = (os.environ.get("SNOWSTORM_ECL_CHUNKS") or "").strip()
    use_custom_chunks = bool(raw_env)

    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    by_id: dict[str, dict[str, Any]] = {}
    chunk_count_for_meta = 0

    if use_custom_chunks:
        parts = [p.strip() for p in raw_env.splitlines() if p.strip()]
        chunks = tuple(parts)
        chunk_count_for_meta = len(chunks)
        per_chunk = max(50, cap // max(1, len(chunks)))
        for ecl in chunks:
            if len(by_id) >= cap:
                break
            remaining = cap - len(by_id)
            take = min(per_chunk, remaining)
            rows = _fetch_one_ecl(session, base, branch, ecl, max_for_this_ecl=take)
            for row in rows:
                cid = row["conceptId"]
                if cid not in by_id:
                    by_id[cid] = row
                if len(by_id) >= cap:
                    break
    else:
        # Seeds first (each ECL is one concept); body-system ``<<`` slices share the *remaining* cap
        # so a long seed list does not shrink every subtree’s page budget.
        for ecl in _COMMON_AMBULANCE_DISORDER_SEED_ECLS:
            if len(by_id) >= cap:
                break
            remaining = cap - len(by_id)
            rows = _fetch_one_ecl(
                session, base, branch, ecl, max_for_this_ecl=max(1, min(20, remaining))
            )
            for row in rows:
                cid = row["conceptId"]
                if cid not in by_id:
                    by_id[cid] = row
                if len(by_id) >= cap:
                    break

        body_chunks = DEFAULT_UK_AMBULANCE_BODY_SYSTEM_ECL_CHUNKS
        chunk_count_for_meta = len(_COMMON_AMBULANCE_DISORDER_SEED_ECLS) + len(body_chunks)
        per_chunk = max(50, (cap - len(by_id)) // max(1, len(body_chunks)))
        for ecl in body_chunks:
            if len(by_id) >= cap:
                break
            remaining = cap - len(by_id)
            take = min(per_chunk, remaining)
            rows = _fetch_one_ecl(session, base, branch, ecl, max_for_this_ecl=take)
            for row in rows:
                cid = row["conceptId"]
                if cid not in by_id:
                    by_id[cid] = row
                if len(by_id) >= cap:
                    break

    if not by_id:
        raise RuntimeError(
            "No SNOMED concepts retrieved. Check SNOWSTORM_BASE_URL / network, or use JSON upload in Cura settings."
        )

    concepts = sorted(by_id.values(), key=lambda x: (x.get("pt") or "").lower())
    return {
        "schemaVersion": 1,
        "profile": "uk_nhs_ambulance_emergency",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "source": {
            "kind": "snowstorm_rest",
            "baseUrl": base,
            "branch": branch,
            "eclChunks": chunk_count_for_meta,
            "maxConcepts": cap,
        },
        "concepts": concepts,
    }
