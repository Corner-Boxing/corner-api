import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client, Client

# -------------------------
# Flask + CORS
# -------------------------
app = Flask(__name__)
CORS(app)

# -------------------------
# Supabase Client (SERVICE ROLE ONLY)
# -------------------------
# Render env vars:
#   SUPABASE_URL
#   SUPABASE_SERVICE_KEY
#
# NEVER hardcode them. NEVER ship these to Netlify/frontend.

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise RuntimeError(
        "Missing SUPABASE_URL or SUPABASE_SERVICE_KEY. "
        "Set both in Render Environment settings."
    )

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# -------------------------
# Helpers
# -------------------------
ALLOWED_DIFFICULTIES = {"beginner", "intermediate", "advanced"}
ALLOWED_PACES = {"Slow", "Normal", "Fast"}

def normalize_plan(payload: dict):
    difficulty = (payload.get("difficulty") or "beginner").strip().lower()
    if difficulty not in ALLOWED_DIFFICULTIES:
        difficulty = "beginner"

    # Frontend sends length as "length" (minutes). Worker expects "length_min".
    try:
        length_min = int(payload.get("length") or payload.get("length_min") or 30)
    except Exception:
        length_min = 30
    # keep sane bounds
    if length_min not in (20, 30, 45, 60):
        # allow others later, but keep predictable for now
        length_min = 30

    pace = str(payload.get("pace") or "Normal").strip()
    if pace not in ALLOWED_PACES:
        pace = "Normal"

    music = str(payload.get("music") or "none").strip()
    # normalize common variants
    if music.lower() in ("none", "no", "off", "coach only", "coach-only"):
        music = "none"

    # EXACT structure the worker expects
    worker_plan = {
        "difficulty": difficulty,
        "length_min": length_min,
        "pace": pace,
        "music": music,
    }

    # For product tracking table, store the same plan (keep it consistent)
    session_plan = dict(worker_plan)

    return worker_plan, session_plan

# -------------------------
# Routes
# -------------------------
@app.route("/")
def home():
    return "Corner API OK"

@app.route("/generate", methods=["POST"])
def generate():
    """
    Creates:
      1) a job in public.jobs for the local Corner Worker
      2) a matching row in public.class_sessions for future product tracking

    Returns: { status: "queued", job_id }
    """

    # ---- Safe JSON Parsing ----
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        return jsonify({"status": "error", "error": "Invalid JSON"}), 400

    worker_plan, session_plan = normalize_plan(data)

    job_id = None

    # ---- Insert job row (worker consumes this) ----
    try:
        job_res = (
            supabase
            .table("jobs")
            .insert({
                "status": "queued",
                "plan": worker_plan,
                "error": None,
                "file_url": None,
            })
            .execute()
        )

        if not job_res.data or not job_res.data[0].get("id"):
            return jsonify({"status": "error", "error": "Failed to create job row"}), 500

        job_id = str(job_res.data[0]["id"])

    except Exception as e:
        return jsonify({
            "status": "error",
            "error_message": str(e),
            "error_type": type(e).__name__,
        }), 500

    # ---- Insert class session row (product layer consumes this) ----
    # If this fails, we roll back by deleting the job row so you don’t create orphan jobs.
    try:
        sess_res = (
            supabase
            .table("class_sessions")
            .insert({
                "job_id": job_id,
                "status": "queued",
                "plan": session_plan,
                "file_url": None,
                "error": None,
                "started_at": None,
                "completed_at": None,
                # user_id stays null until auth is wired
            })
            .execute()
        )

        # sanity check
        if not sess_res.data:
            raise RuntimeError("class_sessions insert returned empty result")

    except Exception as e:
        # rollback job row to keep system consistent
        try:
            supabase.table("jobs").delete().eq("id", job_id).execute()
        except Exception:
            pass

        return jsonify({
            "status": "error",
            "error_message": f"class_sessions insert failed: {str(e)}",
            "error_type": type(e).__name__,
        }), 500

    return jsonify({"status": "queued", "job_id": job_id}), 202

# -------------------------
# Run for Render
# -------------------------
if __name__ == "__main__":
    # Render requires port 10000
    app.run(host="0.0.0.0", port=10000)
