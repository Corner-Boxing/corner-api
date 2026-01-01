import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client, Client

app = Flask(__name__)
CORS(app)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise RuntimeError(
        "Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in Render env vars."
    )

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

ALLOWED_DIFFICULTIES = {"beginner", "intermediate", "advanced"}
ALLOWED_PACES = {"Slow", "Normal", "Fast"}

def normalize_plan(payload: dict):
    difficulty = (payload.get("difficulty") or "beginner").strip().lower()
    if difficulty not in ALLOWED_DIFFICULTIES:
        difficulty = "beginner"

    try:
        length_min = int(payload.get("length") or payload.get("length_min") or 30)
    except Exception:
        length_min = 30
    if length_min not in (20, 30, 45, 60):
        length_min = 30

    pace = str(payload.get("pace") or "Normal").strip()
    if pace not in ALLOWED_PACES:
        pace = "Normal"

    music = str(payload.get("music") or "none").strip()
    if music.lower() in ("none", "no", "off", "coach only", "coach-only"):
        music = "none"

    worker_plan = {
        "difficulty": difficulty,
        "length_min": length_min,
        "pace": pace,
        "music": music,
    }
    return worker_plan

def supa_ok(resp):
    # supabase-py responses typically have: data, error
    err = getattr(resp, "error", None)
    if err:
        # err may be dict-like or an object
        msg = getattr(err, "message", None) or getattr(err, "msg", None) or str(err)
        return False, msg
    return True, None

@app.route("/")
def home():
    return "Corner API OK"

@app.route("/generate", methods=["POST"])
def generate():
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        return jsonify({"status": "error", "error": "Invalid JSON"}), 400

    plan = normalize_plan(data)

    # 1) Create job row
    job_res = (
        supabase
        .table("jobs")
        .insert({
            "status": "queued",
            "plan": plan,
            "error": None,
            "file_url": None,
        })
        .execute()
    )

    ok, err = supa_ok(job_res)
    if not ok or not job_res.data:
        return jsonify({
            "status": "error",
            "error": "Failed to create job row",
            "details": err or "Unknown Supabase error",
        }), 500

    job_id = job_res.data[0].get("id")
    if not job_id:
        return jsonify({"status": "error", "error": "Job row missing id"}), 500

    # 2) Create class_sessions row
    sess_res = (
        supabase
        .table("class_sessions")
        .insert({
            "job_id": job_id,
            "status": "queued",
            "plan": plan,
            "file_url": None,
            "error": None,
            "started_at": None,
            "completed_at": None,
            # user_id intentionally omitted until auth is wired
        })
        .execute()
    )

    ok, err = supa_ok(sess_res)
    if not ok:
        # rollback job row if session fails
        rb_res = supabase.table("jobs").delete().eq("id", job_id).execute()
        rb_ok, rb_err = supa_ok(rb_res)

        return jsonify({
            "status": "error",
            "error": "class_sessions insert failed",
            "details": err,
            "rollback": "ok" if rb_ok else "failed",
            "rollback_details": None if rb_ok else rb_err,
            "job_id": str(job_id),
        }), 500

    return jsonify({"status": "queued", "job_id": str(job_id)}), 202

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
