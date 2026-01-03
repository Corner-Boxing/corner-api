import os
import base64
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client, Client

app = Flask(__name__)

# IMPORTANT:
# We must allow Authorization header for Bearer tokens from the frontend.
CORS(
    app,
    resources={r"/*": {"origins": "*"}},
    allow_headers=["Content-Type", "Authorization"],
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in Render env vars.")

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

    return {
        "difficulty": difficulty,
        "length_min": length_min,
        "pace": pace,
        "music": music,
    }

def supa_err(resp):
    # supabase-py responses typically have: data, error
    err = getattr(resp, "error", None)
    if not err:
        return None
    # err may be dict-like or an object
    msg = getattr(err, "message", None) or getattr(err, "msg", None)
    return msg or str(err)

def jwt_claims(jwt: str):
    """
    SAFE: decode payload WITHOUT verifying signature, and return non-sensitive claims.
    We do NOT return the token or any secret, just decoded claims like 'role'.
    """
    try:
        parts = jwt.split(".")
        if len(parts) < 2:
            return {}
        payload_b64 = parts[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)  # padding
        payload = base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8")
        return json.loads(payload)
    except Exception:
        return {}

def get_user_id_from_request():
    """
    If the client sends Authorization: Bearer <JWT>,
    validate it via Supabase and return user.id.

    If missing/invalid => None (anon allowed).
    """
    auth_header = request.headers.get("Authorization") or ""
    if not auth_header.lower().startswith("bearer "):
        return None

    token = auth_header.split(" ", 1)[1].strip()
    if not token:
        return None

    try:
        user_resp = supabase.auth.get_user(token)

        # supabase-py shape can vary; handle common shapes
        if isinstance(user_resp, dict):
            user = user_resp.get("user")
        else:
            user = getattr(user_resp, "user", None)

        if not user:
            return None

        if isinstance(user, dict):
            return user.get("id")
        return getattr(user, "id", None)
    except Exception:
        return None

@app.route("/")
def home():
    return "Corner API OK"

@app.route("/_whoami")
def whoami():
    claims = jwt_claims(SUPABASE_SERVICE_KEY)
    return jsonify({
        "supabase_url_set": bool(SUPABASE_URL),
        "key_claims": {
            "role": claims.get("role"),
            "ref": claims.get("ref"),
            "iat": claims.get("iat"),
            "exp": claims.get("exp"),
        }
    })

@app.route("/generate", methods=["POST"])
def generate():
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        return jsonify({"status": "error", "error": "Invalid JSON"}), 400

    plan = normalize_plan(data)

    # optional (logged-in) user association
    user_id = get_user_id_from_request()

    # 1) Create job row
    job_payload = {
        "status": "queued",
        "plan": plan,
        "error": None,
        "file_url": None,
    }

    # If your jobs table has user_id, attach it when present.
    # If the column doesn't exist, Supabase will error—so we add it only when we actually have a user.
    if user_id:
        job_payload["user_id"] = user_id

    job_res = supabase.table("jobs").insert(job_payload).execute()

    job_err = supa_err(job_res)
    if job_err or not job_res.data:
        return jsonify({
            "status": "error",
            "error": "Failed to create job row",
            "details": job_err or "Unknown Supabase error",
        }), 500

    job_id = job_res.data[0].get("id")
    if not job_id:
        return jsonify({"status": "error", "error": "Job row missing id"}), 500

    # 2) Create class_sessions row (KEEPING your current schema shape)
    sess_payload = {
        "job_id": job_id,
        "status": "queued",
        "plan": plan,
        "file_url": None,
        "error": None,
        "started_at": None,
        "completed_at": None,
    }

    # If your class_sessions table has user_id, attach it when present.
    if user_id:
        sess_payload["user_id"] = user_id

    sess_res = supabase.table("class_sessions").insert(sess_payload).execute()

    sess_err = supa_err(sess_res)
    if sess_err:
        # rollback job row if session fails (best-effort)
        supabase.table("jobs").delete().eq("id", job_id).execute()

        return jsonify({
            "status": "error",
            "error": "class_sessions insert failed",
            "details": sess_err,
            "job_id": str(job_id),
        }), 500

    # Return user_id for debugging (null for anon)
    return jsonify({
        "status": "queued",
        "job_id": str(job_id),
        "user_id": user_id,
    }), 202

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
