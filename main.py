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
    methods=["GET", "POST", "OPTIONS"],
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
    """
    Creates:
      1) jobs row (status=queued)
      2) class_sessions row (status=queued, job_id=jobs.id, user_id if available)

    IMPORTANT:
      - Never crash on auth issues.
      - Always return JSON (even on internal errors).
    """
    try:
        # ---- Parse JSON body safely ----
        try:
            data = request.get_json(force=True, silent=True) or {}
        except Exception:
            return jsonify({"status": "error", "error": "Invalid JSON"}), 400

        plan = normalize_plan(data)

        # ---- Optional user id (no-verify decode is fine for attaching metadata) ----
        user_id = None
        try:
            auth = request.headers.get("Authorization") or ""
            if auth.lower().startswith("bearer "):
                token = auth.split(" ", 1)[1].strip()
                claims = jwt_claims(token)
                user_id = claims.get("sub")  # Supabase user id
        except Exception:
            user_id = None

        # ---- 1) Create job row ----
        job_res = (
            supabase
            .table("jobs")
            .insert({
                "status": "queued",
                "plan": plan,
                "error": None,
                "file_url": None,
                "storage_path": None,
            })
            .execute()
        )

        job_err = supa_err(job_res)
        if job_err or not getattr(job_res, "data", None):
            return jsonify({
                "status": "error",
                "error": "Failed to create job row",
                "details": job_err or "Unknown Supabase error",
            }), 500

        job_id = job_res.data[0].get("id")
        if not job_id:
            return jsonify({"status": "error", "error": "Job row missing id"}), 500

        # ---- 2) Create class_sessions row ----
        # right before insert:
        is_public = True if not user_id else False

        sess_payload = {
            "job_id": job_id,
            "user_id": user_id,
            "is_public": is_public,
            "status": "queued",
            "plan": plan,
            "difficulty": plan.get("difficulty"),
            "length_min": plan.get("length_min"),
            "pace": plan.get("pace"),
            "music": plan.get("music"),
            "file_url": None,
            "error": None,
            "started_at": None,
            "completed_at": None,
            "storage_path": None,
        }


        sess_res = supabase.table("class_sessions").insert(sess_payload).execute()
        sess_err = supa_err(sess_res)
        if sess_err:
            # rollback job row if session fails (best-effort)
            try:
                supabase.table("jobs").delete().eq("id", job_id).execute()
            except Exception:
                pass

            return jsonify({
                "status": "error",
                "error": "class_sessions insert failed",
                "details": sess_err,
                "job_id": str(job_id),
            }), 500

        return jsonify({
            "status": "queued",
            "job_id": str(job_id),
            "user_id": user_id,
        }), 202

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Internal server error",
            "details": str(e),
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
