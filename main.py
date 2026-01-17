import os
import base64
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client, Client

app = Flask(__name__)

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

# Demo rules (your spec)
DEMO_DIFFICULTY = "intermediate"   # locked
DEMO_LENGTH_MIN = 20              # locked
DEMO_MUSIC = "demo"               # locked (we'll add audio later; for now it's just a label)
DEMO_PACE = "Normal"              # keep simple + consistent


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
    err = getattr(resp, "error", None)
    if not err:
        return None
    msg = getattr(err, "message", None) or getattr(err, "msg", None)
    return msg or str(err)


def jwt_claims_no_verify(jwt: str):
    # only used for debug-ish reads; not for auth decisions
    try:
        parts = jwt.split(".")
        if len(parts) < 2:
            return {}
        payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8")
        return json.loads(payload)
    except Exception:
        return {}


def get_bearer_token():
    auth = request.headers.get("Authorization") or ""
    if not auth.lower().startswith("bearer "):
        return None
    token = auth.split(" ", 1)[1].strip()
    return token or None


def get_verified_user_id():
    """
    IMPORTANT: Verify token with Supabase Auth (don’t trust decode-only for tier decisions).
    If it fails, treat as guest.
    """
    token = get_bearer_token()
    if not token:
        return None

    try:
        user_res = supabase.auth.get_user(token)

        # handle common shapes
        user_obj = getattr(user_res, "user", None)
        if not user_obj and hasattr(user_res, "data"):
            user_obj = getattr(user_res.data, "user", None)

        if user_obj and getattr(user_obj, "id", None):
            return user_obj.id

        # sometimes dict-like
        if isinstance(user_res, dict):
            u = user_res.get("user")
            if isinstance(u, dict):
                return u.get("id")

        return None
    except Exception:
        return None


def get_plan_tier(user_id: str):
    """
    Read profiles.plan_tier for the user.
    Defaults to 'free' if missing.
    """
    if not user_id:
        return "free"
    try:
        res = supabase.table("profiles").select("plan_tier").eq("id", user_id).limit(1).execute()
        if res.data and isinstance(res.data[0], dict):
            return (res.data[0].get("plan_tier") or "free").strip().lower()
        return "free"
    except Exception:
        return "free"


@app.route("/")
def home():
    return "Corner API OK"


@app.route("/_whoami")
def whoami():
    claims = jwt_claims_no_verify(SUPABASE_SERVICE_KEY)
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
    Also sets:
      - class_mode: 'demo' or 'full'
      - demo mode forces: intermediate + 20min + demo music + normal pace
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
        requested_plan = normalize_plan(data)

        # 1) Verify user (or guest)
        user_id = get_verified_user_id()

        # 2) Determine tier
        #    - Guest => demo
        #    - Logged in, not pro => demo
        #    - Pro => full
        plan_tier = get_plan_tier(user_id) if user_id else "free"
        is_pro = (plan_tier == "pro")

        class_mode = "full" if is_pro else "demo"

        # 3) Final plan (forced if demo)
        if class_mode == "demo":
            final_plan = {
                "difficulty": DEMO_DIFFICULTY,
                "length_min": DEMO_LENGTH_MIN,
                "pace": DEMO_PACE,
                "music": DEMO_MUSIC,
            }
        else:
            final_plan = requested_plan

        # 4) Create job row
        job_res = (
            supabase
            .table("jobs")
            .insert({
                "status": "queued",
                "plan": final_plan,
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

        # 5) Create class_sessions row
        # Guests: public demo sessions (so they can still poll status + play)
        is_public = True if not user_id else False

        sess_payload = {
            "job_id": job_id,
            "user_id": user_id,
            "is_public": is_public,

            "class_mode": class_mode,   # <-- NEW COLUMN
            "status": "queued",
            "plan": final_plan,

            "difficulty": final_plan.get("difficulty"),
            "length_min": final_plan.get("length_min"),
            "pace": final_plan.get("pace"),
            "music": final_plan.get("music"),

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
            "class_mode": class_mode,
            "plan_tier": plan_tier if user_id else "guest",
        }), 202

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Internal server error",
            "details": str(e),
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
