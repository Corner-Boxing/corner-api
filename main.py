import os
import time
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
GUEST_GENERATE_COOLDOWN_SECONDS = 15
_guest_last_generate_by_ip = {}


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

def get_client_ip():
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return (request.remote_addr or "").strip() or "unknown"

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

def get_active_job_for_user(user_id: str):
    """
    Returns one active job id for this signed-in user, or None.
    Active means one of:
      - queued
      - pending
      - processing
    """
    if not user_id:
        return None

    try:
        res = (
            supabase.table("class_sessions")
            .select("job_id,status")
            .eq("user_id", user_id)
            .in_("status", ["queued", "pending", "processing"])
            .limit(1)
            .execute()
        )

        if res.data and isinstance(res.data[0], dict):
            return res.data[0].get("job_id")

        return None
    except Exception:
        return None

def get_verified_user_id_from_request() -> str | None:
    """
    Verify token with Supabase Auth and return user id.
    If it fails, return None.
    """
    token = get_bearer_token()
    if not token:
        return None

    try:
        user_res = supabase.auth.get_user(token)

        user_obj = getattr(user_res, "user", None)
        if not user_obj and hasattr(user_res, "data"):
            user_obj = getattr(user_res.data, "user", None)

        if user_obj and getattr(user_obj, "id", None):
            return user_obj.id

        if isinstance(user_res, dict):
            u = user_res.get("user")
            if isinstance(u, dict):
                return u.get("id")

        return None
    except Exception:
        return None


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

@app.route("/me", methods=["GET"])
def me():
    uid = get_verified_user_id_from_request()
    if not uid:
        return jsonify({"signed_in": False}), 200

    prof = (
        supabase.table("profiles")
        .select("id,username,display_name,plan_tier")
        .eq("id", uid)
        .limit(1)
        .execute()
    )

    row = prof.data[0] if prof.data else None
    plan_tier = ((row.get("plan_tier") if row else None) or "free").strip().lower()

    return jsonify({
        "signed_in": True,
        "user_id": uid,
        "plan_tier": plan_tier,
        "profile": row,
    }), 200


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
        user_id = get_verified_user_id_from_request()

        client_ip = get_client_ip()

        if not user_id:
            now = time.time()
            last_at = _guest_last_generate_by_ip.get(client_ip, 0)
            if now - last_at < GUEST_GENERATE_COOLDOWN_SECONDS:
                remaining = int(GUEST_GENERATE_COOLDOWN_SECONDS - (now - last_at))
                return jsonify({
                    "status": "rate_limited",
                    "error": "Please wait a moment before generating again.",
                    "retry_after_seconds": max(1, remaining),
                }), 429

        # 2) Determine tier
        #    - Guest => demo
        #    - Logged in, not pro => demo
        #    - Pro => full
        plan_tier = get_plan_tier(user_id) if user_id else "free"
        is_pro = (plan_tier == "pro")

        # 2.5) Signed-in users may only have one active job at a time
        if user_id:
            active_job_id = get_active_job_for_user(user_id)
            if active_job_id:
                return jsonify({
                    "status": "conflict",
                    "error": "You already have a class generating.",
                    "active_job_id": str(active_job_id),
                }), 409

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

        if not user_id:
            _guest_last_generate_by_ip[client_ip] = time.time()

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


# -------------------------------------------------
# Home / Social API
# -------------------------------------------------

def require_user_id():
    uid = get_verified_user_id_from_request()
    if not uid:
        return None, (jsonify({
            "status": "error",
            "error": "Unauthorized",
        }), 401)
    return uid, None


def profile_display_name(profile: dict | None, fallback: str = "Corner athlete"):
    if not profile:
      return fallback

    display_name = (profile.get("display_name") or "").strip()
    username = (profile.get("username") or "").strip()

    if display_name:
        return display_name
    if username:
        return username
    return fallback


def normalize_profile(row: dict | None):
    row = row or {}
    username = (row.get("username") or "").strip()
    display_name = (row.get("display_name") or "").strip()

    return {
        "id": row.get("id"),
        "username": username,
        "display_name": display_name or username or "Corner athlete",
        "bio": row.get("bio") or "",
        "avatar_url": row.get("avatar_url") or "assets/avatars/default-avatar-head.png",
    }


def load_profiles_map(user_ids: list[str]):
    clean_ids = [str(x) for x in set(user_ids or []) if x]
    if not clean_ids:
        return {}

    try:
        res = (
            supabase.table("profiles")
            .select("id,username,display_name,bio,avatar_url")
            .in_("id", clean_ids)
            .execute()
        )
        return {str(row.get("id")): normalize_profile(row) for row in (res.data or []) if row.get("id")}
    except Exception:
        return {}


def load_following_ids(user_id: str):
    try:
        res = (
            supabase.table("follows")
            .select("following_id")
            .eq("follower_id", user_id)
            .execute()
        )
        return {str(row.get("following_id")) for row in (res.data or []) if row.get("following_id")}
    except Exception:
        return set()


def safe_session_post(row: dict, profiles_by_id: dict):
    author_id = str(row.get("user_id") or "")
    profile = profiles_by_id.get(author_id) or normalize_profile({"id": author_id})

    visibility = (row.get("visibility") or row.get("privacy") or "public")
    class_mode = row.get("class_mode") or "class"
    difficulty = row.get("difficulty") or ""
    length_min = row.get("length_min") or row.get("length") or ""
    music = row.get("music") or ""
    created_at = row.get("created_at")

    title = row.get("title") or "Completed a Corner class."
    body = row.get("body") or row.get("caption") or "Finished a verified boxing session."

    tags = []
    if class_mode:
        tags.append(str(class_mode).replace("_", " ").title())
    if difficulty:
        tags.append(str(difficulty).title())
    if length_min:
        tags.append(f"{length_min} min")
    if visibility:
        tags.append(str(visibility).replace("_", "-"))

    return {
        "id": row.get("id"),
        "type": "session_post",
        "author": profile,
        "title": title,
        "body": body,
        "created_at": created_at,
        "tags": tags[:4],
        "visibility": visibility,
        "class_session_id": row.get("class_session_id"),
        "job_id": row.get("job_id"),
        "music": music,
    }


@app.route("/home/feed", methods=["GET"])
def home_feed():
    uid, err = require_user_id()
    if err:
        return err

    try:
        following_ids = load_following_ids(uid)
        allowed_user_ids = set(following_ids)
        allowed_user_ids.add(uid)

        posts_res = (
            supabase.table("session_posts")
            .select("*")
            .order("created_at", desc=True)
            .limit(60)
            .execute()
        )

        raw_posts = posts_res.data or []

        visible_posts = []
        author_ids = []

        for row in raw_posts:
            author_id = str(row.get("user_id") or "")
            visibility = str(row.get("visibility") or row.get("privacy") or "public").lower()

            can_see = False

            if author_id == uid:
                can_see = True
            elif visibility == "public":
                can_see = True
            elif visibility in ("friends", "friends_only", "followers", "followers_only") and author_id in allowed_user_ids:
                can_see = True

            if can_see:
                visible_posts.append(row)
                if author_id:
                    author_ids.append(author_id)

            if len(visible_posts) >= 30:
                break

        profiles_by_id = load_profiles_map(author_ids)

        return jsonify({
            "status": "ok",
            "items": [safe_session_post(row, profiles_by_id) for row in visible_posts],
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load home feed.",
            "details": str(e),
        }), 500


@app.route("/home/notifications", methods=["GET"])
def home_notifications():
    uid, err = require_user_id()
    if err:
        return err

    try:
        res = (
            supabase.table("notifications")
            .select("*")
            .eq("user_id", uid)
            .order("created_at", desc=True)
            .limit(40)
            .execute()
        )

        rows = res.data or []

        actor_ids = [str(row.get("actor_id")) for row in rows if row.get("actor_id")]
        profiles_by_id = load_profiles_map(actor_ids)

        items = []
        for row in rows:
            actor_id = str(row.get("actor_id") or "")
            actor = profiles_by_id.get(actor_id)

            items.append({
                "id": row.get("id"),
                "type": row.get("type") or "notification",
                "title": row.get("title") or "Notification",
                "body": row.get("body") or row.get("message") or "",
                "read": bool(row.get("read") or row.get("is_read") or False),
                "created_at": row.get("created_at"),
                "actor": actor,
                "entity_type": row.get("entity_type"),
                "entity_id": row.get("entity_id"),
            })

        return jsonify({
            "status": "ok",
            "items": items,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load notifications.",
            "details": str(e),
        }), 500


@app.route("/home/suggestions", methods=["GET"])
def home_suggestions():
    uid, err = require_user_id()
    if err:
        return err

    try:
        following_ids = load_following_ids(uid)

        res = (
            supabase.table("profiles")
            .select("id,username,display_name,bio,avatar_url")
            .neq("id", uid)
            .limit(60)
            .execute()
        )

        following_items = []
        suggestion_items = []

        for row in (res.data or []):
            profile_id = str(row.get("id") or "")
            if not profile_id:
                continue

            already_following = profile_id in following_ids

            item = {
                "profile": normalize_profile(row),
                "reason": "Following" if already_following else "Corner athlete",
                "already_following": already_following,
            }

            if already_following:
                following_items.append(item)
            else:
                suggestion_items.append(item)

        items = following_items[:12] + suggestion_items[:12]

        return jsonify({
            "status": "ok",
            "items": items[:18],
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load suggestions.",
            "details": str(e),
        }), 500


@app.route("/follow/<target_user_id>", methods=["POST", "DELETE"])
def follow_user(target_user_id):
    uid, err = require_user_id()
    if err:
        return err

    target_user_id = str(target_user_id or "").strip()

    if not target_user_id:
        return jsonify({
            "status": "error",
            "error": "Missing target user id.",
        }), 400

    if target_user_id == uid:
        return jsonify({
            "status": "error",
            "error": "You cannot follow yourself.",
        }), 400

    try:
        if request.method == "DELETE":
            supabase.table("follows").delete().eq("follower_id", uid).eq("following_id", target_user_id).execute()
            return jsonify({
                "status": "ok",
                "following": False,
                "target_user_id": target_user_id,
            }), 200

        existing = (
            supabase.table("follows")
            .select("follower_id,following_id")
            .eq("follower_id", uid)
            .eq("following_id", target_user_id)
            .limit(1)
            .execute()
        )

        if not existing.data:
            insert_res = supabase.table("follows").insert({
                "follower_id": uid,
                "following_id": target_user_id,
            }).execute()

            insert_err = supa_err(insert_res)
            if insert_err:
                return jsonify({
                    "status": "error",
                    "error": "Follow insert failed.",
                    "details": insert_err,
                }), 500

        return jsonify({
            "status": "ok",
            "following": True,
            "target_user_id": target_user_id,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not update follow state.",
            "details": str(e),
        }), 500


@app.route("/profile/<profile_id>", methods=["GET"])
def read_profile(profile_id):
    uid = get_verified_user_id_from_request()
    profile_id = str(profile_id or "").strip()

    try:
        res = (
            supabase.table("profiles")
            .select("id,username,display_name,bio,avatar_url,created_at,plan_tier")
            .eq("id", profile_id)
            .limit(1)
            .execute()
        )

        if not res.data:
            return jsonify({
                "status": "error",
                "error": "Profile not found.",
            }), 404

        following = False
        if uid and uid != profile_id:
            following_ids = load_following_ids(uid)
            following = profile_id in following_ids

        return jsonify({
            "status": "ok",
            "profile": normalize_profile(res.data[0]),
            "following": following,
            "is_self": bool(uid and uid == profile_id),
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load profile.",
            "details": str(e),
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
