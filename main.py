import os
import time
import base64
import json
import re
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client, Client

app = Flask(__name__)

CORS(
    app,
    resources={r"/*": {"origins": "*"}},
    allow_headers=["Content-Type", "Authorization"],
    methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
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

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def clean_username(value: str | None):
    raw = str(value or "").strip().lower()
    raw = raw.replace(" ", "_")
    raw = re.sub(r"[^a-z0-9_]", "", raw)
    raw = re.sub(r"_+", "_", raw).strip("_")
    return raw


def username_is_valid(username: str):
    if not username:
        return False
    if len(username) < 3 or len(username) > 24:
        return False
    return bool(re.fullmatch(r"[a-z0-9_]+", username))


def profile_is_complete(profile: dict | None):
    if not profile:
        return False
    return bool((profile.get("username") or "").strip() and (profile.get("display_name") or "").strip())


def ensure_profile_row(user_id: str):
    if not user_id:
        return None

    res = (
        supabase.table("profiles")
        .select("id,username,display_name,bio,avatar_url,plan_tier,subscription_status,tier_updated_at,created_at,updated_at")
        .eq("id", user_id)
        .limit(1)
        .execute()
    )

    if res.data:
        return res.data[0]

    payload = {
        "id": user_id,
        "username": None,
        "display_name": None,
        "bio": "",
        "avatar_url": "assets/avatars/default-avatar-head.png",
        "plan_tier": "free",
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
    }

    insert_res = supabase.table("profiles").insert(payload).execute()
    if insert_res.data:
        return insert_res.data[0]

    # Race-condition fallback: if another request created it first, read again.
    retry = (
        supabase.table("profiles")
        .select("id,username,display_name,bio,avatar_url,plan_tier,subscription_status,tier_updated_at,created_at,updated_at")
        .eq("id", user_id)
        .limit(1)
        .execute()
    )
    return retry.data[0] if retry.data else None


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

    row = ensure_profile_row(uid)
    plan_tier = ((row.get("plan_tier") if row else None) or "free").strip().lower()

    return jsonify({
        "signed_in": True,
        "user_id": uid,
        "plan_tier": plan_tier,
        "profile": profile_payload(row, uid),
        "profile_complete": profile_is_complete(row),
    }), 200

@app.route("/me/profile", methods=["PATCH"])
def update_my_profile():
    uid, err = require_user_id()
    if err:
        return err

    data = request.get_json(force=True, silent=True) or {}

    display_name = str(data.get("display_name") or "").strip()
    username = clean_username(data.get("username"))
    bio = str(data.get("bio") or "").strip()
    avatar_url = str(data.get("avatar_url") or "").strip()

    if not display_name or len(display_name) > 40:
        return jsonify({
            "status": "error",
            "error": "Display name must be 1-40 characters.",
        }), 400

    if not username_is_valid(username):
        return jsonify({
            "status": "error",
            "error": "Username must be 3-24 characters and use only letters, numbers, or underscores.",
        }), 400

    if len(bio) > 160:
        return jsonify({
            "status": "error",
            "error": "Bio must be 160 characters or less.",
        }), 400

    if avatar_url:
        allowed_avatar = (
            avatar_url.startswith("assets/avatars/")
            or avatar_url.startswith("https://")
            or avatar_url.startswith("http://")
        )
        if not allowed_avatar or len(avatar_url) > 500:
            return jsonify({
                "status": "error",
                "error": "Avatar URL is not allowed.",
            }), 400
    else:
        avatar_url = "assets/avatars/default-avatar-head.png"

    existing_username = (
        supabase.table("profiles")
        .select("id,username")
        .eq("username", username)
        .limit(1)
        .execute()
    )

    if existing_username.data:
        owner_id = str(existing_username.data[0].get("id") or "")
        if owner_id and owner_id != uid:
            return jsonify({
                "status": "error",
                "error": "That username is already taken.",
            }), 409

    ensure_profile_row(uid)

    payload = {
        "id": uid,
        "username": username,
        "display_name": display_name,
        "bio": bio,
        "avatar_url": avatar_url,
        "updated_at": utc_now_iso(),
    }

    res = supabase.table("profiles").upsert(payload).execute()
    err_msg = supa_err(res)

    if err_msg:
        return jsonify({
            "status": "error",
            "error": "Profile update failed.",
            "details": err_msg,
        }), 500

    saved = ensure_profile_row(uid)

    return jsonify({
        "status": "ok",
        "profile": normalize_profile(saved),
        "profile_complete": profile_is_complete(saved),
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

def load_follower_ids(user_id: str):
    try:
        res = (
            supabase.table("follows")
            .select("follower_id")
            .eq("following_id", user_id)
            .execute()
        )
        return {str(row.get("follower_id")) for row in (res.data or []) if row.get("follower_id")}
    except Exception:
        return set()


def count_followers(user_id: str):
    try:
        res = (
            supabase.table("follows")
            .select("follower_id")
            .eq("following_id", user_id)
            .execute()
        )
        return len(res.data or [])
    except Exception:
        return 0


def count_following(user_id: str):
    try:
        res = (
            supabase.table("follows")
            .select("following_id")
            .eq("follower_id", user_id)
            .execute()
        )
        return len(res.data or [])
    except Exception:
        return 0


def profile_payload(profile: dict | None, viewer_id: str | None = None):
    clean = normalize_profile(profile)

    profile_id = str(clean.get("id") or "")
    following = False
    is_self = bool(viewer_id and profile_id and viewer_id == profile_id)

    if viewer_id and profile_id and not is_self:
        following = profile_id in load_following_ids(viewer_id)

    clean["is_self"] = is_self
    clean["following"] = following
    clean["followers_count"] = count_followers(profile_id) if profile_id else 0
    clean["following_count"] = count_following(profile_id) if profile_id else 0

    return clean


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


def safe_int(value, fallback=0):
    try:
        if value is None:
            return fallback
        return int(float(value))
    except Exception:
        return fallback


def compute_listen_required_seconds(session_row: dict, audio_duration_seconds: int | None = None):
    existing = safe_int(session_row.get("listen_required_seconds"), 0)
    if existing > 0:
        return existing

    if audio_duration_seconds and audio_duration_seconds > 0:
        return max(30, int(audio_duration_seconds * 0.85))

    length_min = safe_int(session_row.get("length_min"), 0)
    if length_min > 0:
        return max(30, int(length_min * 60 * 0.85))

    return 60


def create_session_post_for_completed_session(session_row: dict):
    user_id = session_row.get("user_id")
    session_id = session_row.get("id")
    job_id = session_row.get("job_id")

    if not user_id or not session_id:
        return {
            "post": None,
            "error": "missing_user_id_or_class_session_id",
        }

    existing = (
        supabase.table("session_posts")
        .select("*")
        .eq("class_session_id", session_id)
        .limit(1)
        .execute()
    )

    if existing.data:
        return {
            "post": existing.data[0],
            "error": None,
        }

    difficulty = session_row.get("difficulty") or ""
    length_min_raw = session_row.get("length_min")
    length_min = safe_int(length_min_raw, 0)
    class_mode = session_row.get("class_mode") or "full"
    music = session_row.get("music") or ""

    title = "Completed a verified Corner class."
    body_bits = []

    if length_min:
        body_bits.append(f"{length_min}-minute")

    if difficulty:
        body_bits.append(str(difficulty).title())

    body_bits.append("Corner Boxing session")

    body = "Finished a verified " + " ".join(body_bits) + "."

    # Important:
    # Some earlier table versions may reject "followers" with a visibility CHECK constraint.
    # Use values most likely to survive old/new schemas.
    preferred_visibility = "public" if bool(session_row.get("is_public")) else "friends_only"

    base_payload = {
        "user_id": str(user_id),
        "class_session_id": str(session_id),
        "job_id": str(job_id) if job_id else None,
        "title": title,
        "body": body,
        "visibility": preferred_visibility,
        "class_mode": str(class_mode),
        "difficulty": str(difficulty),
        "length_min": length_min if length_min else None,
        "music": str(music),
    }

    # Remove None values so older schemas are less likely to reject the insert.
    base_payload = {k: v for k, v in base_payload.items() if v is not None}

    last_error = None

    # Try multiple visibility values in case the DB already had a CHECK constraint.
    visibility_attempts = [preferred_visibility, "followers", "public"]

    for visibility in visibility_attempts:
        payload = {**base_payload, "visibility": visibility}

        try:
            res = supabase.table("session_posts").insert(payload).execute()

            if res.data:
                return {
                    "post": res.data[0],
                    "error": None,
                }

            # Supabase may insert but return no rows depending on client behavior.
            verify = (
                supabase.table("session_posts")
                .select("*")
                .eq("class_session_id", session_id)
                .limit(1)
                .execute()
            )

            if verify.data:
                return {
                    "post": verify.data[0],
                    "error": None,
                }

            last_error = "insert_returned_no_data"

        except Exception as e:
            last_error = str(e)

            # Duplicate race fallback.
            try:
                retry = (
                    supabase.table("session_posts")
                    .select("*")
                    .eq("class_session_id", session_id)
                    .limit(1)
                    .execute()
                )

                if retry.data:
                    return {
                        "post": retry.data[0],
                        "error": None,
                    }
            except Exception as retry_error:
                last_error = f"{last_error} | retry_error={str(retry_error)}"

    return {
        "post": None,
        "error": last_error or "post_insert_failed",
    }


@app.route("/class-session/<job_id>/listen-heartbeat", methods=["POST"])
def listen_heartbeat(job_id):
    uid, err = require_user_id()
    if err:
        return err

    job_id = str(job_id or "").strip()
    data = request.get_json(force=True, silent=True) or {}

    listened_delta_seconds = max(0, min(20, safe_int(data.get("listened_delta_seconds"), 0)))
    audio_position_seconds = max(0, safe_int(data.get("audio_position_seconds"), 0))
    audio_duration_seconds = max(0, safe_int(data.get("audio_duration_seconds"), 0))

    if not job_id:
        return jsonify({
            "status": "error",
            "error": "Missing job id.",
        }), 400

    try:
        sess_res = (
            supabase.table("class_sessions")
            .select("id,job_id,user_id,is_public,class_mode,status,difficulty,length_min,pace,music,listen_required_seconds,listen_progress_seconds,listened_complete,listened_complete_at,storage_path")
            .eq("job_id", job_id)
            .limit(1)
            .execute()
        )

        if not sess_res.data:
            return jsonify({
                "status": "error",
                "error": "Session not found.",
            }), 404

        session_row = sess_res.data[0]

        if str(session_row.get("user_id") or "") != uid:
            return jsonify({
                "status": "error",
                "error": "Forbidden.",
            }), 403

        if str(session_row.get("status") or "").lower() != "done":
            return jsonify({
                "status": "ok",
                "completed": False,
                "reason": "session_not_done",
            }), 200

        required_seconds = compute_listen_required_seconds(session_row, audio_duration_seconds)
        old_progress = safe_int(session_row.get("listen_progress_seconds"), 0)
        new_progress = max(old_progress, old_progress + listened_delta_seconds)

        already_complete = bool(session_row.get("listened_complete"))

        update_payload = {
            "listen_required_seconds": required_seconds,
            "listen_progress_seconds": new_progress,
            "last_heartbeat_at": utc_now_iso(),
        }

        completed_now = False

        if not already_complete and new_progress >= required_seconds:
            completed_now = True
            update_payload["listened_complete"] = True
            update_payload["listened_complete_at"] = utc_now_iso()

        supabase.table("class_sessions").update(update_payload).eq("id", session_row.get("id")).execute()

        post = None
        post_error = None

        if already_complete or completed_now:
            refreshed = (
                supabase.table("class_sessions")
                .select("id,job_id,user_id,is_public,class_mode,status,difficulty,length_min,pace,music,listen_required_seconds,listen_progress_seconds,listened_complete,listened_complete_at,storage_path")
                .eq("id", session_row.get("id"))
                .limit(1)
                .execute()
            )

            refreshed_row = refreshed.data[0] if refreshed.data else {**session_row, **update_payload}

            post_result = create_session_post_for_completed_session(refreshed_row)
            post = post_result.get("post") if isinstance(post_result, dict) else post_result
            post_error = post_result.get("error") if isinstance(post_result, dict) else None

        return jsonify({
            "status": "ok",
            "completed": bool(already_complete or completed_now),
            "completed_now": completed_now,
            "listen_progress_seconds": new_progress,
            "listen_required_seconds": required_seconds,
            "audio_position_seconds": audio_position_seconds,
            "post": post,
            "post_created": bool(post),
            "post_error": post_error,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not update listening progress.",
            "details": str(e),
        }), 500

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


@app.route("/profile/<profile_id>/posts", methods=["GET"])
def profile_posts(profile_id):
    viewer_id = get_verified_user_id_from_request()
    profile_id = str(profile_id or "").strip()

    if not profile_id:
        return jsonify({
            "status": "error",
            "error": "Missing profile id.",
        }), 400

    try:
        following_ids = load_following_ids(viewer_id) if viewer_id else set()
        is_self = bool(viewer_id and viewer_id == profile_id)

        posts_res = (
            supabase.table("session_posts")
            .select("*")
            .eq("user_id", profile_id)
            .order("created_at", desc=True)
            .limit(40)
            .execute()
        )

        visible = []

        for row in (posts_res.data or []):
            visibility = str(row.get("visibility") or row.get("privacy") or "public").lower()

            can_see = False

            if is_self:
                can_see = True
            elif visibility == "public":
                can_see = True
            elif visibility in ("friends", "friends_only", "followers", "followers_only") and profile_id in following_ids:
                can_see = True

            if can_see:
                visible.append(row)

        profiles_by_id = load_profiles_map([profile_id])

        return jsonify({
            "status": "ok",
            "items": [safe_session_post(row, profiles_by_id) for row in visible],
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load profile posts.",
            "details": str(e),
        }), 500


@app.route("/search/users", methods=["GET"])
def search_users():
    uid, err = require_user_id()
    if err:
        return err

    q = str(request.args.get("q") or "").strip().lower()

    try:
        following_ids = load_following_ids(uid)

        res = (
            supabase.table("profiles")
            .select("id,username,display_name,bio,avatar_url")
            .limit(100)
            .execute()
        )

        matches = []

        for row in (res.data or []):
            profile_id = str(row.get("id") or "")
            if not profile_id or profile_id == uid:
                continue

            username = str(row.get("username") or "").lower()
            display_name = str(row.get("display_name") or "").lower()
            bio = str(row.get("bio") or "").lower()

            if q:
                haystack = f"{username} {display_name} {bio}"
                if q not in haystack:
                    continue

            profile = normalize_profile(row)
            profile["following"] = profile_id in following_ids
            profile["is_self"] = False

            matches.append({
                "profile": profile,
                "reason": "Search result" if q else "Corner athlete",
                "already_following": profile_id in following_ids,
            })

            if len(matches) >= 25:
                break

        return jsonify({
            "status": "ok",
            "items": matches,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not search users.",
            "details": str(e),
        }), 500


@app.route("/profile/by-username/<username>", methods=["GET"])
def read_profile_by_username(username):
    viewer_id = get_verified_user_id_from_request()
    username = clean_username(username)

    if not username:
        return jsonify({
            "status": "error",
            "error": "Missing username.",
        }), 400

    try:
        res = (
            supabase.table("profiles")
            .select("id,username,display_name,bio,avatar_url,created_at,plan_tier")
            .eq("username", username)
            .limit(1)
            .execute()
        )

        if not res.data:
            return jsonify({
                "status": "error",
                "error": "Profile not found.",
            }), 404

        return jsonify({
            "status": "ok",
            "profile": profile_payload(res.data[0], viewer_id),
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load profile.",
            "details": str(e),
        }), 500


@app.route("/profile/<profile_id>/following", methods=["GET"])
def profile_following(profile_id):
    viewer_id = get_verified_user_id_from_request()
    profile_id = str(profile_id or "").strip()

    try:
        res = (
            supabase.table("follows")
            .select("following_id,created_at")
            .eq("follower_id", profile_id)
            .execute()
        )

        ids = [str(row.get("following_id")) for row in (res.data or []) if row.get("following_id")]
        profiles_by_id = load_profiles_map(ids)

        items = []
        viewer_following_ids = load_following_ids(viewer_id) if viewer_id else set()

        for target_id in ids:
            profile = profiles_by_id.get(target_id)
            if not profile:
                continue

            profile["following"] = target_id in viewer_following_ids
            profile["is_self"] = bool(viewer_id and viewer_id == target_id)

            items.append({
                "profile": profile,
                "already_following": profile["following"],
                "reason": "Following",
            })

        return jsonify({
            "status": "ok",
            "items": items,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load following.",
            "details": str(e),
        }), 500


@app.route("/profile/<profile_id>/followers", methods=["GET"])
def profile_followers(profile_id):
    viewer_id = get_verified_user_id_from_request()
    profile_id = str(profile_id or "").strip()

    try:
        res = (
            supabase.table("follows")
            .select("follower_id,created_at")
            .eq("following_id", profile_id)
            .execute()
        )

        ids = [str(row.get("follower_id")) for row in (res.data or []) if row.get("follower_id")]
        profiles_by_id = load_profiles_map(ids)

        items = []
        viewer_following_ids = load_following_ids(viewer_id) if viewer_id else set()

        for follower_id in ids:
            profile = profiles_by_id.get(follower_id)
            if not profile:
                continue

            profile["following"] = follower_id in viewer_following_ids
            profile["is_self"] = bool(viewer_id and viewer_id == follower_id)

            items.append({
                "profile": profile,
                "already_following": profile["following"],
                "reason": "Follower",
            })

        return jsonify({
            "status": "ok",
            "items": items,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load followers.",
            "details": str(e),
        }), 500

@app.route("/profile/<profile_id>", methods=["GET"])
def read_profile(profile_id):
    viewer_id = get_verified_user_id_from_request()
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

        return jsonify({
            "status": "ok",
            "profile": profile_payload(res.data[0], viewer_id),
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load profile.",
            "details": str(e),
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
