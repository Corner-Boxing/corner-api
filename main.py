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
        .select("id,username,display_name,bio,avatar_url,account_privacy,plan_tier,subscription_status,tier_updated_at,created_at,updated_at")
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
        "account_privacy": "public",
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
        .select("id,username,display_name,bio,avatar_url,account_privacy,plan_tier,subscription_status,tier_updated_at,created_at,updated_at")
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

    account_privacy = str(data.get("account_privacy") or "public").strip().lower()
    if account_privacy not in ("public", "private"):
        account_privacy = "public"

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
        "account_privacy": account_privacy,
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
        "profile": profile_payload(saved, uid),
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
    account_privacy = str(row.get("account_privacy") or "public").strip().lower()

    if account_privacy not in ("public", "private"):
        account_privacy = "public"

    return {
        "id": row.get("id"),
        "username": username,
        "display_name": display_name or username or "Corner athlete",
        "bio": row.get("bio") or "",
        "avatar_url": row.get("avatar_url") or "assets/avatars/default-avatar-head.png",
        "account_privacy": account_privacy,
    }


def load_profiles_map(user_ids: list[str]):
    clean_ids = [str(x) for x in set(user_ids or []) if x]
    if not clean_ids:
        return {}

    try:
        res = (
            supabase.table("profiles")
            .select("id,username,display_name,bio,avatar_url,account_privacy")
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


def clean_gym_slug(value: str | None):
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9]+", "-", raw)
    raw = re.sub(r"-+", "-", raw).strip("-")
    return raw[:36] or "corner-gym"


def count_gym_members(gym_id: str):
    if not gym_id:
        return 0
    try:
        res = supabase.table("gym_members").select("user_id").eq("gym_id", gym_id).execute()
        return len(res.data or [])
    except Exception:
        return 0


def get_user_primary_gym(user_id: str | None):
    if not user_id:
        return None

    try:
        mem_res = (
            supabase.table("gym_members")
            .select("gym_id,role,joined_at")
            .eq("user_id", user_id)
            .order("joined_at", desc=False)
            .limit(1)
            .execute()
        )

        if not mem_res.data:
            return None

        membership = mem_res.data[0]
        gym_id = membership.get("gym_id")

        gym_res = (
            supabase.table("gyms")
            .select("*")
            .eq("id", gym_id)
            .limit(1)
            .execute()
        )

        if not gym_res.data:
            return None

        gym = normalize_gym(gym_res.data[0], membership=membership)
        return gym

    except Exception:
        return None


def normalize_gym(row: dict | None, membership: dict | None = None):
    row = row or {}
    gym_id = str(row.get("id") or "")

    return {
        "id": row.get("id"),
        "name": row.get("name") or "Corner Gym",
        "slug": row.get("slug") or "",
        "description": row.get("description") or "",
        "badge_url": row.get("badge_url") or "assets/gyms/default-gym-badge.png",
        "banner_url": row.get("banner_url") or "assets/gyms/default-gym-banner.jpg",
        "visibility": row.get("visibility") or "public",
        "owner_id": row.get("owner_id"),
        "member_count": safe_int(row.get("member_count"), 0) or count_gym_members(gym_id),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "my_role": membership.get("role") if membership else None,
        "joined_at": membership.get("joined_at") if membership else None,
    }


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
    clean["primary_gym"] = get_user_primary_gym(profile_id) if profile_id else None

    return clean


def count_post_likes(post_id: str):
    if not post_id:
        return 0
    try:
        res = supabase.table("post_likes").select("post_id").eq("post_id", post_id).execute()
        return len(res.data or [])
    except Exception:
        return 0


def count_post_comments(post_id: str):
    if not post_id:
        return 0
    try:
        res = supabase.table("post_comments").select("id").eq("post_id", post_id).execute()
        return len(res.data or [])
    except Exception:
        return 0


def viewer_liked_post(post_id: str, viewer_id: str | None):
    if not post_id or not viewer_id:
        return False

    try:
        res = (
            supabase.table("post_likes")
            .select("post_id,user_id")
            .eq("post_id", post_id)
            .eq("user_id", viewer_id)
            .limit(1)
            .execute()
        )
        return bool(res.data)
    except Exception:
        return False


def safe_session_post(row: dict, profiles_by_id: dict, viewer_id: str | None = None):
    author_id = str(row.get("user_id") or "")
    profile = profiles_by_id.get(author_id) or normalize_profile({"id": author_id})

    post_id = str(row.get("id") or "")
    visibility = (row.get("visibility") or row.get("privacy") or "public")
    class_mode = row.get("class_mode") or "class"
    difficulty = row.get("difficulty") or ""
    length_min = row.get("length_min") or row.get("length") or ""
    music = row.get("music") or ""
    pace = row.get("pace") or ""
    created_at = row.get("created_at")

    title = row.get("title") or "Completed a Corner class."
    body = row.get("body") or row.get("caption") or "Finished a verified boxing session."

    likes_count = safe_int(row.get("likes_count"), 0)
    comments_count = safe_int(row.get("comments_count"), 0)

    # Backfill counts for old rows where stored counters may still be zero.
    actual_likes = count_post_likes(post_id)
    actual_comments = count_post_comments(post_id)
    likes_count = max(likes_count, actual_likes)
    comments_count = max(comments_count, actual_comments)

    tags = []
    if class_mode:
        tags.append(str(class_mode).replace("_", " ").title())
    if difficulty:
        tags.append(str(difficulty).title())
    if length_min:
        tags.append(f"{length_min} min")
    if pace:
        tags.append(str(pace))
    if music:
        tags.append(f"Music: {music}")
    if visibility:
        tags.append(str(visibility).replace("_", "-"))

    return {
        "id": row.get("id"),
        "type": "session_post",
        "author": profile,
        "title": title,
        "body": body,
        "created_at": created_at,
        "updated_at": row.get("updated_at"),
        "edited_at": row.get("edited_at"),
        "tags": tags[:6],
        "visibility": visibility,
        "class_session_id": row.get("class_session_id"),
        "job_id": row.get("job_id"),
        "music": music,
        "pace": pace,
        "difficulty": difficulty,
        "length_min": length_min,
        "class_mode": class_mode,
        "likes_count": likes_count,
        "comments_count": comments_count,
        "liked_by_me": viewer_liked_post(post_id, viewer_id),
        "is_owner": bool(viewer_id and author_id and viewer_id == author_id),
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
    pace = session_row.get("pace") or ""

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
        "pace": str(pace),
        "likes_count": 0,
        "comments_count": 0,
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

        # Normal verified session posts.
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

            if len(visible_posts) >= 25:
                break

        profiles_by_id = load_profiles_map(author_ids)
        feed_items = [safe_session_post(row, profiles_by_id, viewer_id=uid) for row in visible_posts]

        # Reasonable gym activity in home feed:
        # only session shares + system updates, not every chat message.
        gym, _membership = get_my_gym_membership(uid)

        if gym:
            gym_posts_res = (
                supabase.table("gym_posts")
                .select("*")
                .eq("gym_id", gym["id"])
                .in_("kind", ["session", "system"])
                .order("created_at", desc=True)
                .limit(10)
                .execute()
            )

            gym_rows = gym_posts_res.data or []
            gym_author_ids = [str(r.get("user_id")) for r in gym_rows if r.get("user_id")]
            gym_profiles = load_profiles_map(gym_author_ids)

            session_post_ids = [str(r.get("session_post_id")) for r in gym_rows if r.get("session_post_id")]
            session_posts_by_id = {}

            if session_post_ids:
                linked_res = (
                    supabase.table("session_posts")
                    .select("*")
                    .in_("id", session_post_ids)
                    .execute()
                )

                linked_rows = linked_res.data or []
                linked_author_ids = [str(r.get("user_id")) for r in linked_rows if r.get("user_id")]
                linked_profiles = load_profiles_map(linked_author_ids)

                for linked in linked_rows:
                    session_posts_by_id[str(linked.get("id"))] = safe_session_post(linked, linked_profiles, viewer_id=uid)

            for row in gym_rows:
                safe_item = safe_gym_post(row, gym_profiles, session_posts_by_id)
                safe_item["type"] = "gym_post"
                safe_item["gym"] = gym
                feed_items.append(safe_item)

        feed_items.sort(
            key=lambda item: item.get("created_at") or "",
            reverse=True
        )

        return jsonify({
            "status": "ok",
            "items": feed_items[:35],
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load home feed.",
            "details": str(e),
        }), 500

@app.route("/home/notifications/test", methods=["POST"])
def create_test_notification():
    uid, err = require_user_id()
    if err:
        return err

    result = create_notification(
        uid,
        None,
        "test",
        "Test notification",
        "If you can see this, notification creation is working.",
        "notification",
        None,
    )

    return jsonify({
        "status": "ok" if result.get("ok") else "error",
        "result": result,
    }), 200 if result.get("ok") else 500

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

@app.route("/home/notifications/<notification_id>/read", methods=["POST"])
def mark_notification_read(notification_id):
    uid, err = require_user_id()
    if err:
        return err

    notification_id = str(notification_id or "").strip()

    try:
        supabase.table("notifications").update({
            "read": True,
        }).eq("id", notification_id).eq("user_id", uid).execute()

        return jsonify({
            "status": "ok",
            "notification_id": notification_id,
            "read": True,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not mark notification read.",
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
            .select("id,username,display_name,bio,avatar_url,account_privacy")
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

            actor_profile = ensure_profile_row(uid)
            actor_name = profile_display_name(actor_profile)

            create_notification(
                target_user_id,
                uid,
                "follow",
                f"{actor_name} followed you.",
                "You have a new follower on Corner.",
                "profile",
                uid,
            )

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
        is_following_profile = bool(profile_id in following_ids)

        profile_res = (
            supabase.table("profiles")
            .select("id,username,display_name,bio,avatar_url,account_privacy")
            .eq("id", profile_id)
            .limit(1)
            .execute()
        )

        if not profile_res.data:
            return jsonify({
                "status": "error",
                "error": "Profile not found.",
            }), 404

        profile_row = profile_res.data[0]
        account_privacy = str(profile_row.get("account_privacy") or "public").lower()

        if account_privacy == "private" and not is_self and not is_following_profile:
            return jsonify({
                "status": "locked",
                "error": "This account is private. Follow this athlete to view their class posts.",
                "items": [],
                "locked": True,
                "profile": profile_payload(profile_row, viewer_id),
            }), 403

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
            elif visibility in ("friends", "friends_only", "followers", "followers_only") and is_following_profile:
                can_see = True

            if can_see:
                visible.append(row)

        profiles_by_id = {profile_id: normalize_profile(profile_row)}

        return jsonify({
            "status": "ok",
            "items": [safe_session_post(row, profiles_by_id, viewer_id=viewer_id) for row in visible],
            "locked": False,
            "profile": profile_payload(profile_row, viewer_id),
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load profile posts.",
            "details": str(e),
        }), 500

def can_view_post(row: dict, viewer_id: str | None):
    if not row:
        return False

    author_id = str(row.get("user_id") or "")
    visibility = str(row.get("visibility") or "public").lower()

    if viewer_id and viewer_id == author_id:
        return True

    if visibility == "public":
        return True

    if visibility in ("friends", "friends_only", "followers", "followers_only"):
        if not viewer_id:
            return False
        following_ids = load_following_ids(viewer_id)
        return author_id in following_ids

    return False


def load_post_row(post_id: str):
    if not post_id:
        return None

    res = (
        supabase.table("session_posts")
        .select("*")
        .eq("id", post_id)
        .limit(1)
        .execute()
    )

    return res.data[0] if res.data else None


def update_post_counts(post_id: str):
    if not post_id:
        return

    likes_count = count_post_likes(post_id)
    comments_count = count_post_comments(post_id)

    try:
        supabase.table("session_posts").update({
            "likes_count": likes_count,
            "comments_count": comments_count,
            "updated_at": utc_now_iso(),
        }).eq("id", post_id).execute()
    except Exception:
        pass


@app.route("/session-post/<post_id>", methods=["PATCH", "DELETE"])
def update_or_delete_session_post(post_id):
    uid, err = require_user_id()
    if err:
        return err

    post_id = str(post_id or "").strip()
    row = load_post_row(post_id)

    if not row:
        return jsonify({
            "status": "error",
            "error": "Post not found.",
        }), 404

    if str(row.get("user_id") or "") != uid:
        return jsonify({
            "status": "error",
            "error": "Forbidden.",
        }), 403

    if request.method == "DELETE":
        try:
            supabase.table("session_posts").delete().eq("id", post_id).execute()
            return jsonify({
                "status": "ok",
                "deleted": True,
                "post_id": post_id,
            }), 200
        except Exception as e:
            return jsonify({
                "status": "error",
                "error": "Could not delete post.",
                "details": str(e),
            }), 500

    data = request.get_json(force=True, silent=True) or {}

    title = str(data.get("title") or "").strip()
    body = str(data.get("body") or "").strip()
    visibility = str(data.get("visibility") or row.get("visibility") or "friends_only").strip().lower()

    allowed_visibility = {"public", "friends_only", "followers", "followers_only", "private"}
    if visibility not in allowed_visibility:
        visibility = "friends_only"

    if not title:
        title = "Completed a verified Corner class."

    if len(title) > 80:
        return jsonify({
            "status": "error",
            "error": "Title must be 80 characters or less.",
        }), 400

    if len(body) > 500:
        return jsonify({
            "status": "error",
            "error": "Description must be 500 characters or less.",
        }), 400

    try:
        update_payload = {
            "title": title,
            "body": body,
            "visibility": visibility,
            "edited_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }

        res = (
            supabase.table("session_posts")
            .update(update_payload)
            .eq("id", post_id)
            .execute()
        )

        saved = res.data[0] if res.data else load_post_row(post_id)
        profiles_by_id = load_profiles_map([uid])

        return jsonify({
            "status": "ok",
            "post": safe_session_post(saved, profiles_by_id, viewer_id=uid),
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not update post.",
            "details": str(e),
        }), 500


@app.route("/session-post/<post_id>/like", methods=["POST", "DELETE"])
def like_session_post(post_id):
    uid, err = require_user_id()
    if err:
        return err

    post_id = str(post_id or "").strip()
    row = load_post_row(post_id)

    if not row:
        return jsonify({
            "status": "error",
            "error": "Post not found.",
        }), 404

    if not can_view_post(row, uid):
        return jsonify({
            "status": "error",
            "error": "Forbidden.",
        }), 403

    try:
        if request.method == "DELETE":
            supabase.table("post_likes").delete().eq("post_id", post_id).eq("user_id", uid).execute()
            update_post_counts(post_id)

            return jsonify({
                "status": "ok",
                "liked": False,
                "likes_count": count_post_likes(post_id),
            }), 200

        existing = (
            supabase.table("post_likes")
            .select("post_id,user_id")
            .eq("post_id", post_id)
            .eq("user_id", uid)
            .limit(1)
            .execute()
        )

        if not existing.data:
            supabase.table("post_likes").insert({
                "post_id": post_id,
                "user_id": uid,
            }).execute()

            owner_id = str(row.get("user_id") or "")
            actor_profile = ensure_profile_row(uid)
            actor_name = profile_display_name(actor_profile)

            create_notification(
                owner_id,
                uid,
                "like",
                f"{actor_name} liked your session.",
                row.get("title") or "Someone liked your verified Corner session.",
                "session_post",
                post_id,
            )

        update_post_counts(post_id)

        return jsonify({
            "status": "ok",
            "liked": True,
            "likes_count": count_post_likes(post_id),
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not update like.",
            "details": str(e),
        }), 500


@app.route("/session-post/<post_id>/comments", methods=["GET", "POST"])
def session_post_comments(post_id):
    uid, err = require_user_id()
    if err:
        return err

    post_id = str(post_id or "").strip()
    row = load_post_row(post_id)

    if not row:
        return jsonify({
            "status": "error",
            "error": "Post not found.",
        }), 404

    if not can_view_post(row, uid):
        return jsonify({
            "status": "error",
            "error": "Forbidden.",
        }), 403

    if request.method == "POST":
        data = request.get_json(force=True, silent=True) or {}
        body = str(data.get("body") or "").strip()

        if not body:
            return jsonify({
                "status": "error",
                "error": "Comment cannot be empty.",
            }), 400

        if len(body) > 500:
            return jsonify({
                "status": "error",
                "error": "Comment must be 500 characters or less.",
            }), 400

        try:
            insert_res = supabase.table("post_comments").insert({
                "post_id": post_id,
                "user_id": uid,
                "body": body,
            }).execute()

            update_post_counts(post_id)

            owner_id = str(row.get("user_id") or "")
            actor_profile = ensure_profile_row(uid)
            actor_name = profile_display_name(actor_profile)

            create_notification(
                owner_id,
                uid,
                "comment",
                f"{actor_name} commented on your session.",
                body,
                "session_post",
                post_id,
            )

            comment = insert_res.data[0] if insert_res.data else None
            profiles_by_id = load_profiles_map([uid])

            return jsonify({
                "status": "ok",
                "comment": safe_comment(comment, profiles_by_id, uid) if comment else None,
                "comments_count": count_post_comments(post_id),
            }), 201

        except Exception as e:
            return jsonify({
                "status": "error",
                "error": "Could not add comment.",
                "details": str(e),
            }), 500

    try:
        comments_res = (
            supabase.table("post_comments")
            .select("*")
            .eq("post_id", post_id)
            .order("created_at", desc=False)
            .limit(100)
            .execute()
        )

        comments = comments_res.data or []
        author_ids = [str(c.get("user_id")) for c in comments if c.get("user_id")]
        profiles_by_id = load_profiles_map(author_ids)

        return jsonify({
            "status": "ok",
            "items": [safe_comment(c, profiles_by_id, uid) for c in comments],
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load comments.",
            "details": str(e),
        }), 500


@app.route("/session-comment/<comment_id>", methods=["DELETE"])
def delete_session_comment(comment_id):
    uid, err = require_user_id()
    if err:
        return err

    comment_id = str(comment_id or "").strip()

    try:
        comment_res = (
            supabase.table("post_comments")
            .select("*")
            .eq("id", comment_id)
            .limit(1)
            .execute()
        )

        if not comment_res.data:
            return jsonify({
                "status": "error",
                "error": "Comment not found.",
            }), 404

        comment = comment_res.data[0]
        post_id = str(comment.get("post_id") or "")
        post = load_post_row(post_id)

        is_comment_owner = str(comment.get("user_id") or "") == uid
        is_post_owner = bool(post and str(post.get("user_id") or "") == uid)

        if not is_comment_owner and not is_post_owner:
            return jsonify({
                "status": "error",
                "error": "Forbidden.",
            }), 403

        supabase.table("post_comments").delete().eq("id", comment_id).execute()
        update_post_counts(post_id)

        return jsonify({
            "status": "ok",
            "deleted": True,
            "comment_id": comment_id,
            "comments_count": count_post_comments(post_id),
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not delete comment.",
            "details": str(e),
        }), 500


def safe_comment(row: dict | None, profiles_by_id: dict, viewer_id: str | None):
    if not row:
        return None

    author_id = str(row.get("user_id") or "")
    author = profiles_by_id.get(author_id) or normalize_profile({"id": author_id})

    return {
        "id": row.get("id"),
        "post_id": row.get("post_id"),
        "body": row.get("body") or "",
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "author": author,
        "is_owner": bool(viewer_id and author_id and viewer_id == author_id),
    }

def clean_notification_uuid(value):
    raw = str(value or "").strip()
    if re.fullmatch(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
        raw,
    ):
        return raw
    return None


def create_notification(
    user_id: str | None,
    actor_id: str | None,
    type_: str,
    title: str,
    body: str = "",
    entity_type: str | None = None,
    entity_id: str | None = None,
):
    if not user_id:
        return {
            "ok": False,
            "error": "missing_user_id",
        }

    # Do not notify yourself. It feels broken and noisy.
    if actor_id and str(user_id) == str(actor_id):
        return {
            "ok": True,
            "skipped": True,
            "reason": "self_notification",
        }

    base_payload = {
        "user_id": str(user_id),
        "actor_id": str(actor_id) if actor_id else None,
        "type": str(type_ or "notification")[:80],
        "title": str(title or "Notification")[:120],
        "body": str(body or "")[:500],
        "entity_type": str(entity_type)[:80] if entity_type else None,
    }

    clean_entity_id = clean_notification_uuid(entity_id)
    if clean_entity_id:
        base_payload["entity_id"] = clean_entity_id

    base_payload = {k: v for k, v in base_payload.items() if v is not None}

    # Try a few safe variants because earlier versions may have had slightly different schemas.
    payload_attempts = [
        {**base_payload, "read": False},
        {**base_payload, "is_read": False},
        base_payload,
    ]

    last_error = None

    for payload in payload_attempts:
        try:
            res = supabase.table("notifications").insert(payload).execute()
            err_msg = supa_err(res)

            if err_msg:
                last_error = err_msg
                print("[create_notification] insert response error:", err_msg, "payload_keys=", list(payload.keys()))
                continue

            if getattr(res, "data", None):
                print("[create_notification] created:", res.data[0])
                return {
                    "ok": True,
                    "notification": res.data[0],
                }

            # Some Supabase responses can succeed but return no rows.
            verify = (
                supabase.table("notifications")
                .select("*")
                .eq("user_id", str(user_id))
                .eq("type", str(type_ or "notification")[:80])
                .order("created_at", desc=True)
                .limit(1)
                .execute()
            )

            if verify.data:
                print("[create_notification] created verified:", verify.data[0])
                return {
                    "ok": True,
                    "notification": verify.data[0],
                }

            last_error = "insert_returned_no_data"
            print("[create_notification] no data after insert payload_keys=", list(payload.keys()))

        except Exception as e:
            last_error = str(e)
            print("[create_notification] failed:", str(e), "payload_keys=", list(payload.keys()))

    return {
        "ok": False,
        "error": last_error or "notification_insert_failed",
    }


def notify_gym_members(
    gym_id: str,
    actor_id: str | None,
    type_: str,
    title: str,
    body: str = "",
    entity_type: str | None = "gym",
    entity_id: str | None = None,
):
    if not gym_id:
        return

    try:
        members = (
            supabase.table("gym_members")
            .select("user_id")
            .eq("gym_id", gym_id)
            .execute()
        )

        for row in (members.data or []):
            member_id = row.get("user_id")
            create_notification(
                member_id,
                actor_id,
                type_,
                title,
                body,
                entity_type,
                entity_id or gym_id,
            )
    except Exception:
        pass

def get_my_gym_membership(user_id: str):
    if not user_id:
        return None, None

    mem_res = (
        supabase.table("gym_members")
        .select("gym_id,user_id,role,joined_at")
        .eq("user_id", user_id)
        .order("joined_at", desc=False)
        .limit(1)
        .execute()
    )

    if not mem_res.data:
        return None, None

    membership = mem_res.data[0]
    gym_id = membership.get("gym_id")

    gym_res = (
        supabase.table("gyms")
        .select("*")
        .eq("id", gym_id)
        .limit(1)
        .execute()
    )

    if not gym_res.data:
        return None, None

    return normalize_gym(gym_res.data[0], membership=membership), membership


def user_is_gym_member(gym_id: str, user_id: str):
    if not gym_id or not user_id:
        return False

    try:
        res = (
            supabase.table("gym_members")
            .select("gym_id,user_id")
            .eq("gym_id", gym_id)
            .eq("user_id", user_id)
            .limit(1)
            .execute()
        )
        return bool(res.data)
    except Exception:
        return False


def load_gym_members(gym_id: str):
    try:
        mem_res = (
            supabase.table("gym_members")
            .select("gym_id,user_id,role,joined_at")
            .eq("gym_id", gym_id)
            .order("joined_at", desc=False)
            .limit(100)
            .execute()
        )

        members = mem_res.data or []
        user_ids = [str(m.get("user_id")) for m in members if m.get("user_id")]
        profiles_by_id = load_profiles_map(user_ids)

        items = []
        for m in members:
            uid = str(m.get("user_id") or "")
            profile = profiles_by_id.get(uid) or normalize_profile({"id": uid})
            items.append({
                "user_id": uid,
                "role": m.get("role") or "member",
                "joined_at": m.get("joined_at"),
                "profile": profile_payload(profile, viewer_id=None),
            })

        return items
    except Exception:
        return []


def safe_gym_post(row: dict, profiles_by_id: dict, session_posts_by_id: dict | None = None):
    author_id = str(row.get("user_id") or "")
    profile = profiles_by_id.get(author_id) or normalize_profile({"id": author_id})
    session_post_id = str(row.get("session_post_id") or "")
    linked_post = None

    if session_post_id and session_posts_by_id:
        linked_post = session_posts_by_id.get(session_post_id)

    return {
        "id": row.get("id"),
        "gym_id": row.get("gym_id"),
        "kind": row.get("kind") or "message",
        "body": row.get("body") or "",
        "created_at": row.get("created_at"),
        "author": profile,
        "session_post_id": row.get("session_post_id"),
        "session_post": linked_post,
    }


def load_gym_posts(gym_id: str, viewer_id: str):
    posts_res = (
        supabase.table("gym_posts")
        .select("*")
        .eq("gym_id", gym_id)
        .order("created_at", desc=True)
        .limit(80)
        .execute()
    )

    rows = posts_res.data or []
    author_ids = [str(r.get("user_id")) for r in rows if r.get("user_id")]
    session_post_ids = [str(r.get("session_post_id")) for r in rows if r.get("session_post_id")]

    profiles_by_id = load_profiles_map(author_ids)
    session_posts_by_id = {}

    if session_post_ids:
        sp_res = (
            supabase.table("session_posts")
            .select("*")
            .in_("id", session_post_ids)
            .execute()
        )

        raw_session_posts = sp_res.data or []
        sp_author_ids = [str(r.get("user_id")) for r in raw_session_posts if r.get("user_id")]
        sp_profiles = load_profiles_map(sp_author_ids)

        for sp in raw_session_posts:
            session_posts_by_id[str(sp.get("id"))] = safe_session_post(sp, sp_profiles, viewer_id=viewer_id)

    return [safe_gym_post(row, profiles_by_id, session_posts_by_id) for row in rows]


def refresh_gym_member_count(gym_id: str):
    total = count_gym_members(gym_id)
    try:
        supabase.table("gyms").update({
            "member_count": total,
            "updated_at": utc_now_iso(),
        }).eq("id", gym_id).execute()
    except Exception:
        pass
    return total


@app.route("/gyms/me", methods=["GET"])
def my_gym():
    uid, err = require_user_id()
    if err:
        return err

    try:
        gym, membership = get_my_gym_membership(uid)

        if not gym:
            return jsonify({
                "status": "ok",
                "has_gym": False,
                "gym": None,
                "members": [],
                "posts": [],
            }), 200

        posts = load_gym_posts(gym["id"], uid)
        members = load_gym_members(gym["id"])

        return jsonify({
            "status": "ok",
            "has_gym": True,
            "gym": gym,
            "membership": membership,
            "members": members,
            "posts": posts,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load gym.",
            "details": str(e),
        }), 500


@app.route("/gyms/discover", methods=["GET"])
def discover_gyms():
    uid, err = require_user_id()
    if err:
        return err

    q = str(request.args.get("q") or "").strip().lower()

    try:
        my_gym, _ = get_my_gym_membership(uid)
        my_gym_id = str(my_gym.get("id")) if my_gym else None

        res = (
            supabase.table("gyms")
            .select("*")
            .eq("visibility", "public")
            .order("created_at", desc=True)
            .limit(80)
            .execute()
        )

        items = []

        for row in (res.data or []):
            name = str(row.get("name") or "").lower()
            slug = str(row.get("slug") or "").lower()
            desc = str(row.get("description") or "").lower()

            if q and q not in f"{name} {slug} {desc}":
                continue

            gym = normalize_gym(row)
            gym["already_joined"] = bool(my_gym_id and my_gym_id == str(gym.get("id")))
            items.append(gym)

            if len(items) >= 30:
                break

        return jsonify({
            "status": "ok",
            "items": items,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not discover gyms.",
            "details": str(e),
        }), 500


@app.route("/gyms/create", methods=["POST"])
def create_gym():
    uid, err = require_user_id()
    if err:
        return err

    data = request.get_json(force=True, silent=True) or {}

    name = str(data.get("name") or "").strip()
    description = str(data.get("description") or "").strip()
    visibility = str(data.get("visibility") or "public").strip().lower()

    if not name or len(name) > 40:
        return jsonify({
            "status": "error",
            "error": "Gym name must be 1-40 characters.",
        }), 400

    if len(description) > 240:
        return jsonify({
            "status": "error",
            "error": "Gym description must be 240 characters or less.",
        }), 400

    if visibility not in ("public", "private"):
        visibility = "public"

    try:
        existing_gym, _ = get_my_gym_membership(uid)
        if existing_gym:
            return jsonify({
                "status": "error",
                "error": "You are already in a gym. Leave it before creating another.",
                "gym": existing_gym,
            }), 409

        base_slug = clean_gym_slug(name)
        slug = base_slug

        for i in range(0, 20):
            test_slug = base_slug if i == 0 else f"{base_slug}-{i + 1}"
            exists = (
                supabase.table("gyms")
                .select("id")
                .eq("slug", test_slug)
                .limit(1)
                .execute()
            )
            if not exists.data:
                slug = test_slug
                break

        gym_payload = {
            "owner_id": uid,
            "name": name,
            "slug": slug,
            "description": description,
            "visibility": visibility,
            "badge_url": "assets/gyms/default-gym-badge.png",
            "banner_url": "assets/gyms/default-gym-banner.jpg",
            "member_count": 1,
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }

        gym_res = supabase.table("gyms").insert(gym_payload).execute()

        if not gym_res.data:
            return jsonify({
                "status": "error",
                "error": "Gym creation failed.",
            }), 500

        gym_row = gym_res.data[0]
        gym_id = gym_row.get("id")

        supabase.table("gym_members").insert({
            "gym_id": gym_id,
            "user_id": uid,
            "role": "owner",
            "joined_at": utc_now_iso(),
        }).execute()

        supabase.table("gym_posts").insert({
            "gym_id": gym_id,
            "user_id": uid,
            "kind": "system",
            "body": f"{name} was created.",
        }).execute()

        return jsonify({
            "status": "ok",
            "gym": normalize_gym(gym_row, membership={"role": "owner", "joined_at": utc_now_iso()}),
        }), 201

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not create gym.",
            "details": str(e),
        }), 500

def get_gym_role(gym_id: str, user_id: str):
    if not gym_id or not user_id:
        return None

    try:
        res = (
            supabase.table("gym_members")
            .select("role")
            .eq("gym_id", gym_id)
            .eq("user_id", user_id)
            .limit(1)
            .execute()
        )

        if not res.data:
            return None

        return str(res.data[0].get("role") or "member").lower()
    except Exception:
        return None


@app.route("/gyms/<gym_id>", methods=["PATCH"])
def update_gym(gym_id):
    uid, err = require_user_id()
    if err:
        return err

    gym_id = str(gym_id or "").strip()
    role = get_gym_role(gym_id, uid)

    if role not in ("owner", "admin"):
        return jsonify({
            "status": "error",
            "error": "Only a gym owner or admin can edit this gym.",
        }), 403

    data = request.get_json(force=True, silent=True) or {}

    name = str(data.get("name") or "").strip()
    description = str(data.get("description") or "").strip()
    visibility = str(data.get("visibility") or "public").strip().lower()

    if not name or len(name) > 40:
        return jsonify({
            "status": "error",
            "error": "Gym name must be 1-40 characters.",
        }), 400

    if len(description) > 240:
        return jsonify({
            "status": "error",
            "error": "Gym description must be 240 characters or less.",
        }), 400

    if visibility not in ("public", "private"):
        visibility = "public"

    try:
        update_res = (
            supabase.table("gyms")
            .update({
                "name": name,
                "description": description,
                "visibility": visibility,
                "updated_at": utc_now_iso(),
            })
            .eq("id", gym_id)
            .execute()
        )

        err_msg = supa_err(update_res)
        if err_msg:
            return jsonify({
                "status": "error",
                "error": "Gym update failed.",
                "details": err_msg,
            }), 500

        gym_res = (
            supabase.table("gyms")
            .select("*")
            .eq("id", gym_id)
            .limit(1)
            .execute()
        )

        membership = {
            "gym_id": gym_id,
            "user_id": uid,
            "role": role,
            "joined_at": None,
        }

        gym = normalize_gym(gym_res.data[0], membership=membership) if gym_res.data else None

        return jsonify({
            "status": "ok",
            "gym": gym,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not update gym.",
            "details": str(e),
        }), 500


@app.route("/gyms/<gym_id>", methods=["DELETE"])
def delete_gym(gym_id):
    uid, err = require_user_id()
    if err:
        return err

    gym_id = str(gym_id or "").strip()
    role = get_gym_role(gym_id, uid)

    if role != "owner":
        return jsonify({
            "status": "error",
            "error": "Only the gym owner can delete this gym.",
        }), 403

    try:
        supabase.table("gyms").delete().eq("id", gym_id).execute()

        return jsonify({
            "status": "ok",
            "deleted": True,
            "gym_id": gym_id,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not delete gym.",
            "details": str(e),
        }), 500

@app.route("/gyms/<gym_id>/join", methods=["POST"])
def join_gym(gym_id):
    uid, err = require_user_id()
    if err:
        return err

    gym_id = str(gym_id or "").strip()

    try:
        existing_gym, _ = get_my_gym_membership(uid)
        if existing_gym:
            return jsonify({
                "status": "error",
                "error": "You are already in a gym. Leave it before joining another.",
                "gym": existing_gym,
            }), 409

        gym_res = (
            supabase.table("gyms")
            .select("*")
            .eq("id", gym_id)
            .limit(1)
            .execute()
        )

        if not gym_res.data:
            return jsonify({
                "status": "error",
                "error": "Gym not found.",
            }), 404

        gym_row = gym_res.data[0]

        if str(gym_row.get("visibility") or "public").lower() != "public":
            return jsonify({
                "status": "error",
                "error": "This gym is private.",
            }), 403

        supabase.table("gym_members").insert({
            "gym_id": gym_id,
            "user_id": uid,
            "role": "member",
            "joined_at": utc_now_iso(),
        }).execute()

        refresh_gym_member_count(gym_id)

        profile = ensure_profile_row(uid)
        display = profile_display_name(profile)

        supabase.table("gym_posts").insert({
            "gym_id": gym_id,
            "user_id": uid,
            "kind": "system",
            "body": f"{display} joined the gym.",
        }).execute()

        notify_gym_members(
            gym_id,
            uid,
            "gym_join",
            f"{display} joined your gym.",
            "A new member joined the gym.",
            "gym",
            gym_id,
        )

        gym, membership = get_my_gym_membership(uid)

        return jsonify({
            "status": "ok",
            "gym": gym,
            "membership": membership,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not join gym.",
            "details": str(e),
        }), 500


@app.route("/gyms/<gym_id>/leave", methods=["POST"])
def leave_gym(gym_id):
    uid, err = require_user_id()
    if err:
        return err

    gym_id = str(gym_id or "").strip()

    try:
        mem_res = (
            supabase.table("gym_members")
            .select("gym_id,user_id,role")
            .eq("gym_id", gym_id)
            .eq("user_id", uid)
            .limit(1)
            .execute()
        )

        if not mem_res.data:
            return jsonify({
                "status": "error",
                "error": "You are not in this gym.",
            }), 404

        role = str(mem_res.data[0].get("role") or "member").lower()

        if role == "owner":
            other_members = (
                supabase.table("gym_members")
                .select("user_id")
                .eq("gym_id", gym_id)
                .neq("user_id", uid)
                .limit(1)
                .execute()
            )

            if other_members.data:
                return jsonify({
                    "status": "error",
                    "error": "Owner cannot leave while other members remain. Transfer ownership later, or remove members first.",
                }), 409

        supabase.table("gym_members").delete().eq("gym_id", gym_id).eq("user_id", uid).execute()
        remaining = refresh_gym_member_count(gym_id)

        if remaining <= 0:
            supabase.table("gyms").delete().eq("id", gym_id).execute()

        return jsonify({
            "status": "ok",
            "left": True,
            "deleted_gym": remaining <= 0,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not leave gym.",
            "details": str(e),
        }), 500


@app.route("/gyms/<gym_id>/members", methods=["GET"])
def gym_members(gym_id):
    uid, err = require_user_id()
    if err:
        return err

    gym_id = str(gym_id or "").strip()

    if not user_is_gym_member(gym_id, uid):
        return jsonify({
            "status": "error",
            "error": "You must be a member to view members.",
        }), 403

    return jsonify({
        "status": "ok",
        "items": load_gym_members(gym_id),
    }), 200


@app.route("/gyms/<gym_id>/posts", methods=["GET", "POST"])
def gym_posts(gym_id):
    uid, err = require_user_id()
    if err:
        return err

    gym_id = str(gym_id or "").strip()

    if not user_is_gym_member(gym_id, uid):
        return jsonify({
            "status": "error",
            "error": "You must be a member to use this gym feed.",
        }), 403

    if request.method == "POST":
        data = request.get_json(force=True, silent=True) or {}
        body = str(data.get("body") or "").strip()

        if not body:
            return jsonify({
                "status": "error",
                "error": "Message cannot be empty.",
            }), 400

        if len(body) > 500:
            return jsonify({
                "status": "error",
                "error": "Message must be 500 characters or less.",
            }), 400

        try:
            supabase.table("gym_posts").insert({
                "gym_id": gym_id,
                "user_id": uid,
                "kind": "message",
                "body": body,
            }).execute()

            actor_profile = ensure_profile_row(uid)
            actor_name = profile_display_name(actor_profile)

            notify_gym_members(
                gym_id,
                uid,
                "gym_message",
                f"{actor_name} posted in your gym.",
                body,
                "gym",
                gym_id,
            )

            return jsonify({
                "status": "ok",
                "items": load_gym_posts(gym_id, uid),
            }), 201

        except Exception as e:
            return jsonify({
                "status": "error",
                "error": "Could not post message.",
                "details": str(e),
            }), 500

    try:
        return jsonify({
            "status": "ok",
            "items": load_gym_posts(gym_id, uid),
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load gym posts.",
            "details": str(e),
        }), 500

@app.route("/gyms/<gym_id>/invite", methods=["POST"])
def create_gym_invite(gym_id):
    uid, err = require_user_id()
    if err:
        return err

    gym_id = str(gym_id or "").strip()

    if not user_is_gym_member(gym_id, uid):
        return jsonify({
            "status": "error",
            "error": "You must be a member to create an invite.",
        }), 403

    try:
        existing = (
            supabase.table("gym_invites")
            .select("*")
            .eq("gym_id", gym_id)
            .eq("created_by", uid)
            .eq("active", True)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )

        if existing.data:
            invite = existing.data[0]
        else:
            code = base64.urlsafe_b64encode(os.urandom(9)).decode("utf-8").replace("=", "")

            invite_res = supabase.table("gym_invites").insert({
                "gym_id": gym_id,
                "created_by": uid,
                "code": code,
                "active": True,
            }).execute()

            invite = invite_res.data[0] if invite_res.data else {"code": code}

        return jsonify({
            "status": "ok",
            "code": invite.get("code"),
            "join_text": f"Join my Corner gym with code: {invite.get('code')}",
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not create invite.",
            "details": str(e),
        }), 500


@app.route("/gyms/join-code/<code>", methods=["POST"])
def join_gym_by_code(code):
    uid, err = require_user_id()
    if err:
        return err

    code = str(code or "").strip()

    if not code:
        return jsonify({
            "status": "error",
            "error": "Missing invite code.",
        }), 400

    try:
        existing_gym, _ = get_my_gym_membership(uid)
        if existing_gym:
            return jsonify({
                "status": "error",
                "error": "You are already in a gym. Leave it before joining another.",
                "gym": existing_gym,
            }), 409

        invite_res = (
            supabase.table("gym_invites")
            .select("*")
            .eq("code", code)
            .eq("active", True)
            .limit(1)
            .execute()
        )

        if not invite_res.data:
            return jsonify({
                "status": "error",
                "error": "Invite code not found.",
            }), 404

        invite = invite_res.data[0]
        gym_id = str(invite.get("gym_id") or "")

        gym_res = (
            supabase.table("gyms")
            .select("*")
            .eq("id", gym_id)
            .limit(1)
            .execute()
        )

        if not gym_res.data:
            return jsonify({
                "status": "error",
                "error": "Gym not found.",
            }), 404

        supabase.table("gym_members").insert({
            "gym_id": gym_id,
            "user_id": uid,
            "role": "member",
            "joined_at": utc_now_iso(),
        }).execute()

        refresh_gym_member_count(gym_id)

        profile = ensure_profile_row(uid)
        display = profile_display_name(profile)

        supabase.table("gym_posts").insert({
            "gym_id": gym_id,
            "user_id": uid,
            "kind": "system",
            "body": f"{display} joined by invite code.",
        }).execute()

        notify_gym_members(
            gym_id,
            uid,
            "gym_join",
            f"{display} joined your gym.",
            "A new member joined through an invite code.",
            "gym",
            gym_id,
        )

        gym, membership = get_my_gym_membership(uid)

        return jsonify({
            "status": "ok",
            "gym": gym,
            "membership": membership,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not join with invite code.",
            "details": str(e),
        }), 500

@app.route("/gyms/<gym_id>/share-session", methods=["POST"])
def share_session_to_gym(gym_id):
    uid, err = require_user_id()
    if err:
        return err

    gym_id = str(gym_id or "").strip()

    if not user_is_gym_member(gym_id, uid):
        return jsonify({
            "status": "error",
            "error": "You must be a member to share to this gym.",
        }), 403

    try:
        data = request.get_json(force=True, silent=True) or {}
        session_post_id = str(data.get("session_post_id") or "").strip()

        if not session_post_id:
            latest = (
                supabase.table("session_posts")
                .select("*")
                .eq("user_id", uid)
                .order("created_at", desc=True)
                .limit(1)
                .execute()
            )

            if not latest.data:
                return jsonify({
                    "status": "error",
                    "error": "No session post found to share.",
                }), 404

            session_post_id = str(latest.data[0].get("id") or "")

        post_res = (
            supabase.table("session_posts")
            .select("*")
            .eq("id", session_post_id)
            .eq("user_id", uid)
            .limit(1)
            .execute()
        )

        if not post_res.data:
            return jsonify({
                "status": "error",
                "error": "Session post not found.",
            }), 404

        existing = (
            supabase.table("gym_posts")
            .select("id")
            .eq("gym_id", gym_id)
            .eq("session_post_id", session_post_id)
            .limit(1)
            .execute()
        )

        if not existing.data:
            title = post_res.data[0].get("title") or "Completed a Corner class."
            supabase.table("gym_posts").insert({
                "gym_id": gym_id,
                "user_id": uid,
                "kind": "session",
                "body": title,
                "session_post_id": session_post_id,
            }).execute()

            actor_profile = ensure_profile_row(uid)
            actor_name = profile_display_name(actor_profile)

            notify_gym_members(
                gym_id,
                uid,
                "gym_session",
                f"{actor_name} shared a verified session.",
                title,
                "session_post",
                session_post_id,
            )

        return jsonify({
            "status": "ok",
            "items": load_gym_posts(gym_id, uid),
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not share session to gym.",
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
            .select("id,username,display_name,bio,avatar_url,account_privacy")
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
            .select("id,username,display_name,bio,avatar_url,account_privacy,created_at,plan_tier")
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
            .select("id,username,display_name,bio,avatar_url,account_privacy,created_at,plan_tier")
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
