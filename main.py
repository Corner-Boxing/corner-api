import os
import time
import base64
import json
import re
from datetime import datetime, timezone, timedelta
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
GYM_POST_RETENTION_DAYS = 10
_guest_last_generate_by_ip = {}

DEVELOPER_USER_IDS = {
    "8c2fe150-4d87-4622-8b8a-42b6e572a016",
}

PROMO_BENEFIT_TYPES = {
    "pro_forever",
    "pro_days",
    "discount_percent",
    "discount_fixed_cents",
}

BANNER_OVERLAY_OPTIONS = {
    "none",
    "abstract",
    "blocky",
    "fabric",
    "gritty",
    "ringside",
    "stripes",
    "tiger",
    "wave",
}

GYM_BADGE_BORDER_OPTIONS = {
    "none",
    "basic",
    "broken",
    "double",
}

GYM_BADGE_ICON_OPTIONS = {
    "none",
    "bag",
    "bell",
    "bolt",
    "champs",
    "corner",
    "eagle",
    "fist",
    "flame",
    "flight",
    "glove",
    "heart",
    "iron",
    "king",
    "lion",
    "shield",
    "skull",
    "star",
    "summit",
    "target",
    "wild",
}

DEFAULT_PROFILE_BANNER_CONFIG = {
    "base_color": "#101114",
    "overlay": "ringside",
    "overlay_color": "#e63946",
    "overlay_opacity": 38,
}

DEFAULT_GYM_BANNER_CONFIG = {
    "base_color": "#101114",
    "overlay": "fabric",
    "overlay_color": "#d4af37",
    "overlay_opacity": 32,
}

DEFAULT_GYM_BADGE_CONFIG = {
    "base_color": "#111214",
    "border": "double",
    "border_color": "#d4af37",
    "icon": "corner",
    "icon_color": "#f6f6f6",
}

def is_developer_user(user_id: str | None):
    return bool(
        user_id
        and str(user_id) in DEVELOPER_USER_IDS
    )


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

AVATAR_VERSION = "avatar-v2"

AVATAR_ACHIEVEMENT_BADGES = {
    "first-strike": {
        "name": "First Strike Badge",
        "unlock_description": "Complete your first class.",
    },
    "contender": {
        "name": "Contender Badge",
        "unlock_description": "Complete 10 classes.",
    },
    "pro-fighter": {
        "name": "Pro Fighter Badge",
        "unlock_description": "Complete 50 classes.",
    },
    "century-club": {
        "name": "Century Club Badge",
        "unlock_description": "Complete 100 classes.",
    },
    "novice": {
        "name": "Novice Badge",
        "unlock_description": "Reach level 1.",
    },
    "amateur": {
        "name": "Amateur Badge",
        "unlock_description": "Reach level 2.",
    },
    "heavy-hitter": {
        "name": "Heavy Hitter Badge",
        "unlock_description": "Reach level 5.",
    },
    "ding-ding": {
        "name": "Ding Ding Badge",
        "unlock_description": "Complete 100 total rounds.",
    },
    "charged": {
        "name": "Charged Badge",
        "unlock_description": "Earn 100 total Endurance stat points.",
    },
    "speeding-up": {
        "name": "Speeding Up Badge",
        "unlock_description": "Earn 100 total Speed stat points.",
    },
    "feel-the-power": {
        "name": "Feel the Power Badge",
        "unlock_description": "Earn 100 total Power stat points.",
    },
    "stand-back": {
        "name": "Stand Back Badge",
        "unlock_description": "Earn 100 total Defense stat points.",
    },
    "jump-around": {
        "name": "Jump Around Badge",
        "unlock_description": "Earn 100 total Footwork stat points.",
    },
    "good-eye": {
        "name": "Good Eye Badge",
        "unlock_description": "Earn 100 total Focus stat points.",
    },
    "huddle-up": {
        "name": "Huddle Up Badge",
        "unlock_description": "Join a gym for the first time.",
    },
    "thousand-island": {
        "name": "Thousand Island Badge",
        "unlock_description": "Earn 1,000 total stat points across all categories.",
    },
}

AVATAR_ALLOWED_OPTIONS = {
    "stance": {"guard-front"},
    "head": {"standard"},

    "eyes": {
        "angry",
        "focused",
        "neutral",
        "shiner",
        "tired",
    },

    "hair": {
        "none",
        "bob",
        "braids",
        "buzz-cut",
        "crew-cut",
        "curly-long",
        "curly-medium",
        "middle-part",
        "pigtails",
        "ponytail",
        "straight-long",
    },

    "facial_hair": {
        "none",
        "beard",
        "goatee",
        "mustache",
        "stubble",
    },

    "top": {
        "none",
        "basic-tee",
        "compression-tee",
        "robe",
        "training-tank",
    },

    "bottom": {
        "boxing-shorts",
        "joggers",
        "leggings",
    },

    "gloves": {
        "none",
        "classic",
    },

    "wraps": {
        "none",
        "standard",
    },

    "shoes": {
        "none",
        "boxing-boots",
        "training-shoes",
    },

    "headwear": {
        "none",
        "basic-cap",
        "headband",
    },

    "facewear": {
        "none",
        "glasses",
        "shades",
    },

    "neckwear": {
        "none",
        "basic-chain",
        "cross-chain",
        "pendant-chain",
    },

    "badge": {
        "none",
        "corner-badge",
        "gym-badge",
        *AVATAR_ACHIEVEMENT_BADGES.keys(),
    },
}

AVATAR_LIMITED_COLOR_STOPS = {
    "neckwear": [
        "#d4af37",
        "#c2c6cc",
        "#9a6a3a",
        "#6f4a2e",
    ],
}

AVATAR_DEFAULT_CONFIG = {
    "version": AVATAR_VERSION,

    "presentation": "masc",

    "colors": {
        "skin": "#b8754f",
        "hair": "#392316",
        "eyes": "#111217",

        "top": "#111217",
        "bottom": "#e63946",

        "gloves": "#e63946",
        "wraps": "#f6f6f6",
        "shoes": "#111217",

        "headwear": "#111217",
        "facewear": "#111217",
        "neckwear": "#d4af37",
    },

    "parts": {
        "stance": "guard-front",
        "head": "standard",

        "eyes": "neutral",
        "hair": "curly-medium",
        "facial_hair": "none",

        "top": "training-tank",
        "bottom": "boxing-shorts",

        "gloves": "classic",
        "wraps": "standard",
        "shoes": "boxing-boots",

        "headwear": "none",
        "facewear": "none",
        "neckwear": "none",
        "badge": "none",
    },
}


def default_avatar_config():
    return json.loads(json.dumps(AVATAR_DEFAULT_CONFIG))


def safe_hex_color(value, fallback):
    value = str(value or "").strip().lower()

    if re.fullmatch(r"#[0-9a-f]{6}", value):
        return value

    return fallback

def normalize_banner_config(value, fallback=None):
    base = dict(fallback or DEFAULT_PROFILE_BANNER_CONFIG)
    raw = safe_json_value(value, {}) if value is not None else {}

    overlay = str(raw.get("overlay") or base["overlay"]).strip().lower()
    if overlay not in BANNER_OVERLAY_OPTIONS:
        overlay = base["overlay"]

    try:
        overlay_opacity = int(raw.get("overlay_opacity", base["overlay_opacity"]))
    except Exception:
        overlay_opacity = base["overlay_opacity"]

    overlay_opacity = max(0, min(100, overlay_opacity))

    return {
        "base_color": safe_hex_color(raw.get("base_color"), base["base_color"]),
        "overlay": overlay,
        "overlay_color": safe_hex_color(raw.get("overlay_color"), base["overlay_color"]),
        "overlay_opacity": overlay_opacity,
    }


def normalize_gym_badge_config(value):
    raw = safe_json_value(value, {}) if value is not None else {}

    border = str(raw.get("border") or DEFAULT_GYM_BADGE_CONFIG["border"]).strip().lower()
    if border not in GYM_BADGE_BORDER_OPTIONS:
        border = DEFAULT_GYM_BADGE_CONFIG["border"]

    icon = str(raw.get("icon") or DEFAULT_GYM_BADGE_CONFIG["icon"]).strip().lower()
    if icon not in GYM_BADGE_ICON_OPTIONS:
        icon = DEFAULT_GYM_BADGE_CONFIG["icon"]

    return {
        "base_color": safe_hex_color(raw.get("base_color"), DEFAULT_GYM_BADGE_CONFIG["base_color"]),
        "border": border,
        "border_color": safe_hex_color(raw.get("border_color"), DEFAULT_GYM_BADGE_CONFIG["border_color"]),
        "icon": icon,
        "icon_color": safe_hex_color(raw.get("icon_color"), DEFAULT_GYM_BADGE_CONFIG["icon_color"]),
    }

def hex_to_rgb_tuple(value: str):
    clean = str(value or "").strip().lower()

    if not re.fullmatch(r"#[0-9a-f]{6}", clean):
        return (255, 255, 255)

    return (
        int(clean[1:3], 16),
        int(clean[3:5], 16),
        int(clean[5:7], 16),
    )


def rgb_tuple_to_hex(rgb):
    values = []

    for value in rgb:
        values.append(
            max(
                0,
                min(
                    255,
                    int(round(value)),
                ),
            )
        )

    return "#{:02x}{:02x}{:02x}".format(
        values[0],
        values[1],
        values[2],
    )


def snap_hex_to_palette_gradient(
    value: str,
    palette: list[str],
    fallback: str,
):
    candidate = safe_hex_color(
        value,
        fallback,
    )

    colors = [
        hex_to_rgb_tuple(color)
        for color in (palette or [])
        if re.fullmatch(
            r"#[0-9a-f]{6}",
            str(color or "").strip().lower(),
        )
    ]

    if not colors:
        return candidate

    if len(colors) == 1:
        return rgb_tuple_to_hex(
            colors[0]
        )

    target = hex_to_rgb_tuple(
        candidate
    )

    best_rgb = colors[0]
    best_distance = None

    for index in range(
        len(colors) - 1
    ):
        a = colors[index]
        b = colors[index + 1]

        vx = b[0] - a[0]
        vy = b[1] - a[1]
        vz = b[2] - a[2]

        wx = target[0] - a[0]
        wy = target[1] - a[1]
        wz = target[2] - a[2]

        length_squared = (
            (vx * vx)
            + (vy * vy)
            + (vz * vz)
        )

        if length_squared > 0:
            t = (
                (wx * vx)
                + (wy * vy)
                + (wz * vz)
            ) / length_squared
        else:
            t = 0.0

        t = max(
            0.0,
            min(
                1.0,
                t,
            ),
        )

        projected = (
            a[0] + (vx * t),
            a[1] + (vy * t),
            a[2] + (vz * t),
        )

        distance = (
            (target[0] - projected[0]) ** 2
            + (target[1] - projected[1]) ** 2
            + (target[2] - projected[2]) ** 2
        )

        if (
            best_distance is None
            or distance < best_distance
        ):
            best_distance = distance
            best_rgb = projected

    return rgb_tuple_to_hex(
        best_rgb
    )


def safe_avatar_id(value, fallback):
    value = str(value or "").strip().lower()

    if re.fullmatch(r"[a-z0-9_-]{1,64}", value):
        return value

    return fallback


def clean_avatar_config(value, user_id: str | None = None):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = {}

    if not isinstance(value, dict):
        value = {}

    clean = default_avatar_config()

    presentation = str(
        value.get("presentation")
        or clean["presentation"]
    ).strip().lower()

    if presentation in ("masc", "fem"):
        clean["presentation"] = presentation

    colors = (
        value.get("colors")
        if isinstance(value.get("colors"), dict)
        else {}
    )

    for key, fallback in clean["colors"].items():
        candidate = safe_hex_color(
            colors.get(key),
            fallback,
        )

        limited_palette = (
            AVATAR_LIMITED_COLOR_STOPS.get(
                key
            )
        )

        if limited_palette:
            candidate = (
                snap_hex_to_palette_gradient(
                    candidate,
                    limited_palette,
                    fallback,
                )
            )

        clean["colors"][key] = (
            candidate
        )

    parts = (
        value.get("parts")
        if isinstance(value.get("parts"), dict)
        else {}
    )

    for key, fallback in clean["parts"].items():
        candidate = safe_avatar_id(
            parts.get(key),
            fallback,
        )

        allowed = AVATAR_ALLOWED_OPTIONS.get(key)

        clean["parts"][key] = (
            candidate
            if not allowed or candidate in allowed
            else fallback
        )

    # Leggings only exist in the feminine asset tree.
    if (
        clean["presentation"] == "masc"
        and clean["parts"].get("bottom") == "leggings"
    ):
        clean["parts"]["bottom"] = "boxing-shorts"

    # A bottom is always required.
    if clean["parts"].get("bottom") in ("", "none", None):
        clean["parts"]["bottom"] = (
            AVATAR_DEFAULT_CONFIG["parts"]["bottom"]
        )

    # Facial hair is unavailable on the feminine base.
    if clean["presentation"] == "fem":
        clean["parts"]["facial_hair"] = "none"

    # Achievement badge ownership is server-authoritative.
    selected_badge = (
        clean["parts"].get("badge")
        or "none"
    )

    if (
        selected_badge in AVATAR_ACHIEVEMENT_BADGES
        and user_id
    ):
        try:
            unlocked = get_unlocked_avatar_badges(user_id)

            if selected_badge not in unlocked:
                clean["parts"]["badge"] = "none"

        except Exception:
            clean["parts"]["badge"] = "none"

    if (
        selected_badge == "gym-badge"
        and user_id
    ):
        try:
            if not get_user_primary_gym(user_id):
                clean["parts"]["badge"] = "none"
        except Exception:
            clean["parts"]["badge"] = "none"

    clean["version"] = AVATAR_VERSION

    return clean

def ensure_profile_row(user_id: str):
    if not user_id:
        return None

    res = (
        supabase.table("profiles")
        .select("id,username,display_name,bio,avatar_url,account_privacy,avatar_config,avatar_version,avatar_updated_at,banner_config,plan_tier,subscription_status,tier_updated_at,created_at,updated_at")
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
        "avatar_url": "",
        "avatar_config": default_avatar_config(),
        "avatar_version": AVATAR_VERSION,
        "avatar_updated_at": utc_now_iso(),
        "account_privacy": "public",
        "plan_tier": "free",
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
    }

    insert_res = supabase.table("profiles").insert(payload).execute()

    if insert_res.data:
        # Current Corner level math begins at level 1.
        # A genuinely new account therefore earns Novice immediately.
        record_avatar_badge_unlock(
            user_id,
            "novice",
            notify=True,
        )

        return insert_res.data[0]

    # Race-condition fallback: if another request created it first, read again.
    retry = (
        supabase.table("profiles")
        .select("id,username,display_name,bio,avatar_url,account_privacy,avatar_config,avatar_version,avatar_updated_at,banner_config,plan_tier,subscription_status,tier_updated_at,created_at,updated_at")
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

def parse_utc_datetime(value):
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(
            str(value).replace(
                "Z",
                "+00:00",
            )
        )

        if parsed.tzinfo is None:
            parsed = parsed.replace(
                tzinfo=timezone.utc
            )

        return parsed.astimezone(
            timezone.utc
        )

    except Exception:
        return None


def get_effective_plan(
    user_id: str | None,
):
    if not user_id:
        return {
            "plan_tier": "free",
            "source": "guest",
            "pro_expires_at": None,
            "promo_code_id": None,
        }

    base_tier = "free"

    try:
        profile_res = (
            supabase
            .table("profiles")
            .select("plan_tier")
            .eq("id", user_id)
            .limit(1)
            .execute()
        )

        if (
            profile_res.data
            and isinstance(
                profile_res.data[0],
                dict,
            )
        ):
            base_tier = str(
                profile_res.data[0]
                .get("plan_tier")
                or "free"
            ).strip().lower()

    except Exception:
        base_tier = "free"

    if base_tier == "pro":
        return {
            "plan_tier": "pro",
            "source": "profile",
            "pro_expires_at": None,
            "promo_code_id": None,
        }

    try:
        entitlement_res = (
            supabase
            .table("promo_entitlements")
            .select(
                "id,"
                "promo_code_id,"
                "entitlement_type,"
                "starts_at,"
                "ends_at,"
                "revoked_at,"
                "created_at"
            )
            .eq(
                "user_id",
                user_id,
            )
            .execute()
        )

        now = datetime.now(
            timezone.utc
        )

        active_rows = []

        for row in (
            entitlement_res.data
            or []
        ):
            if row.get("revoked_at"):
                continue

            if (
                str(
                    row.get(
                        "entitlement_type"
                    )
                    or ""
                ).lower()
                != "pro"
            ):
                continue

            starts_at = (
                parse_utc_datetime(
                    row.get("starts_at")
                )
                or now
            )

            ends_at = (
                parse_utc_datetime(
                    row.get("ends_at")
                )
            )

            if starts_at > now:
                continue

            if (
                ends_at is not None
                and ends_at <= now
            ):
                continue

            active_rows.append(
                {
                    **row,
                    "_starts_at": starts_at,
                    "_ends_at": ends_at,
                }
            )

        if active_rows:
            permanent = next(
                (
                    row
                    for row in active_rows
                    if row["_ends_at"]
                    is None
                ),
                None,
            )

            if permanent:
                selected = permanent
            else:
                selected = max(
                    active_rows,
                    key=lambda row:
                        row["_ends_at"],
                )

            expires_at = (
                selected["_ends_at"]
                .isoformat()
                if selected["_ends_at"]
                else None
            )

            return {
                "plan_tier": "pro",
                "source": "promo",
                "pro_expires_at": expires_at,
                "promo_code_id":
                    selected.get(
                        "promo_code_id"
                    ),
            }

    except Exception:
        pass

    return {
        "plan_tier": "free",
        "source": "free",
        "pro_expires_at": None,
        "promo_code_id": None,
    }


def get_plan_tier(user_id: str):
    return (
        get_effective_plan(
            user_id
        )
        .get(
            "plan_tier",
            "free",
        )
    )

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
    uid = (
        get_verified_user_id_from_request()
    )

    if not uid:
        return jsonify({
            "signed_in": False,
        }), 200

    row = ensure_profile_row(
        uid
    )

    effective_plan = (
        get_effective_plan(
            uid
        )
    )

    return jsonify({
        "signed_in": True,
        "user_id": uid,

        "plan_tier":
            effective_plan[
                "plan_tier"
            ],

        "plan_source":
            effective_plan[
                "source"
            ],

        "pro_expires_at":
            effective_plan[
                "pro_expires_at"
            ],

        "is_developer":
            is_developer_user(
                uid
            ),

        "profile":
            profile_payload(
                row,
                uid,
            ),

        "profile_complete":
            profile_is_complete(
                row
            ),
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

    banner_config = normalize_banner_config(
        data.get("banner_config"),
        DEFAULT_PROFILE_BANNER_CONFIG,
    )

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
        avatar_url = ""

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
        "banner_config": banner_config,
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

@app.route("/me/avatar", methods=["PATCH"])
def update_my_avatar():
    uid, err = require_user_id()
    if err:
        return err

    data = request.get_json(force=True, silent=True) or {}
    avatar_config = clean_avatar_config(
        data.get("avatar_config"),
        user_id=uid,
    )

    try:
        ensure_profile_row(uid)

        payload = {
            "avatar_config": avatar_config,
            "avatar_version": AVATAR_VERSION,
            "avatar_updated_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        }

        res = (
            supabase.table("profiles")
            .update(payload)
            .eq("id", uid)
            .execute()
        )

        err_msg = supa_err(res)
        if err_msg:
            return jsonify({
                "status": "error",
                "error": "Avatar update failed.",
                "details": err_msg,
            }), 500

        saved = ensure_profile_row(uid)

        return jsonify({
            "status": "ok",
            "profile": profile_payload(saved, uid),
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not save avatar.",
            "details": str(e),
        }), 500

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

        # 2) Determine EFFECTIVE tier.
        #    Permanent/paid Pro and active
        #    promo Pro both count.
        effective_plan = (
            get_effective_plan(
                user_id
            )
            if user_id
            else {
                "plan_tier": "free",
                "source": "guest",
                "pro_expires_at": None,
            }
        )

        plan_tier = (
            effective_plan.get(
                "plan_tier",
                "free",
            )
        )

        is_pro = (
            plan_tier == "pro"
        )

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

            "class_mode":
                class_mode,

            "plan_tier":
                plan_tier
                if user_id
                else "guest",

            "plan_source":
                effective_plan.get(
                    "source",
                    "guest",
                ),

            "pro_expires_at":
                effective_plan.get(
                    "pro_expires_at"
                ),
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

def require_developer_user_id():
    uid, err = require_user_id()

    if err:
        return None, err

    if not is_developer_user(uid):
        return None, (
            jsonify({
                "status": "error",
                "error": "Developer access required.",
            }),
            403,
        )

    return uid, None


def normalize_promo_code(value):
    return re.sub(
        r"\s+",
        "",
        str(value or "")
        .strip()
        .upper(),
    )


def promo_int(
    value,
    default=0,
):
    try:
        if value in (
            None,
            "",
        ):
            return default

        return int(value)

    except Exception:
        return default


def promo_datetime_text(
    value,
):
    if value in (
        None,
        "",
    ):
        return None

    parsed = parse_utc_datetime(
        value
    )

    if not parsed:
        raise ValueError(
            "Invalid date/time."
        )

    return parsed.isoformat()


def promo_benefit_is_valid(
    benefit_type,
    benefit_value,
):
    if benefit_type == "pro_forever":
        return benefit_value == 0

    if benefit_type == "pro_days":
        return (
            1
            <= benefit_value
            <= 3650
        )

    if benefit_type == "discount_percent":
        return (
            1
            <= benefit_value
            <= 100
        )

    if benefit_type == "discount_fixed_cents":
        return (
            1
            <= benefit_value
            <= 1000000
        )

    return False


@app.route(
    "/promo/redeem",
    methods=["POST"],
)
def redeem_promo():
    uid, err = require_user_id()

    if err:
        return err

    data = (
        request.get_json(
            force=True,
            silent=True,
        )
        or {}
    )

    code = normalize_promo_code(
        data.get("code")
    )

    if not re.fullmatch(
        r"[A-Z0-9][A-Z0-9_-]{2,31}",
        code,
    ):
        return jsonify({
            "status": "error",
            "error": "Enter a valid promo code.",
        }), 400

    try:
        rpc_res = (
            supabase
            .rpc(
                "redeem_corner_promo",
                {
                    "p_user_id": uid,
                    "p_code": code,
                },
            )
            .execute()
        )

        result = rpc_res.data

        if (
            isinstance(
                result,
                list,
            )
            and result
        ):
            result = result[0]

        if isinstance(
            result,
            str,
        ):
            try:
                result = json.loads(
                    result
                )
            except Exception:
                result = None

        if not isinstance(
            result,
            dict,
        ):
            return jsonify({
                "status": "error",
                "error":
                    "Promo redemption failed.",
            }), 500

        if (
            result.get("status")
            != "ok"
        ):
            error_code = str(
                result.get("error")
                or "invalid_code"
            )

            messages = {
                "invalid_code":
                    "That promo code does not exist.",

                "inactive_code":
                    "That promo code is no longer active.",

                "not_started":
                    "That promo code is not active yet.",

                "expired_code":
                    "That promo code has expired.",

                "redemption_limit_reached":
                    "That promo code has reached its redemption limit.",

                "already_redeemed":
                    "You have already used that promo code.",
            }

            status_code = (
                409
                if error_code in {
                    "redemption_limit_reached",
                    "already_redeemed",
                }
                else 400
            )

            return jsonify({
                "status": "error",
                "error":
                    messages.get(
                        error_code,
                        "Promo code could not be redeemed.",
                    ),
                "code":
                    error_code,
            }), status_code

        effective_plan = (
            get_effective_plan(
                uid
            )
        )

        benefit_type = str(
            result.get(
                "benefit_type"
            )
            or ""
        )

        benefit_value = promo_int(
            result.get(
                "benefit_value"
            ),
            0,
        )

        if benefit_type == "pro_forever":
            message = (
                "Promo applied. "
                "Your account now has Pro access."
            )

        elif benefit_type == "pro_days":
            message = (
                "Promo applied. "
                f"You received {benefit_value} "
                "days of Pro access."
            )

        elif benefit_type == "discount_percent":
            message = (
                "Promo saved. "
                f"{benefit_value}% off is now "
                "attached to your account for "
                "future Pro checkout."
            )

        elif benefit_type == "discount_fixed_cents":
            message = (
                "Promo saved. "
                f"${benefit_value / 100:.2f} off "
                "is now attached to your account "
                "for future Pro checkout."
            )

        else:
            message = (
                "Promo applied successfully."
            )

        return jsonify({
            "status": "ok",
            "message": message,

            "benefit_type":
                benefit_type,

            "benefit_value":
                benefit_value,

            "plan_tier":
                effective_plan[
                    "plan_tier"
                ],

            "plan_source":
                effective_plan[
                    "source"
                ],

            "pro_expires_at":
                effective_plan[
                    "pro_expires_at"
                ],
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error":
                "Could not redeem promo code.",
            "details":
                str(e),
        }), 500


@app.route(
    "/admin/promo-codes",
    methods=["GET", "POST"],
)
def admin_promo_codes():
    uid, err = (
        require_developer_user_id()
    )

    if err:
        return err

    if request.method == "GET":
        try:
            codes_res = (
                supabase
                .table("promo_codes")
                .select("*")
                .order(
                    "created_at",
                    desc=True,
                )
                .limit(100)
                .execute()
            )

            redemptions_res = (
                supabase
                .table("promo_redemptions")
                .select("promo_code_id")
                .execute()
            )

            counts = {}

            for row in (
                redemptions_res.data
                or []
            ):
                promo_id = str(
                    row.get(
                        "promo_code_id"
                    )
                    or ""
                )

                counts[promo_id] = (
                    counts.get(
                        promo_id,
                        0,
                    )
                    + 1
                )

            items = []

            for row in (
                codes_res.data
                or []
            ):
                promo_id = str(
                    row.get("id")
                    or ""
                )

                items.append({
                    **row,
                    "redemption_count":
                        counts.get(
                            promo_id,
                            0,
                        ),
                })

            return jsonify({
                "status": "ok",
                "items": items,
            }), 200

        except Exception as e:
            return jsonify({
                "status": "error",
                "error":
                    "Could not load promo codes.",
                "details":
                    str(e),
            }), 500


    data = (
        request.get_json(
            force=True,
            silent=True,
        )
        or {}
    )

    code = normalize_promo_code(
        data.get("code")
    )

    if not re.fullmatch(
        r"[A-Z0-9][A-Z0-9_-]{2,31}",
        code,
    ):
        return jsonify({
            "status": "error",
            "error":
                "Promo code must be 3-32 characters using letters, numbers, - or _.",
        }), 400

    label = str(
        data.get("label")
        or ""
    ).strip()[:80]

    benefit_type = str(
        data.get(
            "benefit_type"
        )
        or ""
    ).strip().lower()

    if (
        benefit_type
        not in PROMO_BENEFIT_TYPES
    ):
        return jsonify({
            "status": "error",
            "error":
                "Invalid promo benefit type.",
        }), 400

    benefit_value = promo_int(
        data.get(
            "benefit_value"
        ),
        0,
    )

    if benefit_type == "pro_forever":
        benefit_value = 0

    if not promo_benefit_is_valid(
        benefit_type,
        benefit_value,
    ):
        return jsonify({
            "status": "error",
            "error":
                "Invalid benefit value.",
        }), 400

    max_redemptions = promo_int(
        data.get(
            "max_redemptions"
        ),
        0,
    )

    if max_redemptions <= 0:
        max_redemptions = None

    max_per_user = max(
        1,
        promo_int(
            data.get(
                "max_redemptions_per_user"
            ),
            1,
        ),
    )

    try:
        starts_at = (
            promo_datetime_text(
                data.get("starts_at")
            )
        )

        ends_at = (
            promo_datetime_text(
                data.get("ends_at")
            )
        )

    except ValueError as e:
        return jsonify({
            "status": "error",
            "error": str(e),
        }), 400

    if (
        starts_at
        and ends_at
        and parse_utc_datetime(
            ends_at
        )
        <= parse_utc_datetime(
            starts_at
        )
    ):
        return jsonify({
            "status": "error",
            "error":
                "End time must be after start time.",
        }), 400

    stripe_coupon_id = str(
        data.get(
            "stripe_coupon_id"
        )
        or ""
    ).strip()[:128]

    try:
        existing = (
            supabase
            .table("promo_codes")
            .select("id")
            .eq(
                "code",
                code,
            )
            .limit(1)
            .execute()
        )

        if existing.data:
            return jsonify({
                "status": "error",
                "error":
                    "That promo code already exists.",
            }), 409

        payload = {
            "code":
                code,

            "label":
                label,

            "active":
                True,

            "benefit_type":
                benefit_type,

            "benefit_value":
                benefit_value,

            "starts_at":
                starts_at,

            "ends_at":
                ends_at,

            "max_redemptions":
                max_redemptions,

            "max_redemptions_per_user":
                max_per_user,

            "created_by":
                uid,

            "stripe_coupon_id":
                (
                    stripe_coupon_id
                    or None
                ),

            "updated_at":
                utc_now_iso(),
        }

        insert_res = (
            supabase
            .table("promo_codes")
            .insert(payload)
            .execute()
        )

        if not insert_res.data:
            return jsonify({
                "status": "error",
                "error":
                    "Promo code creation returned no row.",
            }), 500

        return jsonify({
            "status": "ok",
            "promo":
                insert_res.data[0],
        }), 201

    except Exception as e:
        return jsonify({
            "status": "error",
            "error":
                "Could not create promo code.",
            "details":
                str(e),
        }), 500


@app.route(
    "/admin/promo-codes/<promo_id>",
    methods=["PATCH"],
)
def admin_update_promo_code(
    promo_id,
):
    uid, err = (
        require_developer_user_id()
    )

    if err:
        return err

    data = (
        request.get_json(
            force=True,
            silent=True,
        )
        or {}
    )

    payload = {}

    if "active" in data:
        payload["active"] = bool(
            data.get("active")
        )

    if "label" in data:
        payload["label"] = str(
            data.get("label")
            or ""
        ).strip()[:80]

    if "ends_at" in data:
        try:
            payload["ends_at"] = (
                promo_datetime_text(
                    data.get("ends_at")
                )
            )
        except ValueError as e:
            return jsonify({
                "status": "error",
                "error": str(e),
            }), 400

    if "max_redemptions" in data:
        max_redemptions = promo_int(
            data.get(
                "max_redemptions"
            ),
            0,
        )

        payload[
            "max_redemptions"
        ] = (
            max_redemptions
            if max_redemptions > 0
            else None
        )

    if (
        "max_redemptions_per_user"
        in data
    ):
        payload[
            "max_redemptions_per_user"
        ] = max(
            1,
            promo_int(
                data.get(
                    "max_redemptions_per_user"
                ),
                1,
            ),
        )

    if not payload:
        return jsonify({
            "status": "error",
            "error":
                "No promo changes supplied.",
        }), 400

    payload["updated_at"] = (
        utc_now_iso()
    )

    try:
        res = (
            supabase
            .table("promo_codes")
            .update(payload)
            .eq(
                "id",
                promo_id,
            )
            .execute()
        )

        if not res.data:
            return jsonify({
                "status": "error",
                "error":
                    "Promo code not found.",
            }), 404

        return jsonify({
            "status": "ok",
            "promo":
                res.data[0],
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error":
                "Could not update promo code.",
            "details":
                str(e),
        }), 500


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

    avatar_config = clean_avatar_config(
        row.get("avatar_config")
    )

    banner_config = normalize_banner_config(
        row.get("banner_config"),
        DEFAULT_PROFILE_BANNER_CONFIG,
    )

    avatar_primary_gym = None

    if (
        row.get("id")
        and avatar_config
            .get("parts", {})
            .get("badge")
            == "gym-badge"
    ):
        try:
            avatar_primary_gym = (
                get_user_primary_gym(
                    str(row.get("id"))
                )
            )
        except Exception:
            avatar_primary_gym = None

    return {
        "id": row.get("id"),
        "username": username,
        "display_name": (
            display_name
            or username
            or "Corner athlete"
        ),
        "bio": row.get("bio") or "",
        "avatar_url": (
            row.get("avatar_url")
            or ""
        ),
        "avatar_config": avatar_config,
        "avatar_version": (
            row.get("avatar_version")
            or AVATAR_VERSION
        ),
        "avatar_updated_at": (
            row.get("avatar_updated_at")
        ),
        "account_privacy": account_privacy,
        "banner_config": banner_config,
        "primary_gym": avatar_primary_gym,
    }


def load_profiles_map(user_ids: list[str]):
    clean_ids = [str(x) for x in set(user_ids or []) if x]
    if not clean_ids:
        return {}

    try:
        res = (
            supabase.table("profiles")
            .select("id,username,display_name,bio,avatar_url,account_privacy,avatar_config,avatar_version,avatar_updated_at,banner_config")
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


def load_pending_follow_request_ids(requester_id: str | None):
    if not requester_id:
        return set()

    try:
        res = (
            supabase.table("follow_requests")
            .select("target_id")
            .eq("requester_id", requester_id)
            .eq("status", "pending")
            .execute()
        )
        return {str(row.get("target_id")) for row in (res.data or []) if row.get("target_id")}
    except Exception:
        return set()


def get_pending_follow_request(requester_id: str, target_id: str):
    if not requester_id or not target_id:
        return None

    try:
        res = (
            supabase.table("follow_requests")
            .select("*")
            .eq("requester_id", requester_id)
            .eq("target_id", target_id)
            .eq("status", "pending")
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception:
        return None


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
        "badge_config": normalize_gym_badge_config(row.get("badge_config")),
        "banner_config": normalize_banner_config(
            row.get("banner_config"),
            DEFAULT_GYM_BANNER_CONFIG,
        ),
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
    follow_requested = False
    is_self = bool(viewer_id and profile_id and viewer_id == profile_id)

    if viewer_id and profile_id and not is_self:
        following = profile_id in load_following_ids(viewer_id)
        follow_requested = profile_id in load_pending_follow_request_ids(viewer_id)

    clean["is_self"] = is_self
    clean["following"] = following
    clean["follow_requested"] = follow_requested
    clean["followers_count"] = (
        count_followers(profile_id)
        if profile_id
        else 0
    )

    clean["following_count"] = (
        count_following(profile_id)
        if profile_id
        else 0
    )

    primary_gym = (
        get_user_primary_gym(profile_id)
        if profile_id
        else None
    )

    stats = (
        calculate_user_stats(profile_id)
        if profile_id
        else empty_user_stats()
    )

    clean["primary_gym"] = primary_gym
    clean["stats"] = stats

    if profile_id and is_self:
        # Existing users may already satisfy achievements.
        # Record those silently during migration instead of flooding
        # them with historical badge notifications.
        sync_progress_badge_unlocks(
            profile_id,
            stats,
            notify_new=False,
        )

        if primary_gym:
            record_avatar_badge_unlock(
                profile_id,
                "huddle-up",
                notify=False,
            )

        clean["unlocked_badges"] = sorted(
            get_unlocked_avatar_badges(
                profile_id,
                stats,
            )
        )

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

    stat_awards = safe_json_value(row.get("stat_awards"), {})
    round_breakdown = safe_json_value(row.get("round_breakdown"), [])
    stat_total_points = safe_int(row.get("stat_total_points"), sum(safe_int(v, 0) for v in (stat_awards or {}).values()))
    round_count = safe_int(row.get("round_count"), len(round_breakdown) if isinstance(round_breakdown, list) else 0)

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
    if round_count:
        tags.append(f"{round_count} rounds")
    if stat_total_points:
        tags.append(f"+{stat_total_points} pts")
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
        "tags": tags[:8],
        "visibility": visibility,
        "class_session_id": row.get("class_session_id"),
        "job_id": row.get("job_id"),
        "music": music,
        "pace": pace,
        "difficulty": difficulty,
        "length_min": length_min,
        "class_mode": class_mode,
        "stat_awards": stat_awards if isinstance(stat_awards, dict) else {},
        "stat_total_points": stat_total_points,
        "round_breakdown": round_breakdown if isinstance(round_breakdown, list) else [],
        "round_count": round_count,
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


STAT_KEYS = ["power", "speed", "defense", "footwork", "endurance", "focus"]

STAT_LABELS = {
    "power": "Power",
    "speed": "Speed",
    "defense": "Defense",
    "footwork": "Footwork",
    "endurance": "Endurance",
    "focus": "Focus",
}


def empty_stat_points():
    return {key: 0 for key in STAT_KEYS}


def safe_json_value(value, fallback):
    if value is None:
        return fallback

    if isinstance(value, (dict, list)):
        return value

    try:
        parsed = json.loads(value)
        if isinstance(parsed, type(fallback)):
            return parsed
        return fallback
    except Exception:
        return fallback


def stat_level_from_points(points):
    points = max(0, safe_int(points, 0))
    if points <= 0:
        return 0
    return min(99, 1 + (points // 20))


def build_stats_summary(stat_points: dict, completed_classes: int, total_minutes: int, total_rounds: int):
    clean_points = empty_stat_points()

    for key in STAT_KEYS:
        clean_points[key] = max(0, safe_int((stat_points or {}).get(key), 0))

    total_points = sum(clean_points.values())
    stat_levels = {key: stat_level_from_points(clean_points.get(key, 0)) for key in STAT_KEYS}

    primary_stat = max(STAT_KEYS, key=lambda key: clean_points.get(key, 0)) if total_points else None
    level = 1 + (total_points // 100)
    next_level_points = max(0, (level * 100) - total_points)

    return {
        "stat_keys": STAT_KEYS,
        "stat_labels": STAT_LABELS,
        "stat_points": clean_points,
        "stat_levels": stat_levels,
        "total_points": total_points,
        "level": level,
        "next_level_points": next_level_points,
        "completed_classes": max(0, safe_int(completed_classes, 0)),
        "total_minutes": max(0, safe_int(total_minutes, 0)),
        "total_rounds": max(0, safe_int(total_rounds, 0)),
        "primary_stat": primary_stat,
    }


def empty_user_stats():
    return build_stats_summary(empty_stat_points(), 0, 0, 0)


def calculate_user_stats(user_id: str | None):
    if not user_id:
        return empty_user_stats()

    try:
        res = (
            supabase.table("class_sessions")
            .select("id,length_min,round_count,stat_total_points,stat_awards,round_breakdown,status,listened_complete,completed_at")
            .eq("user_id", user_id)
            .eq("status", "done")
            .eq("listened_complete", True)
            .order("completed_at", desc=True)
            .limit(300)
            .execute()
        )

        rows = res.data or []
        points = empty_stat_points()
        total_minutes = 0
        total_rounds = 0
        completed_classes = 0

        round_type_counts = {}

        for row in rows:
            completed_classes += 1
            total_minutes += max(0, safe_int(row.get("length_min"), 0))

            awards = safe_json_value(row.get("stat_awards"), {})
            for key in STAT_KEYS:
                points[key] += max(0, safe_int((awards or {}).get(key), 0))

            breakdown = safe_json_value(row.get("round_breakdown"), [])
            if isinstance(breakdown, list):
                total_rounds += len(breakdown)
                for r in breakdown:
                    if isinstance(r, dict):
                        rt = str(r.get("type") or "").strip()
                        if rt:
                            round_type_counts[rt] = round_type_counts.get(rt, 0) + 1
            else:
                total_rounds += max(0, safe_int(row.get("round_count"), 0))

        summary = build_stats_summary(points, completed_classes, total_minutes, total_rounds)
        summary["round_type_counts"] = round_type_counts
        return summary

    except Exception:
        return empty_user_stats()

def badge_ids_from_stats(stats: dict | None):
    stats = stats or empty_user_stats()

    stat_points = (
        stats.get("stat_points")
        if isinstance(stats.get("stat_points"), dict)
        else {}
    )

    completed_classes = max(
        0,
        safe_int(stats.get("completed_classes"), 0),
    )

    total_rounds = max(
        0,
        safe_int(stats.get("total_rounds"), 0),
    )

    total_points = max(
        0,
        safe_int(stats.get("total_points"), 0),
    )

    level = max(
        1,
        safe_int(stats.get("level"), 1),
    )

    earned = set()

    if completed_classes >= 1:
        earned.add("first-strike")

    if completed_classes >= 10:
        earned.add("contender")

    if completed_classes >= 50:
        earned.add("pro-fighter")

    if completed_classes >= 100:
        earned.add("century-club")

    if level >= 1:
        earned.add("novice")

    if level >= 2:
        earned.add("amateur")

    if level >= 5:
        earned.add("heavy-hitter")

    if total_rounds >= 100:
        earned.add("ding-ding")

    if max(
        0,
        safe_int(stat_points.get("endurance"), 0),
    ) >= 100:
        earned.add("charged")

    if max(
        0,
        safe_int(stat_points.get("speed"), 0),
    ) >= 100:
        earned.add("speeding-up")

    if max(
        0,
        safe_int(stat_points.get("power"), 0),
    ) >= 100:
        earned.add("feel-the-power")

    if max(
        0,
        safe_int(stat_points.get("defense"), 0),
    ) >= 100:
        earned.add("stand-back")

    if max(
        0,
        safe_int(stat_points.get("footwork"), 0),
    ) >= 100:
        earned.add("jump-around")

    if max(
        0,
        safe_int(stat_points.get("focus"), 0),
    ) >= 100:
        earned.add("good-eye")

    if total_points >= 1000:
        earned.add("thousand-island")

    return earned


def load_persisted_avatar_badges(
    user_id: str | None,
):
    if not user_id:
        return set()

    try:
        res = (
            supabase
            .table("avatar_badge_unlocks")
            .select("badge_id")
            .eq("user_id", user_id)
            .execute()
        )

        return {
            str(row.get("badge_id") or "").strip()
            for row in (res.data or [])
            if str(row.get("badge_id") or "").strip()
            in AVATAR_ACHIEVEMENT_BADGES
        }

    except Exception:
        return set()


def record_avatar_badge_unlock(
    user_id: str | None,
    badge_id: str,
    notify: bool = False,
):
    badge_id = str(
        badge_id or ""
    ).strip().lower()

    meta = AVATAR_ACHIEVEMENT_BADGES.get(
        badge_id
    )

    if not user_id or not meta:
        return False

    try:
        existing = (
            supabase
            .table("avatar_badge_unlocks")
            .select("badge_id")
            .eq("user_id", user_id)
            .eq("badge_id", badge_id)
            .limit(1)
            .execute()
        )

        if existing.data:
            return False

        try:
            inserted = (
                supabase
                .table("avatar_badge_unlocks")
                .insert({
                    "user_id": user_id,
                    "badge_id": badge_id,
                    "unlocked_at": utc_now_iso(),
                })
                .execute()
            )

            if not inserted.data:
                verify = (
                    supabase
                    .table("avatar_badge_unlocks")
                    .select("badge_id")
                    .eq("user_id", user_id)
                    .eq("badge_id", badge_id)
                    .limit(1)
                    .execute()
                )

                if not verify.data:
                    return False

        except Exception:
            # If simultaneous requests race, the primary key means
            # exactly one wins. Do not duplicate the notification.
            verify = (
                supabase
                .table("avatar_badge_unlocks")
                .select("badge_id")
                .eq("user_id", user_id)
                .eq("badge_id", badge_id)
                .limit(1)
                .execute()
            )

            if verify.data:
                return False

            raise

        if notify:
            create_notification(
                user_id,
                None,
                f"badge_unlock:{badge_id}",
                f"Badge unlocked: {re.sub(r'\s+Badge$', '', meta['name'])}",
                (
                    f"{meta['unlock_description']} "
                    "Tap to equip it in Avatar Customization."
                ),
                "avatar_badge",
                None,
            )

        return True

    except Exception as e:
        print(
            "[avatar-badge] unlock failed "
            f"user={user_id} "
            f"badge={badge_id}: {e}"
        )

        return False


def sync_progress_badge_unlocks(
    user_id: str | None,
    stats: dict | None = None,
    notify_new: bool = False,
):
    if not user_id:
        return []

    stats = stats or calculate_user_stats(
        user_id
    )

    earned = badge_ids_from_stats(
        stats
    )

    newly_unlocked = []

    # Dictionary insertion order deliberately determines the order
    # when several achievements unlock at the same moment.
    for badge_id in AVATAR_ACHIEVEMENT_BADGES.keys():
        if badge_id == "huddle-up":
            continue

        if badge_id not in earned:
            continue

        unlocked_now = (
            record_avatar_badge_unlock(
                user_id,
                badge_id,
                notify=notify_new,
            )
        )

        if unlocked_now:
            newly_unlocked.append(
                badge_id
            )

    return newly_unlocked


def get_unlocked_avatar_badges(
    user_id: str | None,
    stats: dict | None = None,
):
    if not user_id:
        return {"corner-badge"}

    stats = (
        stats
        or calculate_user_stats(user_id)
    )

    return (
        {"corner-badge"}
        | badge_ids_from_stats(stats)
        | load_persisted_avatar_badges(user_id)
    )

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

    stat_awards = safe_json_value(session_row.get("stat_awards"), {})
    round_breakdown = safe_json_value(session_row.get("round_breakdown"), [])
    stat_total_points = safe_int(session_row.get("stat_total_points"), sum(safe_int(v, 0) for v in (stat_awards or {}).values()))
    round_count = safe_int(session_row.get("round_count"), len(round_breakdown) if isinstance(round_breakdown, list) else 0)

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
        "stat_awards": stat_awards if isinstance(stat_awards, dict) else {},
        "stat_total_points": stat_total_points,
        "round_breakdown": round_breakdown if isinstance(round_breakdown, list) else [],
        "round_count": round_count,
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
            .select("id,job_id,user_id,is_public,class_mode,status,difficulty,length_min,pace,music,listen_required_seconds,listen_progress_seconds,listened_complete,listened_complete_at,storage_path,round_breakdown,stat_awards,stat_total_points,round_count")
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
        badge_unlocks = []

        if (
            not already_complete
            and new_progress >= required_seconds
        ):
            # Capture historical achievements before counting
            # this new completion. Existing achievements are
            # silently backfilled instead of re-announced.
            stats_before = calculate_user_stats(uid)

            sync_progress_badge_unlocks(
                uid,
                stats_before,
                notify_new=False,
            )

            completed_now = True

            update_payload["listened_complete"] = True
            update_payload["listened_complete_at"] = utc_now_iso()

        (
            supabase
            .table("class_sessions")
            .update(update_payload)
            .eq("id", session_row.get("id"))
            .execute()
        )

        if completed_now:
            stats_after = calculate_user_stats(uid)

            badge_unlocks = sync_progress_badge_unlocks(
                uid,
                stats_after,
                notify_new=True,
            )

        post = None
        post_error = None

        if already_complete or completed_now:
            refreshed = (
                supabase.table("class_sessions")
                .select("id,job_id,user_id,is_public,class_mode,status,difficulty,length_min,pace,music,listen_required_seconds,listen_progress_seconds,listened_complete,listened_complete_at,storage_path,round_breakdown,stat_awards,stat_total_points,round_count")
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
            "badge_unlocks": badge_unlocks,
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
            gym_post_cutoff = (
                purge_expired_gym_posts(
                    gym["id"]
                )
            )

            gym_posts_res = (
                supabase.table("gym_posts")
                .select("*")
                .eq("gym_id", gym["id"])
                .gte(
                    "created_at",
                    gym_post_cutoff,
                )
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
            .limit(30)
            .execute()
        )

        rows = res.data or []

        actor_ids = [str(row.get("actor_id")) for row in rows if row.get("actor_id")]
        profiles_by_id = load_profiles_map(actor_ids)

        pending_request_by_actor = {}
        try:
            req_res = (
                supabase.table("follow_requests")
                .select("id,requester_id")
                .eq("target_id", uid)
                .eq("status", "pending")
                .execute()
            )

            for req in (req_res.data or []):
                requester_id = str(req.get("requester_id") or "")
                if requester_id:
                    pending_request_by_actor[requester_id] = req.get("id")
        except Exception:
            pending_request_by_actor = {}

        items = []
        for row in rows:
            actor_id = str(row.get("actor_id") or "")
            actor = profiles_by_id.get(actor_id)
            type_ = row.get("type") or "notification"
            follow_request_id = pending_request_by_actor.get(actor_id) if type_ == "follow_request" else None

            items.append({
                "id": row.get("id"),
                "type": type_,
                "title": row.get("title") or "Notification",
                "body": row.get("body") or row.get("message") or "",
                "read": bool(row.get("read") or row.get("is_read") or False),
                "created_at": row.get("created_at"),
                "actor": actor,
                "entity_type": row.get("entity_type"),
                "entity_id": row.get("entity_id"),
                "follow_request_id": follow_request_id,
                "can_delete": not bool(type_ == "follow_request" and follow_request_id),
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

@app.route("/home/notifications/<notification_id>", methods=["DELETE"])
def delete_notification(notification_id):
    uid, err = require_user_id()
    if err:
        return err

    notification_id = str(notification_id or "").strip()

    try:
        existing = (
            supabase.table("notifications")
            .select("*")
            .eq("id", notification_id)
            .eq("user_id", uid)
            .limit(1)
            .execute()
        )

        row = existing.data[0] if existing.data else None

        if not row:
            return jsonify({
                "status": "ok",
                "notification_id": notification_id,
                "deleted": True,
            }), 200

        type_ = str(row.get("type") or "").strip().lower()
        actor_id = str(row.get("actor_id") or "").strip()

        if type_ == "follow_request" and actor_id:
            pending = (
                supabase.table("follow_requests")
                .select("id")
                .eq("requester_id", actor_id)
                .eq("target_id", uid)
                .eq("status", "pending")
                .limit(1)
                .execute()
            )

            if pending.data:
                return jsonify({
                    "status": "blocked",
                    "error": "Respond to this follow request before clearing the notification.",
                }), 409

        supabase.table("notifications").delete().eq("id", notification_id).eq("user_id", uid).execute()

        return jsonify({
            "status": "ok",
            "notification_id": notification_id,
            "deleted": True,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not delete notification.",
            "details": str(e),
        }), 500

@app.route("/home/suggestions", methods=["GET"])
def home_suggestions():
    uid, err = require_user_id()
    if err:
        return err

    try:
        following_ids = load_following_ids(uid)
        requested_ids = load_pending_follow_request_ids(uid)

        res = (
            supabase.table("profiles")
            .select("id,username,display_name,bio,avatar_url,account_privacy,avatar_config,avatar_version,avatar_updated_at,banner_config")
            .neq("id", uid)
            .limit(30)
            .execute()
        )

        following_items = []
        requested_items = []
        suggestion_items = []

        for row in (res.data or []):
            profile_id = str(row.get("id") or "")
            if not profile_id:
                continue

            already_following = profile_id in following_ids
            requested = profile_id in requested_ids

            profile = normalize_profile(row)
            profile["following"] = already_following
            profile["follow_requested"] = requested
            profile["is_self"] = False

            item = {
                "profile": profile,
                "reason": "Following" if already_following else ("Requested" if requested else "Corner athlete"),
                "already_following": already_following,
                "requested": requested,
            }

            if already_following:
                following_items.append(item)
            elif requested:
                requested_items.append(item)
            else:
                suggestion_items.append(item)

        items = following_items[:12] + requested_items[:12] + suggestion_items[:12]

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

            try:
                supabase.table("follow_requests").delete().eq("requester_id", uid).eq("target_id", target_user_id).eq("status", "pending").execute()
            except Exception:
                pass

            try:
                supabase.table("notifications").delete().eq("user_id", target_user_id).eq("actor_id", uid).eq("type", "follow_request").execute()
            except Exception:
                pass

            return jsonify({
                "status": "ok",
                "following": False,
                "requested": False,
                "target_user_id": target_user_id,
            }), 200

        target_profile = ensure_profile_row(target_user_id)
        if not target_profile:
            return jsonify({
                "status": "error",
                "error": "Profile not found.",
            }), 404

        existing = (
            supabase.table("follows")
            .select("follower_id,following_id")
            .eq("follower_id", uid)
            .eq("following_id", target_user_id)
            .limit(1)
            .execute()
        )

        if existing.data:
            return jsonify({
                "status": "ok",
                "following": True,
                "requested": False,
                "target_user_id": target_user_id,
            }), 200

        actor_profile = ensure_profile_row(uid)
        actor_name = profile_display_name(actor_profile)
        target_privacy = str(target_profile.get("account_privacy") or "public").lower()

        if target_privacy == "private":
            pending = get_pending_follow_request(uid, target_user_id)

            if not pending:
                req_res = supabase.table("follow_requests").insert({
                    "requester_id": uid,
                    "target_id": target_user_id,
                    "status": "pending",
                    "created_at": utc_now_iso(),
                    "updated_at": utc_now_iso(),
                }).execute()

                req_err = supa_err(req_res)
                if req_err:
                    return jsonify({
                        "status": "error",
                        "error": "Follow request failed.",
                        "details": req_err,
                    }), 500

                create_notification(
                    target_user_id,
                    uid,
                    "follow_request",
                    f"{actor_name} requested to follow you.",
                    "Approve or decline this request directly from your notifications.",
                    "profile",
                    uid,
                )

            return jsonify({
                "status": "ok",
                "following": False,
                "requested": True,
                "target_user_id": target_user_id,
            }), 200

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

        try:
            supabase.table("follow_requests").delete().eq("requester_id", uid).eq("target_id", target_user_id).execute()
        except Exception:
            pass

        try:
            supabase.table("notifications").delete().eq("user_id", target_user_id).eq("actor_id", uid).eq("type", "follow_request").execute()
        except Exception:
            pass

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
            "requested": False,
            "target_user_id": target_user_id,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not update follow state.",
            "details": str(e),
        }), 500


@app.route("/follow-requests", methods=["GET"])
def my_follow_requests():
    uid, err = require_user_id()
    if err:
        return err

    try:
        req_res = (
            supabase.table("follow_requests")
            .select("*")
            .eq("target_id", uid)
            .eq("status", "pending")
            .order("created_at", desc=True)
            .limit(30)
            .execute()
        )

        rows = req_res.data or []
        requester_ids = [str(row.get("requester_id")) for row in rows if row.get("requester_id")]
        profiles_by_id = load_profiles_map(requester_ids)

        items = []
        for row in rows:
            requester_id = str(row.get("requester_id") or "")
            profile = profiles_by_id.get(requester_id) or normalize_profile({"id": requester_id})
            profile["following"] = requester_id in load_following_ids(uid)
            profile["is_self"] = False

            items.append({
                "id": row.get("id"),
                "created_at": row.get("created_at"),
                "profile": profile,
                "reason": "Requested to follow you",
            })

        return jsonify({
            "status": "ok",
            "items": items,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not load follow requests.",
            "details": str(e),
        }), 500


@app.route("/follow-requests/<request_id>/accept", methods=["POST"])
def accept_follow_request(request_id):
    uid, err = require_user_id()
    if err:
        return err

    request_id = str(request_id or "").strip()

    try:
        req_res = (
            supabase.table("follow_requests")
            .select("*")
            .eq("id", request_id)
            .eq("target_id", uid)
            .eq("status", "pending")
            .limit(1)
            .execute()
        )

        if not req_res.data:
            return jsonify({
                "status": "error",
                "error": "Follow request not found.",
            }), 404

        row = req_res.data[0]
        requester_id = str(row.get("requester_id") or "")

        existing = (
            supabase.table("follows")
            .select("follower_id,following_id")
            .eq("follower_id", requester_id)
            .eq("following_id", uid)
            .limit(1)
            .execute()
        )

        if not existing.data:
            supabase.table("follows").insert({
                "follower_id": requester_id,
                "following_id": uid,
            }).execute()

        supabase.table("follow_requests").update({
            "status": "accepted",
            "updated_at": utc_now_iso(),
        }).eq("id", request_id).execute()

        try:
            supabase.table("notifications").delete().eq("user_id", uid).eq("actor_id", requester_id).eq("type", "follow_request").execute()
        except Exception:
            pass

        target_profile = ensure_profile_row(uid)
        target_name = profile_display_name(target_profile)

        create_notification(
            requester_id,
            uid,
            "follow_request_accepted",
            f"{target_name} approved your follow request.",
            "You can now view their followers-only class posts.",
            "profile",
            uid,
        )

        return jsonify({
            "status": "ok",
            "accepted": True,
            "request_id": request_id,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not accept follow request.",
            "details": str(e),
        }), 500


@app.route("/follow-requests/<request_id>/decline", methods=["POST"])
def decline_follow_request(request_id):
    uid, err = require_user_id()
    if err:
        return err

    request_id = str(request_id or "").strip()

    try:
        req_res = (
            supabase.table("follow_requests")
            .select("*")
            .eq("id", request_id)
            .eq("target_id", uid)
            .eq("status", "pending")
            .limit(1)
            .execute()
        )

        if not req_res.data:
            return jsonify({
                "status": "error",
                "error": "Follow request not found.",
            }), 404

        row = req_res.data[0]
        requester_id = str(row.get("requester_id") or "")

        supabase.table("follow_requests").update({
            "status": "declined",
            "updated_at": utc_now_iso(),
        }).eq("id", request_id).execute()

        try:
            supabase.table("notifications").delete().eq("user_id", uid).eq("actor_id", requester_id).eq("type", "follow_request").execute()
        except Exception:
            pass

        return jsonify({
            "status": "ok",
            "declined": True,
            "request_id": request_id,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not decline follow request.",
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
            .select("id,username,display_name,bio,avatar_url,account_privacy,avatar_config,avatar_version,avatar_updated_at,banner_config")
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
            .limit(30)
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


@app.route("/session-post/<post_id>", methods=["GET", "PATCH", "DELETE"])
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

    if request.method == "GET":
        if not can_view_post(row, uid):
            return jsonify({
                "status": "error",
                "error": "Forbidden.",
            }), 403

        author_id = str(row.get("user_id") or "")
        profiles_by_id = load_profiles_map([author_id])

        return jsonify({
            "status": "ok",
            "post": safe_session_post(row, profiles_by_id, viewer_id=uid),
        }), 200

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


def build_comment_tree(comments: list[dict], profiles_by_id: dict, viewer_id: str | None):
    by_parent = {}

    for comment in comments or []:
        parent_id = str(comment.get("parent_comment_id") or "")
        by_parent.setdefault(parent_id, []).append(comment)

    for key in by_parent:
        by_parent[key].sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)

    def serialize(row):
        item = safe_comment(row, profiles_by_id, viewer_id)
        comment_id = str(row.get("id") or "")
        item["replies"] = [serialize(child) for child in by_parent.get(comment_id, [])]
        return item

    return [serialize(row) for row in by_parent.get("", [])]


def collect_comment_descendant_ids(comment_id: str):
    found = []

    def walk(parent_id):
        try:
            child_res = (
                supabase.table("post_comments")
                .select("id")
                .eq("parent_comment_id", parent_id)
                .execute()
            )
            for child in (child_res.data or []):
                child_id = str(child.get("id") or "")
                if child_id and child_id not in found:
                    found.append(child_id)
                    walk(child_id)
        except Exception:
            return

    walk(comment_id)
    return found

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
        parent_comment_id = str(data.get("parent_comment_id") or "").strip() or None

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

        parent_comment = None

        if parent_comment_id:
            parent_res = (
                supabase.table("post_comments")
                .select("*")
                .eq("id", parent_comment_id)
                .eq("post_id", post_id)
                .limit(1)
                .execute()
            )

            if not parent_res.data:
                return jsonify({
                    "status": "error",
                    "error": "Parent comment not found.",
                }), 404

            parent_comment = parent_res.data[0]

        try:
            payload = {
                "post_id": post_id,
                "user_id": uid,
                "body": body,
            }

            if parent_comment_id:
                payload["parent_comment_id"] = parent_comment_id

            insert_res = supabase.table("post_comments").insert(payload).execute()

            update_post_counts(post_id)

            owner_id = str(row.get("user_id") or "")
            actor_profile = ensure_profile_row(uid)
            actor_name = profile_display_name(actor_profile)

            if parent_comment:
                parent_author_id = str(parent_comment.get("user_id") or "")

                create_notification(
                    parent_author_id,
                    uid,
                    "comment_reply",
                    f"{actor_name} replied to your comment.",
                    body,
                    "session_post",
                    post_id,
                )

                if owner_id and owner_id != uid and owner_id != parent_author_id:
                    create_notification(
                        owner_id,
                        uid,
                        "comment",
                        f"{actor_name} replied on your session.",
                        body,
                        "session_post",
                        post_id,
                    )
            else:
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
            .order("created_at", desc=True)
            .limit(200)
            .execute()
        )

        comments = comments_res.data or []
        author_ids = [str(c.get("user_id")) for c in comments if c.get("user_id")]
        profiles_by_id = load_profiles_map(author_ids)

        return jsonify({
            "status": "ok",
            "items": build_comment_tree(comments, profiles_by_id, uid),
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

        delete_ids = collect_comment_descendant_ids(comment_id)
        delete_ids.append(comment_id)

        for cid in delete_ids:
            supabase.table("post_comments").delete().eq("id", cid).execute()

        update_post_counts(post_id)

        return jsonify({
            "status": "ok",
            "deleted": True,
            "comment_id": comment_id,
            "deleted_ids": delete_ids,
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
        "parent_comment_id": row.get("parent_comment_id"),
        "body": row.get("body") or "",
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "author": author,
        "is_owner": bool(viewer_id and author_id and viewer_id == author_id),
        "replies": [],
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


def gym_post_cutoff_iso():
    return (
        datetime.now(timezone.utc)
        - timedelta(days=GYM_POST_RETENTION_DAYS)
    ).isoformat()


def purge_expired_gym_posts(
    gym_id: str | None = None,
):
    cutoff = gym_post_cutoff_iso()

    try:
        query = (
            supabase
            .table("gym_posts")
            .delete()
            .lt("created_at", cutoff)
        )

        if gym_id:
            query = query.eq(
                "gym_id",
                gym_id,
            )

        query.execute()

    except Exception as e:
        print(
            "[gym-retention] purge failed "
            f"gym={gym_id or 'all'}: {e}"
        )

    return cutoff

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
    cutoff = purge_expired_gym_posts(
        gym_id
    )

    posts_res = (
        supabase.table("gym_posts")
        .select("*")
        .eq("gym_id", gym_id)
        .gte("created_at", cutoff)
        .order("created_at", desc=True)
        .limit(30)
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
    banner_config = normalize_banner_config(
        data.get("banner_config"),
        DEFAULT_GYM_BANNER_CONFIG,
    )
    badge_config = normalize_gym_badge_config(
        data.get("badge_config")
    )

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
            "badge_config": badge_config,
            "banner_config": banner_config,
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

        record_avatar_badge_unlock(
            uid,
            "huddle-up",
            notify=True,
        )

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
    banner_config = normalize_banner_config(
        data.get("banner_config"),
        DEFAULT_GYM_BANNER_CONFIG,
    )
    badge_config = normalize_gym_badge_config(
        data.get("badge_config")
    )

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
                "banner_config": banner_config,
                "badge_config": badge_config,
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

        record_avatar_badge_unlock(
            uid,
            "huddle-up",
            notify=True,
        )

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

@app.route("/gyms/<gym_id>/members/<member_user_id>", methods=["DELETE"])
def remove_gym_member(gym_id, member_user_id):
    uid, err = require_user_id()
    if err:
        return err

    gym_id = str(gym_id or "").strip()
    member_user_id = str(member_user_id or "").strip()
    role = get_gym_role(gym_id, uid)

    if role not in ("owner", "admin"):
        return jsonify({
            "status": "error",
            "error": "Only a gym owner or admin can remove members.",
        }), 403

    if member_user_id == uid:
        return jsonify({
            "status": "error",
            "error": "Use Leave Gym instead of removing yourself.",
        }), 400

    target_role = get_gym_role(gym_id, member_user_id)

    if target_role == "owner":
        return jsonify({
            "status": "error",
            "error": "The gym owner cannot be removed.",
        }), 403

    if role == "admin" and target_role == "admin":
        return jsonify({
            "status": "error",
            "error": "Admins cannot remove other admins.",
        }), 403

    try:
        member_profile = ensure_profile_row(member_user_id)
        member_name = profile_display_name(member_profile)

        supabase.table("gym_members").delete().eq("gym_id", gym_id).eq("user_id", member_user_id).execute()
        remaining = refresh_gym_member_count(gym_id)

        supabase.table("gym_posts").insert({
            "gym_id": gym_id,
            "user_id": uid,
            "kind": "system",
            "body": f"{member_name} was removed from the gym.",
        }).execute()

        create_notification(
            member_user_id,
            uid,
            "gym_removed",
            "You were removed from a gym.",
            "A gym owner or admin removed you from the gym.",
            "gym",
            gym_id,
        )

        return jsonify({
            "status": "ok",
            "removed": True,
            "member_user_id": member_user_id,
            "member_count": remaining,
            "members": load_gym_members(gym_id),
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": "Could not remove gym member.",
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

        record_avatar_badge_unlock(
            uid,
            "huddle-up",
            notify=True,
        )

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
        requested_ids = load_pending_follow_request_ids(uid)

        res = (
            supabase.table("profiles")
            .select("id,username,display_name,bio,avatar_url,account_privacy,avatar_config,avatar_version,avatar_updated_at,banner_config")
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

            already_following = profile_id in following_ids
            requested = profile_id in requested_ids

            profile = normalize_profile(row)
            profile["following"] = already_following
            profile["follow_requested"] = requested
            profile["is_self"] = False

            matches.append({
                "profile": profile,
                "reason": "Search result" if q else ("Requested" if requested else "Corner athlete"),
                "already_following": already_following,
                "requested": requested,
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
            .select("id,username,display_name,bio,avatar_url,account_privacy,avatar_config,avatar_version,avatar_updated_at,banner_config,created_at,plan_tier")
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
            .order("created_at", desc=True)
            .limit(80)
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
            .order("created_at", desc=True)
            .limit(80)
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
            .select("id,username,display_name,bio,avatar_url,account_privacy,avatar_config,avatar_version,avatar_updated_at,banner_config,created_at,plan_tier")
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
