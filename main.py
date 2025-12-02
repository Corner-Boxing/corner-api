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
# Supabase Client (SAFE)
# -------------------------
# These MUST come from Render environment variables:
#
#   SUPABASE_URL
#   SUPABASE_SERVICE_KEY
#
# NEVER hardcode them — Render will lose them on redeploy.

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError(
        "Missing SUPABASE_URL or SUPABASE_SERVICE_KEY. "
        "Make sure they are set in the Render dashboard."
    )

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# -------------------------
# Routes
# -------------------------

@app.route("/")
def home():
    return "Corner API OK"


@app.route("/generate", methods=["POST"])
def generate():
    """
    Creates a job in the Supabase 'jobs' table with:

         status = 'queued'
         plan = {
             difficulty,
             length_min,
             pace,
             music
         }

    The local Corner Worker picks it up and generates audio.
    """

    # ---- Safe JSON Parsing ----
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        return jsonify({"status": "error", "error": "Invalid JSON"}), 400

    # ---- Normalize incoming payload ----
    difficulty = (data.get("difficulty") or "beginner").lower()
    length_min = int(data.get("length") or 30)
    pace = str(data.get("pace") or "Normal")
    music = str(data.get("music") or "None")

    # EXACT structure the worker expects
    plan = {
        "difficulty": difficulty,
        "length_min": length_min,
        "pace": pace,
        "music": music,
    }

    # ---- Insert job row ----
    try:
        result = (
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

        job_id = result.data[0]["id"]
        return jsonify({"status": "queued", "job_id": job_id}), 202

    except Exception as e:
        return jsonify({
            "status": "error",
            "error_message": str(e),
            "error_type": type(e).__name__,
        }), 500


# -------------------------
# Run for Render
# -------------------------

if __name__ == "__main__":
    # Render requires port 10000
    app.run(host="0.0.0.0", port=10000)
