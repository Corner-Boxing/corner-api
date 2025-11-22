import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client, Client

# -------------------------
# Flask + Supabase
# -------------------------

app = Flask(__name__)
CORS(app)

SUPABASE_URL = "https://lbhmfkmrluoropzfleaa.supabase.co"
SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxiaG1m"
    "a21ybHVvcm9wemZsZWFhIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MzIyMjAyOSwi"
    "ZXhwIjoyMDc4Nzk4MDI5fQ.Bmqu3Y9Woe4JPVO9bNviXN9ePJWc0LeIsItLjUT2mgQ"
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
    This ONLY creates a small job record in Supabase:
      jobs.status = 'queued'
      jobs.plan = {
        difficulty, length_min, pace, music
      }

    The worker backend will later read this, rebuild the full
    class plan, generate audio, and update file_url/status.
    """
    try:
        data = request.get_json() or {}
    except Exception:
        data = {}

    difficulty = (data.get("difficulty") or "beginner").lower()
    length_min = int(data.get("length") or 60)
    pace = data.get("pace") or "Normal"
    music = data.get("music") or "None"

    # SMALL, SAFE PAYLOAD — no "segments" here
    small_plan = {
        "difficulty": difficulty,
        "length_min": length_min,
        "pace": pace,
        "music": music,
    }

    try:
        result = (
            supabase
            .table("jobs")
            .insert({
                "status": "queued",
                "plan": small_plan,
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


if __name__ == "__main__":
    # Render expects port 10000
    app.run(host="0.0.0.0", port=10000)
