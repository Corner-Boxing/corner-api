import os
from datetime import datetime

from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client, Client

# --------------------------
# Config: Supabase
# --------------------------

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://lbhmfkmrluoropzfleaa.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", 
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxiaG1m"
    "a21ybHVvcm9wemZsZWFhIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MzIyMjAyOSwi"
    "ZXhwIjoyMDc4Nzk4MDI5fQ.Bmqu3Y9Woe4JPVO9bNviXN9ePJWc0LeIsItLjUT2mgQ"
)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --------------------------
# Flask app
# --------------------------

app = Flask(__name__)
CORS(app)


def safe_int(value, default):
    try:
        return int(value)
    except Exception:
        return default


# --------------------------
# Routes
# --------------------------

@app.route("/")
def health():
    return "Corner API OK"


@app.route("/generate", methods=["POST"])
def generate():
    """
    1) Validate input
    2) Insert job into Supabase `jobs` table
       - status = 'queued'
       - plan = jsonb with all config
    3) Return job_id + status
    """
    data = request.get_json() or {}

    difficulty = (data.get("difficulty") or "beginner").lower()
    length_min = safe_int(data.get("length"), 60)
    pace = data.get("pace") or "Normal"
    music = data.get("music") or "None"

    plan = {
        "difficulty": difficulty,
        "length_min": length_min,
        "pace": pace,
        "music": music,
    }

    try:
        result = (
            supabase
            .table("jobs")
            .insert({"status": "queued", "plan": plan})
            .execute()
        )

        # supabase-py v1 style: result.data is list of rows
        if not result.data or not isinstance(result.data, list):
            return jsonify({
                "status": "error",
                "error_message": "Insert returned no rows"
            }), 500

        row = result.data[0]
        job_id = row.get("id")

        if not job_id:
            return jsonify({
                "status": "error",
                "error_message": "Job inserted but no id returned"
            }), 500

        return jsonify({
            "status": "queued",
            "job_id": str(job_id),
            "plan": plan
        }), 202

    except Exception as e:
        return jsonify({
            "status": "error",
            "error_message": str(e),
            "error_type": type(e).__name__,
        }), 500


@app.route("/job-status/<job_id>", methods=["GET"])
def job_status(job_id):
    """
    Return the full job row from Supabase, or a simple not_found.
    """
    try:
        result = (
            supabase
            .table("jobs")
            .select("*")
            .eq("id", job_id)
            .execute()
        )

        if not result.data:
            return jsonify({"status": "not_found"}), 404

        row = result.data[0]

        # Normalize output
        return jsonify({
            "status": row.get("status"),
            "file_url": row.get("file_url"),
            "error": row.get("error"),
            "job": row,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error_message": str(e),
            "error_type": type(e).__name__,
        }), 500


if __name__ == "__main__":
    # Simple dev run for local testing
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
