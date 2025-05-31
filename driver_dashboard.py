from flask import Flask, render_template, jsonify
import json
import os

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/locations")
def get_locations():
    try:
        with open("driver_states.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            result = [
                {
                    "driver_id": driver_id,
                    "lat": state["latitude"],
                    "lon": state["longitude"]
                }
                for driver_id, state in data.items()
            ]
            return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    print("Flask server is running. Visit http://localhost:5000")
    app.run(debug=False, use_reloader=False, host="0.0.0.0", port=5000)
