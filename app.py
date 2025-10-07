from flask import Flask, render_template, redirect, url_for
from pymongo import MongoClient
from threading import Thread
from datetime import datetime
import os

# import pipeline function
from news_pipeline_mongo import run_full_pipeline
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# MongoDB setup
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = "NewsletterDB"
MONGO_COLLECTION = "news"

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
news_col = db[MONGO_COLLECTION]

# Global variable to track pipeline status
pipeline_status = {"running": False, "last_run": None}

# Helper to run pipeline in background
def run_pipeline_background():
    global pipeline_status
    pipeline_status["running"] = True
    try:
        run_full_pipeline()
    finally:
        pipeline_status["running"] = False
        pipeline_status["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@app.route("/")
def index():
    # Fetch latest news from MongoDB
    news = list(news_col.find().sort("created_at", -1).limit(20))
    return render_template("index.html", news=news, pipeline_status=pipeline_status)

@app.route("/run_pipeline")
def run_pipeline():
    if not pipeline_status["running"]:
        # Run pipeline in background
        thread = Thread(target=run_pipeline_background)
        thread.start()
    return redirect(url_for("index"))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))  # default 5000 supaya sama dengan docker run
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

