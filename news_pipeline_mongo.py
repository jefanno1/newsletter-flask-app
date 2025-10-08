# news_pipeline_mongo.py
import os
import json
import time
import re
import gc
from datetime import datetime
from typing import List, Optional
from dotenv import load_dotenv

from serpapi import GoogleSearch
from openai import OpenAI
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient

# =====================
# CONFIG
# =====================
load_dotenv()

SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = "NewsletterDB"
MONGO_COLLECTION = "news"

TOPIC_TOKEN_BUSINESS = "CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx1YlY4U0FtVnVHZ0pWVXlnQVAB"

HEADLINE_LIMIT = 10
LLM_SELECT_MODEL = "gpt-5-nano"
LLM_SUMMARY_MODEL = "gpt-5-nano"
SUPPORTING_PER_HEADLINE = 5
SCRAPE_WAIT = 3
BAD_LABELS = {"top news", "posts on x", "frequently asked questions"}

# =====================
# UTIL
# =====================
def safe_filename(s: str, maxlen: int = 80) -> str:
    f = re.sub(r'[^A-Za-z0-9_\- ]+', '', s)
    f = f.strip().replace(" ", "_")
    return f[:maxlen] if len(f) > 0 else "untitled"

# =====================
# SerpAPI headline fetcher
# =====================
def fetch_headlines_serpapi(topic_token: str, limit: int = 10):
    params = {
        "engine": "google_news",
        "topic_token": topic_token,
        "hl": "en",
        "gl": "US",
        "api_key": SERPAPI_API_KEY
    }
    try:
        search = GoogleSearch(params)
        results = search.get_dict()
    except Exception as e:
        print("❌ Error fetching headlines:", e)
        return []

    news_results = results.get("news_results", [])[:limit]
    out = []
    for n in news_results:
        hl = n.get("highlight", {}) or {}
        title = (hl.get("title") or n.get("title") or "").strip()
        if not title or any(bad in title.lower() for bad in BAD_LABELS):
            continue
        link = hl.get("link") or n.get("link") or ""
        source = (hl.get("source") or {}).get("name") or (n.get("source") or {}).get("name") or ""
        date = hl.get("date") or n.get("date") or ""
        story_token = hl.get("story_token") or n.get("story_token")
        if not story_token:
            for s in (n.get("stories") or []):
                st = s.get("story_token")
                title_s = (s.get("title") or "").strip().lower()
                if st and title_s and not any(bad in title_s for bad in BAD_LABELS):
                    story_token = st
                    break
        out.append({
            "Title": title,
            "Link": link,
            "Source": source,
            "Published": date,
            "StoryToken": story_token or ""
        })
    return out

# =====================
# LLM helpers
# =====================
def get_openai_client():
    return OpenAI(api_key=OPENAI_API_KEY)

def ask_llm_select_top5(headlines: List[dict]) -> List[int]:
    client = get_openai_client()
    prompt = "Here are 10 headlines. Choose 5 most interesting to a general reader. Answer ONLY a JSON object like {\"selected\": [1,2,3,4,5]} with indices (1-based).\n\n"
    for i, h in enumerate(headlines, start=1):
        prompt += f"{i}. {h['Title']}\n"

    resp = client.chat.completions.create(
        model=LLM_SELECT_MODEL,
        messages=[
            {"role": "system", "content": "You are an assistant that selects the most interesting news headlines."},
            {"role": "user", "content": prompt}
        ]
    )
    print("LLM prompt:\n", prompt)
    raw = resp.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    try:
        sel = json.loads(raw).get("selected", [])
        return [int(x) for x in sel][:5]
    except:
        return list(range(1, min(6, len(headlines)+1)))

def ask_llm_summarize_two_langs(text: str) -> dict:
    client = get_openai_client()
    prompt = (
        "Ringkas teks berikut dalam 2 bahasa (komprehensif, jelas, agak panjang).\n"
        "Output HARUS valid JSON exactly like:\n"
        '{ "id": "Ringkasan Bahasa Indonesia", "en": "English summary" }\n\n'
        "Teks:\n" + text
    )
    resp = client.chat.completions.create(
        model=LLM_SUMMARY_MODEL,
        messages=[
            {"role": "system", "content": "You are an assistant that summarizes news articles into Indonesian and English."},
            {"role": "user", "content": prompt}
        ]
    )
    raw = resp.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    try:
        j = json.loads(raw)
        return {"id": j.get("id", "").strip(), "en": j.get("en", "").strip()}
    except:
        return {"id": raw, "en": ""}

def ask_llm_igpost_from_text(summary_en: str) -> Optional[dict]:
    client = get_openai_client()
    prompt = (
        "Given the following English summary, produce JSON: {\"title\": \"short title (<=10 words)\", \"ig_post\": \"IG post text (one slide)\"}\n\n"
        f"Summary:\n{summary_en}"
    )
    resp = client.chat.completions.create(
        model="gpt-5-nano",
        messages=[
            {"role": "system", "content": "You are a social media copywriter."},
            {"role": "user", "content": prompt}
        ]
    )
    raw = resp.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    try:
        j = json.loads(raw)
        return {"title": j.get("title","").strip(), "ig_post": j.get("ig_post","").strip()}
    except:
        return None

# =====================
# Scraper
# =====================
def make_selenium_driver(headless: bool = True):
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.chrome.options import Options
    from selenium import webdriver

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--remote-debugging-port=9222")
    print("🌐 Starting Selenium Chrome driver...")
    driver = webdriver.Chrome(service=ChromeService(), options=chrome_options)
    print("🌐 Chrome driver started")
    return driver


def scrape_article_text(driver, url: str, wait_seconds: int = SCRAPE_WAIT) -> str:
    try:
        driver.get(url)
        time.sleep(wait_seconds)
        html = driver.page_source[:200000]  # ambil maksimal 200 KB
        soup = BeautifulSoup(html, "html.parser")
        article = soup.find("article") or soup.find(role="main")
        if article:
            paras = [p.get_text().strip() for p in article.find_all("p") if p.get_text().strip()]
            return "\n".join(paras)
        body = soup.body
        if body:
            paras = [p.get_text().strip() for p in body.find_all("p") if p.get_text().strip()]
            return "\n".join(paras)
        return ""
    except Exception as e:
        print("❌ Error loading URL:", url, e)
        return ""

# =====================
# FULL PIPELINE
# =====================
def run_full_pipeline():
    client_mongo = MongoClient(MONGO_URI)
    db = client_mongo[MONGO_DB]
    news_col = db[MONGO_COLLECTION]

    print("🔎 Fetching headlines...")
    headlines = fetch_headlines_serpapi(TOPIC_TOKEN_BUSINESS, limit=HEADLINE_LIMIT)
    if not headlines:
        print("No headlines -> exit")
        return

    print("🤖 Selecting top 5 headlines via LLM...")
    top_idx = ask_llm_select_top5(headlines[:HEADLINE_LIMIT])
    print("🤖 LLM returned top indices:", top_idx)
    selected = [headlines[i-1] for i in top_idx if 1 <= i <= len(headlines)]
    print("Selected:", [h["Title"] for h in selected])

    

    for h in selected:
        driver = make_selenium_driver(headless=True)
        title_safe = safe_filename(h["Title"], 60)
        print(f"\n📂 Processing headline: {h['Title']}")
        token = h.get("StoryToken")
        supporting_articles = []

        if token:
            params = {
                "engine": "google_news",
                "story_token": token,
                "hl": "en",
                "gl": "US",
                "api_key": SERPAPI_API_KEY
            }
            try:
                results = GoogleSearch(params).get_dict()
                news_results = results.get("news_results", [])[:SUPPORTING_PER_HEADLINE]
                links = [nr.get("link") for nr in news_results if nr.get("link")]
            except:
                links = []

            for link in links:
                print(f"   Scraping article: {link}")
                text = scrape_article_text(driver, link)
                print(f"   Done scraping, length={len(text)}")
                if text.strip():
                    supporting_articles.append({"link": link, "text": text})

        combined_text = "\n".join([a["text"] for a in supporting_articles])
        if len(combined_text) > 5000:
            combined_text = combined_text[:5000]
        summaries = ask_llm_summarize_two_langs(combined_text) if combined_text else {"id":"","en":""}
        ig_post = ask_llm_igpost_from_text(summaries.get("en","")) if summaries.get("en") else None

        doc = {
            "title": h.get("Title"),
            "link": h.get("Link"),
            "source": h.get("Source"),
            "published": h.get("Published"),
            "story_token": h.get("StoryToken"),
            "selected_top5": True,
            "supporting_articles": supporting_articles,
            "summaries": summaries,
            "ig_post": ig_post,
            "created_at": datetime.now()
        }
        print(f"💾 Inserting document for headline: {h['Title']}")
        news_col.insert_one(doc)
        print(f"✅ Inserted into MongoDB: {h['Title']}")
        del combined_text
        del summaries
        del ig_post
        del supporting_articles
        driver.quit()
        print("\n🏁 Pipeline finished. All data saved in MongoDB collection:", MONGO_COLLECTION)
        gc.collect()

if __name__ == "__main__":
    run_full_pipeline()
