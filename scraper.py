import re
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from supabase import create_client, Client
from datetime import datetime
import os

import chromedriver_autoinstaller
chromedriver_autoinstaller.install()


# ---------- CONFIG ----------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
BUCKET_NAME = "dining-data"  # create this in Supabase Storage
UPLOAD_FILENAME = "menus.json"

# ---------- SUPABASE CLIENT ----------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------- RESTAURANT LINKS ----------
class Restaurant:
    def __init__(self, link, name):
        self.link = link
        self.name = name

dining_halls = [
    Restaurant("https://dining.columbia.edu/content/jjs-place-0", "jjs"),
    Restaurant("https://dining.columbia.edu/content/john-jay-dining-hall", "john_jay"),
    Restaurant("https://dining.columbia.edu/content/ferris-booth-commons-0", "ferris"),
    Restaurant("https://dining.columbia.edu/chef-mikes", "mikes"),
    Restaurant("https://dining.columbia.edu/content/chef-dons-pizza-pi-ft-blue-java", "dons"),
    Restaurant("https://dining.columbia.edu/content/grace-dodge-dining-hall-0", "grace_dodge"),
    Restaurant("https://dining.columbia.edu/content/fac-shack-0", "fac_shack"),
    Restaurant("https://dining.columbia.edu/content/faculty-house-0", "fac_house"),
    Restaurant("https://dining.columbia.edu/johnnys", "johnnys"),
]

# ---------- SELENIUM SETUP ----------
# def get_json(url):
#     chrome_options = Options()
#     chrome_options.add_argument("--headless")
#     chrome_options.add_argument("--no-sandbox")
#     chrome_options.add_argument("--disable-dev-shm-usage")
#     driver = webdriver.Chrome(options=chrome_options)

#     driver.get(url)
#     scripts = driver.find_elements("tag name", "script")
#     script_text = ""
#     for s in scripts:
#         txt = s.get_attribute("innerHTML")
#         if "var dining_nodes" in txt:
#             script_text = txt
#             break

#     driver.quit()
#     return script_text
def get_json(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # use new headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    scripts = driver.find_elements("tag name", "script")
    script_text = ""
    for s in scripts:
        txt = s.get_attribute("innerHTML")
        if "var dining_nodes" in txt:
            script_text = txt
            break
    driver.quit()
    return script_text


# ---------- PARSING HELPERS ----------
def extract_js_json(var_name, text):
    match = re.search(rf"var\s+{var_name}\s*=\s*`([\s\S]*?)`", text)
    if not match:
        print(f"⚠️ {var_name} not found")
        return None
    raw = match.group(1)
    clean = raw.encode("utf-8").decode("unicode_escape")
    return json.loads(clean)

def flatten_nodes(nodes):
    node_rows, hour_rows = [], []
    if nodes and "locations" in nodes:
        for loc in nodes["locations"]:
            nid = loc.get("nid")
            title = loc.get("title", "")
            node_rows.append({
                "nid": nid,
                "title": title,
                "building": loc.get("building_name", ""),
                "type": loc.get("type", ""),
                "status": loc.get("status", ""),
                "latitude": loc.get("latitude"),
                "longitude": loc.get("longitude"),
            })

            for h in loc.get("open_hours_fields", []):
                hour_rows.append({
                    "nid": nid,
                    "title": title,
                    "date_from": h.get("date_from"),
                    "date_to": h.get("date_to"),
                    "displayed_hours": h.get("displayed_hours", ""),
                    "excluded": h.get("excluded", ""),
                })
    return {
        "nodes": node_rows,
        "hours": hour_rows,
    }

def flatten_dining_terms(terms):
    term_rows = []
    if isinstance(terms, list):
        for t in terms:
            term_rows.append({
                "nid": t.get("nid"),
                "title": t.get("title"),
                "term_start": t.get("term_start"),
                "term_end": t.get("term_end"),
                "locations": ",".join(t.get("locations", [])),
                "stations": ",".join(t.get("stations", [])),
            })
    return term_rows

def flatten_menu(menus):
    menu_rows = []
    if not isinstance(menus, list):
        return []

    weekday_names = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    weekday_regex = re.compile(r'\b(' + '|'.join(weekday_names) + r')\b', re.IGNORECASE)
    monthname_re = re.compile(r'([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})')
    numeric_re = re.compile(r'(\d{1,2})[\\/]+(\d{1,2})[\\/]+(\d{2,4})')

    for entry in menus:
        date_title = entry.get("title", "") or ""
        day_of_week, date_iso = "", ""

        m_w = weekday_regex.search(date_title)
        if m_w:
            day_of_week = m_w.group(1).strip()

        m1 = monthname_re.search(date_title)
        if m1:
            try:
                dt = datetime.strptime(f"{m1.group(1)} {m1.group(2)} {m1.group(3)}", "%B %d %Y")
                date_iso = dt.strftime("%Y-%m-%d")
            except Exception:
                pass
        else:
            m2 = numeric_re.search(date_title)
            if m2:
                mm, dd, yy = int(m2.group(1)), int(m2.group(2)), m2.group(3)
                year = 2000 + int(yy) if len(yy) == 2 else int(yy)
                try:
                    dt = datetime(year, mm, dd)
                    date_iso = dt.strftime("%Y-%m-%d")
                except Exception:
                    pass

        if not day_of_week and date_iso:
            try:
                dt = datetime.strptime(date_iso, "%Y-%m-%d")
                day_of_week = dt.strftime("%A")
            except Exception:
                pass

        for drf in entry.get("date_range_fields", []):
            date_from, date_to = drf.get("date_from"), drf.get("date_to")
            menu_type = drf.get("menu_type", [])
            for st in drf.get("stations", []):
                station_id = st.get("station", [""])[0] if st.get("station") else ""
                for meal in st.get("meals_paragraph", []):
                    menu_rows.append({
                        "menu_title": date_title,
                        "day_of_week": day_of_week,
                        "date": date_iso,
                        "station_id": station_id,
                        "menu_type": ",".join(menu_type),
                        "date_from": date_from,
                        "date_to": date_to,
                        "meal_title": meal.get("title", "").strip(),
                        "allergens": ",".join(meal.get("allergens", [])) if meal.get("allergens") else "",
                        "prefs": ",".join(meal.get("prefs", [])) if meal.get("prefs") else "",
                    })
    return menu_rows

# ---------- MAIN SCRAPER ----------
def run_scraper():
    all_data = {}

    for hall in dining_halls:
        print(f"🧭 Scraping {hall.name}...")
        script_text = get_json(hall.link)
        if not script_text:
            print(f"⚠️ No script text for {hall.name}")
            continue

        nodes = extract_js_json("dining_nodes", script_text)
        terms = extract_js_json("dining_terms", script_text)
        menus = extract_js_json("menu_data", script_text)

        all_data[hall.name] = {
            "nodes": flatten_nodes(nodes),
            "terms": flatten_dining_terms(terms),
            "menus": flatten_menu(menus),
        }

    # ---------- Upload JSON to Supabase ----------
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = {
        "updated_at": timestamp,
        "data": all_data
    }

    json_bytes = json.dumps(payload, indent=2).encode("utf-8")

    # First, delete existing file if it exists (safe to ignore errors)
    try:
        supabase.storage.from_(BUCKET_NAME).remove([UPLOAD_FILENAME])
    except Exception as e:
        print("⚠️ Could not remove old file (probably doesn’t exist yet):", e)

    # Then upload the new JSON
    supabase.storage.from_(BUCKET_NAME).upload(
        path=UPLOAD_FILENAME,
        file=json_bytes,
        file_options={"content-type": "application/json"},
    )
    print(f"✅ Uploaded to Supabase: {BUCKET_NAME}/{UPLOAD_FILENAME}")

    # supabase.storage.from_(BUCKET_NAME).upload(
    #     path=UPLOAD_FILENAME,
    #     file=json_bytes,
    #     file_options={"content-type": "application/json"},
    #     upsert=True
    # )
    # print(f"✅ Uploaded to Supabase: {BUCKET_NAME}/{UPLOAD_FILENAME}")

if __name__ == "__main__":
    run_scraper()

