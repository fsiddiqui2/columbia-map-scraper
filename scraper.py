import re
import json
import requests
import cloudscraper  # <-- Import cloudscraper
from bs4 import BeautifulSoup
import re
from datetime import datetime
import os
import sys
from supabase import create_client, Client

from dotenv import load_dotenv
load_dotenv() 

# --- Supabase Setup ---
try:
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables.")
    
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Successfully connected to Supabase.")

except Exception as e:
    print(f"Error connecting to Supabase: {e}", file=sys.stderr)
    sys.exit(1)


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
    Restaurant("https://dining.columbia.edu/johnnys", "johnnys")
]

# --- Step 1: Load the page
def get_json(url):
    try:
        # Create a scraper instance that can bypass Cloudflare
        scraper = cloudscraper.create_scraper()

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.columbia.edu/',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'cross-site',
            'Cache-Control': 'max-age=0'
        }

        # Use the scraper's .get() method instead of requests.get()
        # It handles the necessary headers and JS challenges automatically.
        r = scraper.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        scripts = soup.find_all("script")
        script_text = ""
        for s in scripts:
            txt = s.text or ""
            if "var dining_nodes" in txt:
                script_text = txt
                break
        
        return script_text
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return ""


# --- Step 2: Extract JS variables
def extract_js_json(var_name, text):
    if not text:
        return None
    match = re.search(rf"var\s+{var_name}\s*=\s*`([\s\S]*?)`", text)
    if not match:
        print(f"⚠️ {var_name} not found")
        return None
    raw = match.group(1)
    clean = raw.encode("utf-8").decode("unicode_escape")
    try:
        return json.loads(clean)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON for {var_name}: {e}", file=sys.stderr)
        return None

# --- Step 3: dining_nodes — Flatten and upload locations and hours
def upload_nodes(nodes, hall_name, db_client):
    locations_to_insert = []
    hours_to_insert = []

    if not (nodes and "locations" in nodes):
        print(f"No location data found for {hall_name}.")
        return

    for loc in nodes["locations"]:
        nid = loc.get("nid")
        title = loc.get("title", "")
        
        locations_to_insert.append({
            "nid": nid,
            "dining_hall_name": hall_name,
            "title": title,
            "building": loc.get("building_name", ""),
            "type": loc.get("type", ""),
            "status": loc.get("status", ""),
            "latitude": loc.get("latitude"),
            "longitude": loc.get("longitude"),
        })

        for h in loc.get("open_hours_fields", []):
            hours_to_insert.append({
                "nid": nid,
                "dining_hall_name": hall_name,
                "title": title,
                "date_from": h.get("date_from"),
                "date_to": h.get("date_to"),
                "displayed_hours": h.get("displayed_hours", ""),
                "excluded": h.get("excluded", False), # Ensure boolean
            })

    try:
        # Clear old data for this hall
        db_client.table('locations').delete().eq('dining_hall_name', hall_name).execute()
        db_client.table('hours').delete().eq('dining_hall_name', hall_name).execute()

        # Insert new data
        if locations_to_insert:
            db_client.table('locations').insert(locations_to_insert).execute()
        if hours_to_insert:
            db_client.table('hours').insert(hours_to_insert).execute()
            
        print(f"✅ Saved {len(locations_to_insert)} locations and {len(hours_to_insert)} open-hour entries for {hall_name}.")

    except Exception as e:
        print(f"Error uploading node/hour data for {hall_name}: {e}", file=sys.stderr)

# --- Step 4: dining_terms — Flatten and upload
def upload_dining_terms(terms, hall_name, db_client):
    terms_to_insert = []
    
    if not isinstance(terms, list):
        print(f"No valid terms data for {hall_name}.")
        return

    for t in terms:
        terms_to_insert.append({
            "nid": t.get("nid"),
            "dining_hall_name": hall_name,
            "title": t.get("title"),
            "term_start": t.get("term_start"),
            "term_end": t.get("term_end"),
            "locations": t.get("locations", []), # Pass as list for text[]
            "stations": t.get("stations", []),   # Pass as list for text[]
        })
    
    try:
        # Clear old data
        db_client.table('terms').delete().eq('dining_hall_name', hall_name).execute()
        
        # Insert new data
        if terms_to_insert:
            db_client.table('terms').insert(terms_to_insert).execute()
        print(f"✅ Saved {len(terms_to_insert)} dining term entries for {hall_name}.")
    
    except Exception as e:
        print(f"Error uploading terms data for {hall_name}: {e}", file=sys.stderr)

# --- Step 5: menu_data — Flatten and upload
def upload_menu(menus, hall_name, db_client):
    menu_items_to_insert = []
    dates_processed = set()
    
    if not isinstance(menus, list):
        print(f"⚠️ menus for {hall_name} is not a list, skipping.")
        return

    weekday_names = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    weekday_regex = re.compile(r'\b(' + '|'.join(weekday_names) + r')\b', re.IGNORECASE)
    monthname_re = re.compile(r'([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})')
    numeric_re = re.compile(r'(\d{1,2})[\\/]+(\d{1,2})[\\/]+(\d{2,4})')

    for entry in menus:
        date_title = entry.get("title", "") or ""
        day_of_week = ""
        date_iso = None # Use None to indicate not set

        # 1) try to find weekday name
        m_w = weekday_regex.search(date_title)
        if m_w:
            day_of_week = m_w.group(1).strip()

        # 2) try month-name date first
        m1 = monthname_re.search(date_title)
        if m1:
            month_name, day_num, year_num = m1.groups()
            try:
                dt = datetime.strptime(f"{month_name} {day_num} {year_num}", "%B %d %Y")
                date_iso = dt.strftime("%Y-%m-%d")
            except ValueError:
                try:
                    dt = datetime.strptime(f"{month_name} {day_num} {year_num}", "%b %d %Y")
                    date_iso = dt.strftime("%Y-%m-%d")
                except Exception:
                    pass # date_iso remains None
        else:
            # 3) try numeric date patterns (handles escaped slashes)
            m2 = numeric_re.search(date_title)
            if m2:
                mm, dd, yy = m2.groups()
                year = 2000 + int(yy) if len(yy) == 2 else int(yy)
                try:
                    dt = datetime(year, int(mm), int(dd))
                    date_iso = dt.strftime("%Y-%m-%d")
                except Exception:
                    pass # date_iso remains None

        # If day_of_week still empty but date_iso set, infer weekday from date
        if not day_of_week and date_iso:
            try:
                dt = datetime.strptime(date_iso, "%Y-%m-%d")
                day_of_week = dt.strftime("%A")
            except Exception:
                pass

        # Add to set of dates to delete (only if date was parsed)
        if date_iso:
            dates_processed.add(date_iso)

        for drf in entry.get("date_range_fields", []):
            for st in drf.get("stations", []):
                for meal in st.get("meals_paragraph", []):
                    menu_items_to_insert.append({
                        "dining_hall_name": hall_name,
                        "menu_title": date_title,
                        "day_of_week": day_of_week,
                        "date": date_iso, # Will be None if unparsed
                        "station_id": (st.get("station", [""])[0] if st.get("station") else ""),
                        "menu_type": ",".join(drf.get("menu_type", [])), # Keep as string
                        "date_from": drf.get("date_from"),
                        "date_to": drf.get("date_to"),
                        "meal_title": meal.get("title", "").strip(),
                        "allergens": meal.get("allergens", []), # Pass as list for text[]
                        "prefs": meal.get("prefs", []),         # Pass as list for text[]
                    })

    # Now, perform database operations
    try:
        # 1. Delete all menu items for this hall for the dates we just processed
        if dates_processed:
            db_client.table('menu_items').delete().eq('dining_hall_name', hall_name).in_('date', list(dates_processed)).execute()
            print(f"Cleared old menu items for {hall_name} on dates: {dates_processed}")

        # 2. Insert all new menu items
        if menu_items_to_insert:
            db_client.table('menu_items').insert(menu_items_to_insert).execute()
        
        print(f"✅ Saved {len(menu_items_to_insert)} menu items for {hall_name}.")

    except Exception as e:
        print(f"Error uploading menu data for {hall_name}: {e}", file=sys.stderr)

# --- Main Execution ---
def main():
    print(f"Starting scrape job at {datetime.now()}...")
    
    for hall in dining_halls:
        print(f"\n--- Processing: {hall.name} ---")
        script_text = get_json(hall.link)
        
        if not script_text:
            print(f"Skipping {hall.name}, no script data found.")
            continue

        nodes = extract_js_json("dining_nodes", script_text)
        terms = extract_js_json("dining_terms", script_text)
        menus = extract_js_json("menu_data", script_text)

        if nodes:
            upload_nodes(nodes, hall.name, supabase)
        else:
            print(f"⚠️ No dining_nodes found for {hall.name}")

        if terms:
            upload_dining_terms(terms, hall.name, supabase)
        else:
            print(f"⚠️ No dining_terms found for {hall.name}")

        if menus:
            upload_menu(menus, hall.name, supabase)
        else:
            print(f"⚠️ No menu_data found for {hall.name}")
    
    print("\nScrape job finished.")

if __name__ == "__main__":
    main()