import requests
from bs4 import BeautifulSoup
from config import LOGIN_URL, TARGET_URL
from db import save_registrations

def generate_tags(class_code, class_name):
    tags = []
    code = class_code.upper()
    if code.startswith("GEN"): tags.append("general")
    elif code.startswith("MTH"): tags.append("math")
    elif code.startswith("PHYS"): tags.append("physics")
    elif code.startswith("CMPS"): tags.append("computer")
    elif code.startswith("CHE"): tags.append("chemical")
    elif code.startswith("EPES"): tags.append("electrical")
    else: tags.append("other")
    return ",".join(tags) #to join tags if i add more than one tag per class in the future

def scrape_registration(username, password):
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": LOGIN_URL})

    # --- GET login page ---
    resp = session.get(LOGIN_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    def get_hidden(name):
        tag = soup.find("input", {"name": name})
        return tag.get("value", "") if tag else ""

    # --- POST login ---
    payload = {
        "__EVENTTARGET": "ctl03",
        "__EVENTARGUMENT": "Button1|event|Click",
        "__VIEWSTATE": get_hidden("__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": get_hidden("__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": get_hidden("__EVENTVALIDATION"),
        "txtUsername": username,
        "txtPassword": password,
    }
    login_resp = session.post(LOGIN_URL, data=payload)
    login_resp.raise_for_status()
    dashboard = session.get(TARGET_URL, allow_redirects=True)
    if dashboard.url.rstrip("/") == LOGIN_URL.rstrip("/"):
        raise RuntimeError("❌ Login failed")

    # --- Get registration table ---
    dashboard_resp = session.get(TARGET_URL)
    dashboard_resp.raise_for_status()
    soup = BeautifulSoup(dashboard_resp.text, "html.parser")
    def get_hidden(name):
        tag = soup.find("input", {"name": name})
        return tag.get("value", "") if tag else ""

    payload = {
        "submitDirectEventConfig": (
            '{"config":{"extraParams":{"WindowID":"win_17",'
            '"ControlPath":"~/SIS/Modules/Student/RegistrationStatus/RegistrationStatus.ascx"}}}'
        ),
        "__EVENTTARGET": "ResourceManager1",
        "__EVENTARGUMENT": "-|public|LoadWindowControl",
        "__VIEWSTATE": get_hidden("__VIEWSTATE"),
        "__VIEWSTATEGENERATOR": get_hidden("__VIEWSTATEGENERATOR"),
        "__EVENTVALIDATION": get_hidden("__EVENTVALIDATION"),
    }

    resp = session.post(TARGET_URL, data=payload)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    textarea = soup.find("textarea")
    if textarea: soup = BeautifulSoup(textarea.text, "html.parser")
    table = soup.find("table", {"id": "cont_win_17_GridView1"})
    if not table: raise RuntimeError("❌ Table not found")

    # --- Extract data ---
    header_row = table.find("tr")
    headers = [th.get_text(strip=True) for th in header_row.find_all("th")]
    rows = table.find_all("tr")[1:]
    data = []
    for row in rows:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) != len(headers): continue
        row_dict = dict(zip(headers, cols))
        raw_code = row_dict.get(headers[1])
        class_code = raw_code.strip("_") if raw_code else ""
        class_name = row_dict.get(headers[2])
        if not class_code: continue
        status_raw = row_dict.get(headers[11], "").strip("_").lower()
        is_open = status_raw == "opened"
        class_group = row_dict.get(headers[3])

        if cols[2].startswith(("LEC", "TUT", "LAB")):
            if "-" in cols[3] and "_" in cols[3]:
                class_code = cols[3].split("-", 1)[0].strip()
                class_name = cols[3].split("-", 1)[1].strip().split("_")[0].strip()
                class_group = cols[3].split("-", 1)[1].strip().split("_")[1].split("G.")[1].strip()
            elif "-" in cols[3]:
                class_code = cols[3].split("-", 1)[0].strip()
                class_name = cols[3].split("-", 1)[1].strip()
                class_group = 0
            elif "_" in cols[3]:
                class_code = "UNK"
                class_name = cols[3].split("_", 1)[0].strip()
                class_group = cols[3].split("_", 1)[1].strip().split("G.")[1].strip()
            else:
                class_code = "UNK"
                class_name = cols[3].strip()
                class_group = 0
        else:
            # fallback for non-LEC/TUT rows
            raw_group = row_dict.get(headers[3], "").strip()
            try:
                class_group = int(raw_group)
            except ValueError:
                class_group = 0
        
        to_time = row_dict.get(headers[7])
        #if minutes contain anything other than 0 than add one to hours and set minutes to 00 (only for "To" time)
        to_time = to_time.strip("_") if to_time else ""
        if to_time and ":" in to_time:
            hour, minute = to_time.split(":")
            if minute != "00":
                hour = str(int(hour) + 1)
                to_time = f"{hour}:00"
            else:
                to_time = f"{hour}:00"

        #convert to 24 hour format( if hour is less than 8 than its pm, if its between 8 and 12 its am, if its 12 its pm)
        if to_time and ":" in to_time:
            hour, minute = to_time.split(":")
            hour = int(hour)
            if hour < 8:
                hour += 12
            elif 8 <= hour < 12:
                pass
            elif hour == 12:
                pass
            to_time = f"{hour}:{minute}"


        from_time = row_dict.get(headers[6]).strip("_") if row_dict.get(headers[6]) else ""
        
        if from_time and ":" in from_time:
            hour, minute = from_time.split(":")
            hour = int(hour)
            if hour < 8:
                hour += 12
            elif 8 <= hour < 12:
                pass
            elif hour == 12:
                pass
            from_time = f"{hour}:{minute}"


        data.append({
            "Code": class_code,
            "Name": class_name,
            "Group": class_group,
            "Type": row_dict.get(headers[4]).strip("_") if row_dict.get(headers[4]) else "",
            "Day": row_dict.get(headers[5]),
            "From": from_time,
            "To": to_time,
            "Class Size": row_dict.get(headers[8]).strip("_") if row_dict.get(headers[8]) else "",
            "Enrolled": row_dict.get(headers[9]),
            "Waiting": row_dict.get(headers[10]),
            "Status": is_open,
            "Location": row_dict.get(headers[12]),
            "Tags": generate_tags(class_code, class_name)
        })

    # --- Save to DB ---
    save_registrations(data)
    print("Saved scraped data to postgres database")
