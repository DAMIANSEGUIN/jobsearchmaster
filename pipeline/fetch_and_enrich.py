import os, json, time, requests, pandas as pd
from datetime import datetime
from dateutil import tz

UTC = tz.tzutc()
TODAY = datetime.now(UTC).strftime(%Y-%m-%d)
OUT = f"export/jobsearchmaster_leads_{TODAY}.csv"

REMOTIVE_URL = "https://remotive.com/api/remote-jobs"
REMOTEOK_URL = "https://remoteok.io/api"
TORRE_URL = "https://search.torre.co/opportunities/_search/?size=100"

def load_wimd():
    p = config/wimd_profile.json
    if os.path.exists(p):
        with open(p, r) as f: return json.load(f)
    return {"skills": [], "passions": [], "pivot_paths": []}

def fetch_remotive():
    r = requests.get(REMOTIVE_URL, timeout=30)
    r.raise_for_status()
    return [{"source":"Remotive","title":j.get("title"),"company":j.get("company_name"),
             "location":j.get("candidate_required_location"),"assignment_type":j.get("job_type"),
             "apply_url":j.get("url"),"raw":j} for j in r.json().get("jobs",[])]

def fetch_remoteok():
    r = requests.get(REMOTEOK_URL, headers={"User-Agent":"Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data and isinstance(data, list) and "jobs" in data[0]:
        data = data[0]["jobs"]
    return [{"source":"RemoteOK","title":j.get("position") or j.get("title"),
             "company":j.get("company"),"location":j.get("location") or "Remote",
             "assignment_type":j.get("tags"),"apply_url":j.get("url"),"raw":j} for j in data]

def fetch_torre():
    payload = {"and":[{"remote":{"eq":True}},{"active":{"eq":True}}],"size":50}
    r = requests.post(TORRE_URL, json=payload, timeout=30)
    r.raise_for_status()
    return [{"source":"Torre","title":j.get("objective"),"company":(j.get("organizations") or [{}])[0].get("name"),
             "location":"Remote","assignment_type":j.get("type"),
             "apply_url":f"https://torre.co/jobs/{j.get(id)}","raw":j} for j in r.json().get("results",[])]

def main():
    wimd = load_wimd()
    all_rows = []
    for fn in (fetch_remotive, fetch_remoteok, fetch_torre):
        try:
            all_rows += fn()
            time.sleep(1)
        except Exception as e:
            print(f"Error: {fn.__name__} {e}")
    df = pd.DataFrame(all_rows)
    os.makedirs(export, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"Wrote {OUT} with {len(df)} rows.")

if __name__ == "__main__":
    main()
