import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AE_FILE = os.path.join(BASE_DIR, "clean", "ae_clean.csv")
RTT_FILE = os.path.join(BASE_DIR, "clean", "rtt_clean.csv")
WORKFORCE_FILE = os.path.join(BASE_DIR, "clean", "workforce_clean.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "clean", "reference_clean.csv")

ae = pd.read_csv(AE_FILE, dtype=str, usecols=['org_code', 'org_name', 'region'])
rtt = pd.read_csv(RTT_FILE, dtype=str, usecols=['org_code', 'org_name', 'region'])
wf = pd.read_csv(WORKFORCE_FILE, dtype=str, usecols=['org_code', 'org_name', 'region'])

combined = pd.concat([ae, rtt, wf], ignore_index=True)

combined['org_code'] = combined['org_code'].astype(str).str.strip().str.upper()
combined['org_name'] = combined['org_name'].astype(str).str.strip().str.title()
combined['region'] = combined['region'].astype(str).str.strip().str.title()

combined = combined[combined['org_code'].notna()]
combined = combined[combined['org_code'] != 'Nan']
combined = combined[combined['org_code'] != 'NAN']

ref = (
    combined
    .sort_values('org_name')
    .drop_duplicates(subset=['org_code'], keep='first')
    .reset_index(drop=True)
)

ref = ref[['org_code', 'org_name', 'region']]

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
ref.to_csv(OUTPUT_FILE, index=False)

print(f"Saved to {OUTPUT_FILE}")
print(f"Unique orgs: {len(ref):,}")
print(f"Regions: {sorted(ref['region'].unique())}")