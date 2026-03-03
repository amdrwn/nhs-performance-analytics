import pandas as pd
import os
import glob
import zipfile
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_FOLDER = os.path.join(BASE_DIR, "raw", "workforce", "monthly")
OUTPUT_FILE = os.path.join(BASE_DIR, "clean", "workforce_clean.csv")

NHS_TRUST_PATTERN = re.compile(r'^[A-Z][A-Z0-9]{2}$')

EXCLUDE_PREFIXES = ('Q',)  

frames = []
all_zips = glob.glob(os.path.join(RAW_FOLDER, "*.zip"))
print(f"Found {len(all_zips)} ZIP files")

for zippath in all_zips:
    try:
        with zipfile.ZipFile(zippath, 'r') as z:
            target = [f for f in z.namelist() if 'Staff Group and Organisation' in f and f.endswith('.csv')]
            if not target:
                print(f"No org file in {os.path.basename(zippath)}")
                continue
            with z.open(target[0]) as f:
                df = pd.read_csv(f, dtype=str, encoding='utf-8-sig')

        df.columns = df.columns.str.strip()

        df = df[
            (df['Staff Group Sort Order'].str.strip() == '01') &
            (df['Data Type'].str.strip() == 'FTE')
        ]

        if len(df) == 0:
            print(f"No matching rows in {os.path.basename(zippath)}")
            continue

        out = pd.DataFrame()
        out['period'] = pd.to_datetime(df['Date'].str.strip(), errors='coerce').dt.to_period('M').dt.to_timestamp()  
        out['org_code'] = df['Org Code'].str.strip().str.upper()
        out['org_name'] = df['Org Name'].str.strip().str.title()
        out['region'] = df['NHSE_Region_Name'].str.strip().str.title()
        out['fte'] = pd.to_numeric(df['Total'].str.strip(), errors='coerce').fillna(0)

        out = out[out['period'].notna()]
        out = out[out['period'] >= '2019-04-01']
        frames.append(out)

    except Exception as e:
        print(f"ERROR {os.path.basename(zippath)}: {e}")

print(f"Read {len(frames)} files")

combined = pd.concat(frames, ignore_index=True)
combined = combined[combined['org_code'].notna()]
combined = combined[combined['org_code'] != 'NAN']

mask_pattern = combined['org_code'].str.match(NHS_TRUST_PATTERN, na=False)

mask_not_icb = ~combined['org_code'].str.startswith(EXCLUDE_PREFIXES, na=False)

before = len(combined)
combined = combined[mask_pattern & mask_not_icb]
after = len(combined)
print(f"Org code filter: {before:,} → {after:,} rows ({before - after:,} non-Trust rows removed)")


REGION_MAP = {
    'South East Of England': 'South East',
    'South West Of England': 'South West',
}
combined['region'] = combined['region'].replace(REGION_MAP)

sample_codes = sorted(combined['org_code'].unique())
print(f"Sample org codes (first 20): {sample_codes[:20]}")
print(f"Total unique Trust codes retained: {len(sample_codes)}")

counts = combined.groupby(['org_code', 'period']).size()
n_dupes = (counts > 1).sum()
print(f"Deduplicating: {n_dupes:,} org/period combinations have >1 row — keeping max FTE")

combined = combined.groupby(['org_code', 'period'], as_index=False).agg(
    org_name=('org_name', 'first'),
    region=('region', 'first'),
    fte=('fte', 'max')
)
print(f"After dedup: {len(combined):,} rows")
print(f"Sanity check — total NHS FTE (last 6 months, should be ~1.3M-1.6M each):")
print(combined.groupby('period')['fte'].sum().tail(6).to_string())

combined = combined.sort_values(['org_code', 'period']).reset_index(drop=True)

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
combined.to_csv(OUTPUT_FILE, index=False)

print(f"\nSaved to {OUTPUT_FILE}")
print(f"Rows: {len(combined):,}")
print(f"Date range: {combined['period'].min()} to {combined['period'].max()}")
print(f"Unique Trusts: {combined['org_code'].nunique()}")
print(f"Unique periods: {combined['period'].nunique()}")
print(f"Regions present: {sorted(combined['region'].dropna().unique())}")