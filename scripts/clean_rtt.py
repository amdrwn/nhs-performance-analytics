import pandas as pd
import os
import glob
import zipfile

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_FOLDER = os.path.join(BASE_DIR, "raw", "rtt")
OUTPUT_FILE = os.path.join(BASE_DIR, "clean", "rtt_clean.csv")

def map_region(region_str):
    if not isinstance(region_str, str):
        return "Unknown"
    r = region_str.upper()
    if "LONDON" in r:
        return "London"
    if ("MIDLANDS" in r or "BIRMINGHAM" in r or "SOLIHULL" in r
            or "NOTTINGHAM" in r or "NOTTINGHAMSHIRE" in r
            or "DERBY" in r or "DERBYSHIRE" in r
            or "COVENTRY" in r or "WARWICKSHIRE" in r
            or "BLACK COUNTRY" in r or "WEST BIRMINGHAM" in r
            or "SHROPSHIRE" in r or "TELFORD" in r or "WREKIN" in r
            or "STAFFORDSHIRE" in r or "STOKE" in r
            or "HEREFORDSHIRE" in r or "WORCESTERSHIRE" in r
            or "NORTHAMPTONSHIRE" in r
            or "LEICESTER" in r or "LEICESTERSHIRE" in r or "RUTLAND" in r
            or "LINCOLNSHIRE" in r):
        return "Midlands"
    if ("NORTH EAST" in r or "YORKSHIRE" in r or "HUMBER" in r
            or "CUMBRIA AND NORTH" in r or "TEES" in r or "DURHAM" in r):
        return "North East & Yorkshire"
    if ("NORTH WEST" in r or "LANCASHIRE" in r or "MERSEYSIDE" in r
            or "MANCHESTER" in r or "CHESHIRE" in r or "CUMBRIA" in r):
        return "North West"
    if ("SOUTH EAST" in r or "KENT" in r or "SURREY" in r or "SUSSEX" in r
            or "HAMPSHIRE" in r or "OXFORDSHIRE" in r or "BERKSHIRE" in r
            or "BUCKINGHAMSHIRE" in r or "FRIMLEY" in r or "THAMES VALLEY" in r
            or "COASTAL" in r):
        return "South East"
    if ("EAST OF ENGLAND" in r or "NORFOLK" in r or "SUFFOLK" in r
            or "CAMBRIDGESHIRE" in r or "HERTFORDSHIRE" in r
            or "BEDFORDSHIRE" in r or "ESSEX" in r or "LUTON" in r
            or "PETERBOROUGH" in r or "HERTS" in r):
        return "East of England"
    if ("SOUTH WEST" in r or "CORNWALL" in r or "DEVON" in r or "DORSET" in r
            or "GLOUCESTER" in r or "SOMERSET" in r or "WILTSHIRE" in r
            or "BRISTOL" in r or "SWINDON" in r or "BATH" in r
            or "AVON" in r):
        return "South West"
    return "Unknown"

def parse_period(period_str):
    months = {
        'JANUARY': 1, 'FEBRUARY': 2, 'MARCH': 3, 'APRIL': 4,
        'MAY': 5, 'JUNE': 6, 'JULY': 7, 'AUGUST': 8,
        'SEPTEMBER': 9, 'OCTOBER': 10, 'NOVEMBER': 11, 'DECEMBER': 12
    }
    try:
        parts = str(period_str).upper().replace('RTT-', '').strip().split('-')
        month_num = months.get(parts[0])
        year = parts[1]
        if month_num and year.isdigit():
            return pd.Timestamp(int(year), month_num, 1)
    except:
        pass
    return pd.NaT

def to_num(series):
    return pd.to_numeric(
        series.astype(str).str.strip().str.replace(',', '').replace(
            {'-': '0', '': '0', 'nan': '0', 'NaN': '0'}),
        errors='coerce'
    ).fillna(0)

def clean_colname(c):
    return (c.strip().lower()
             .replace(' ', '_')
             .replace('-', '_')
             .replace('(', '')
             .replace(')', '')
             .replace('/', '_'))

def find_col(keyword, cols):
    """Find column containing keyword (case-insensitive, searches cleaned names)."""
    keyword_clean = keyword.lower().replace(' ', '_')
    for c in cols:
        if keyword_clean in clean_colname(c):
            return c
    return None

all_zips = glob.glob(os.path.join(RAW_FOLDER, "*.zip"))
print(f"Found {len(all_zips)} ZIP files")

frames = []

for zippath in all_zips:
    try:
        with zipfile.ZipFile(zippath, 'r') as z:
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            if not csv_files:
                print(f"No CSV in {os.path.basename(zippath)}")
                continue

            with z.open(csv_files[0]) as f:
                df = pd.read_csv(f, dtype=str, encoding='utf-8-sig', low_memory=False)

        raw_cols = list(df.columns)

        period_raw      = find_col('period', raw_cols)
        provider_raw    = find_col('provider_org_code', raw_cols)
        prov_name_raw   = find_col('provider_org_name', raw_cols)
        region_raw      = find_col('provider_parent_name', raw_cols)
        rtt_type_raw    = find_col('rtt_part_type', raw_cols)
        tf_raw          = find_col('treatment_function_code', raw_cols)
        total_all_raw   = next((c for c in raw_cols
                                if clean_colname(c) == 'total_all'), None)

        gt52_raw = next((c for c in raw_cols
                         if clean_colname(c) == 'gt_52_weeks_sum_1'), None)

        week_raws = [c for c in raw_cols
                     if 'weeks_sum_1' in clean_colname(c)
                     and 'gt_' in clean_colname(c)]

        missing = [name for name, col in [
            ('Period', period_raw), ('Provider Org Code', provider_raw),
            ('RTT Part Type', rtt_type_raw), ('Treatment Function Code', tf_raw),
            ('Total All', total_all_raw)
        ] if col is None]

        if missing:
            print(f"Missing columns {missing} in {os.path.basename(zippath)}")
            continue

        df = df[df[rtt_type_raw].str.strip().str.upper() == 'PART_2']
        df = df[df[tf_raw].str.strip().str.upper() == 'C_999']

        if len(df) == 0:
            print(f"No Part_2/C_999 rows in {os.path.basename(zippath)}")
            continue

        df['total_waiting_num'] = to_num(df[total_all_raw])

        if gt52_raw:
            df['gt52_num'] = to_num(df[gt52_raw])
        else:
            gt52_cols = [c for c in week_raws
                         if int(clean_colname(c).split('gt_')[1].split('_')[0]) >= 52]
            df['gt52_num'] = df[gt52_cols].apply(to_num).sum(axis=1) if gt52_cols else 0

        within18_cols = [c for c in week_raws
                         if int(clean_colname(c).split('gt_')[1].split('_')[0]) < 18]
        df['within18_num'] = df[within18_cols].apply(to_num).sum(axis=1) if within18_cols else 0

        agg_dict = {
            'total_waiting':        ('total_waiting_num', 'sum'),
            'waiting_gt52wks':      ('gt52_num', 'sum'),
            'waiting_within_18wks': ('within18_num', 'sum'),
        }
        if prov_name_raw:
            agg_dict['org_name'] = (prov_name_raw, 'first')
        if region_raw:
            agg_dict['region'] = (region_raw, 'first')

        agg = df.groupby([period_raw, provider_raw], as_index=False).agg(**agg_dict)

        agg['period']   = agg[period_raw].apply(parse_period)
        agg['org_code'] = agg[provider_raw].str.strip().str.upper()
        agg['org_name'] = agg['org_name'].str.strip().str.title() if 'org_name' in agg.columns else agg['org_code']
        agg['region']   = agg['region'].str.strip().str.title() if 'region' in agg.columns else 'Unknown'
        agg['clean_region'] = agg['region'].apply(map_region)

        agg = agg[['period', 'org_code', 'org_name', 'region', 'clean_region',
                   'total_waiting', 'waiting_gt52wks', 'waiting_within_18wks']]
        agg = agg[agg['period'].notna()]
        frames.append(agg)
        print(f"OK: {os.path.basename(zippath)} — {len(agg)} rows")

    except Exception as e:
        print(f"ERROR {os.path.basename(zippath)}: {e}")

print(f"\nRead {len(frames)} files successfully")

combined = pd.concat(frames, ignore_index=True)
combined = combined[combined['org_code'].notna()]
combined = combined[~combined['org_code'].isin(['NAN', 'nan'])]

combined['pct_within_18wks'] = (
    combined['waiting_within_18wks']
    / combined['total_waiting'].replace(0, float('nan'))
).astype(float).round(4)

combined = combined.sort_values(['org_code', 'period']).reset_index(drop=True)

unknowns = combined[combined['clean_region'] == 'Unknown']
unknown_count = len(unknowns)
unknown_pct = 100 * unknown_count / len(combined)
print(f"\nUnknown region rows: {unknown_count:,} of {len(combined):,} ({unknown_pct:.1f}%)")
if unknown_count > 0:
    top_unknowns = unknowns['region'].value_counts().head(20)
    print("Top unknown region strings:")
    print(top_unknowns.to_string())

    unknowns[['org_code', 'org_name', 'region']].drop_duplicates().to_csv(
        r"C:\Users\Amdrw\Desktop\nhs performance analytics\clean\unknown_regions.csv",
        index=False)
    print("Saved unknown_regions.csv")

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
combined.to_csv(OUTPUT_FILE, index=False)

print(f"\nSaved: {len(combined):,} rows")
print(f"Date range: {combined['period'].min()} to {combined['period'].max()}")
print(f"Unique orgs: {combined['org_code'].nunique()}")
print(f"Unique periods: {combined['period'].nunique()}")
print(f"Regions: {sorted(combined['clean_region'].unique())}")
print(f"Max rows per org/period: {combined.groupby(['org_code','period']).size().max()}")