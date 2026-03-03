import pandas as pd
import os
import glob

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_FOLDER = os.path.join(BASE_DIR, "raw", "ae")
OUTPUT_FILE = os.path.join(BASE_DIR, "clean", "ae_clean.csv")

def parse_period(period_str):
    months = {
        'JANUARY': 1, 'FEBRUARY': 2, 'MARCH': 3, 'APRIL': 4,
        'MAY': 5, 'JUNE': 6, 'JULY': 7, 'AUGUST': 8,
        'SEPTEMBER': 9, 'OCTOBER': 10, 'NOVEMBER': 11, 'DECEMBER': 12
    }
    try:
        parts = str(period_str).upper().replace('MSITAE-', '').strip().split('-')
        month_num = months.get(parts[0])
        year = parts[1]
        if month_num and year.isdigit():
            return pd.Timestamp(int(year), month_num, 1)
    except:
        pass
    return pd.NaT

def clean_numeric(series):
    return pd.to_numeric(
        series.astype(str).str.strip().replace({'-': '0', '': '0', 'nan': '0'}),
        errors='coerce'
    ).fillna(0).astype(int)

all_files = glob.glob(os.path.join(RAW_FOLDER, "*.csv"))
print(f"Found {len(all_files)} CSV files")

frames = []

for filepath in all_files:
    try:
        df = pd.read_csv(filepath, dtype=str)
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r'[^a-z0-9]+', '_', regex=True)
            .str.strip('_')
        )
        col_map = {
            'period':                                               'period_raw',
            'org_code':                                             'org_code',
            'parent_org':                                           'region',
            'org_name':                                             'org_name',
            'a_e_attendances_type_1':                               'att_type1',
            'a_e_attendances_type_2':                               'att_type2',
            'a_e_attendances_other_a_e_department':                 'att_other',
            'attendances_over_4hrs_type_1':                         'over4hr_type1',
            'attendances_over_4hrs_type_2':                         'over4hr_type2',
            'attendances_over_4hrs_other_department':               'over4hr_other',
            'attendances_over_4hrs_other_a_e_department':           'over4hr_other',
            'patients_who_have_waited_4_12_hs_from_dta_to_admission': 'wait_4_12hr',
            'patients_who_have_waited_12_hrs_from_dta_to_admission':  'wait_12hr_plus',
            'emergency_admissions_via_a_e_type_1':                  'emerg_adm_type1',
            'emergency_admissions_via_a_e_type_2':                  'emerg_adm_type2',
            'emergency_admissions_via_a_e_other_a_e_department':    'emerg_adm_other',
            'number_of_a_e_attendances_type_1':                     'att_type1',
            'number_of_a_e_attendances_type_2':                     'att_type2',
            'number_of_a_e_attendances_other_a_e_department':       'att_other',
            'number_of_attendances_over_4hrs_type_1':               'over4hr_type1',
            'number_of_attendances_over_4hrs_type_2':               'over4hr_type2',
            'number_of_attendances_over_4hrs_other_a_e_department': 'over4hr_other',
        }
        available = {k: v for k, v in col_map.items() if k in df.columns}
        df = df.rename(columns=available)
        df = df[[c for c in available.values() if c in df.columns]]
        df = df.loc[:, ~df.columns.duplicated()]
        frames.append(df)
    except Exception as e:
        print(f"ERROR {os.path.basename(filepath)}: {e}")

print(f"Read {len(frames)} files")

combined = pd.concat(frames, ignore_index=True)
combined['period'] = combined['period_raw'].apply(parse_period)
combined = combined.drop(columns=['period_raw'])

numeric_cols = [
    'att_type1', 'att_type2', 'att_other',
    'over4hr_type1', 'over4hr_type2', 'over4hr_other',
    'wait_4_12hr', 'wait_12hr_plus',
    'emerg_adm_type1', 'emerg_adm_type2', 'emerg_adm_other'
]
for col in numeric_cols:
    if col not in combined.columns:
        combined[col] = 0
    else:
        combined[col] = clean_numeric(combined[col])

combined['org_code'] = combined['org_code'].astype(str).str.strip().str.upper()
combined['org_name'] = combined['org_name'].astype(str).str.strip().str.title()
combined['region'] = combined['region'].astype(str).str.strip().str.title()

combined = combined[combined['org_code'].notna()]
combined = combined[combined['period'].notna()]
combined = combined[combined['org_code'] != 'Nan']

combined['total_attendances'] = combined['att_type1'] + combined['att_type2'] + combined['att_other']
combined['total_over4hr'] = combined['over4hr_type1'] + combined['over4hr_type2'] + combined['over4hr_other']
combined['total_emerg_admissions'] = combined['emerg_adm_type1'] + combined['emerg_adm_type2'] + combined['emerg_adm_other']

combined['type1_4hr_performance'] = (
    (combined['att_type1'] - combined['over4hr_type1'])
    / combined['att_type1'].replace(0, float('nan'))
).astype(float).round(4)

combined['overall_4hr_performance'] = (
    (combined['total_attendances'] - combined['total_over4hr'])
    / combined['total_attendances'].replace(0, float('nan'))
).astype(float).round(4)

combined = combined.sort_values(['org_code', 'period']).reset_index(drop=True)

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
combined.to_csv(OUTPUT_FILE, index=False)

print(f"Saved to {OUTPUT_FILE}")
print(f"Rows: {len(combined):,}")
print(f"Date range: {combined['period'].min()} to {combined['period'].max()}")
print(f"Unique orgs: {combined['org_code'].nunique()}")