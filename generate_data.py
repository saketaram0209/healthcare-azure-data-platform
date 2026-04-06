"""
Healthcare Data Generator
Simulates the kind of on-premises hospital data you'd migrate to Azure.
Produces the same data types listed in the job description:
  - patients (PII/demographics)
  - encounters (admissions)
  - conditions (ICD-10 diagnoses)
  - medications
  - observations/labs
  - procedures (CPT codes / billing)
  - providers (staff)

WHY THIS STRUCTURE?
Each table mirrors a real hospital system table. They're connected
by IDs (patient_id, encounter_id) — just like a real relational database.
This is what you'll be joining in PySpark during the Silver layer.
"""

import pandas as pd
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

# ── helpers ────────────────────────────────────────────────────────────────────

def random_date(start_year=2018, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def uid():
    return str(uuid.uuid4())

# ── 1. PATIENTS ────────────────────────────────────────────────────────────────
# Real-world: sourced from hospital EHR (Epic, Cerner)
# Contains PHI — names, DOB, SSN, insurance ID
# This is why Purview flags these columns for HIPAA compliance

NUM_PATIENTS = 500

genders      = ["M", "F", "Unknown"]
blood_types  = ["A+","A-","B+","B-","AB+","AB-","O+","O-"]
states       = ["TX","CA","NY","FL","IL","OH","PA","GA","NC","MI"]
insurers     = ["BlueCross","Aetna","UnitedHealth","Cigna","Humana","Medicare","Medicaid","Self-Pay"]

patients = []
patient_ids = [uid() for _ in range(NUM_PATIENTS)]

for pid in patient_ids:
    dob = fake.date_of_birth(minimum_age=0, maximum_age=95)
    patients.append({
        "patient_id":     pid,
        "first_name":     fake.first_name(),           # PII
        "last_name":      fake.last_name(),            # PII
        "dob":            dob.strftime("%Y-%m-%d"),    # PII
        "gender":         random.choice(genders),
        "race":           random.choice(["White","Black","Asian","Hispanic","Other"]),
        "address":        fake.street_address(),       # PII
        "city":           fake.city(),
        "state":          random.choice(states),
        "zip":            fake.zipcode(),
        "phone":          fake.phone_number(),         # PII
        "ssn":            fake.ssn(),                  # PII — highly sensitive
        "insurance_id":   "INS-" + fake.bothify("??####??"),
        "insurer":        random.choice(insurers),
        "blood_type":     random.choice(blood_types),
        "created_at":     fake.date_time_between(start_date="-10y").strftime("%Y-%m-%d %H:%M:%S"),
    })

patients_df = pd.DataFrame(patients)

# ── 2. PROVIDERS (staff) ───────────────────────────────────────────────────────
# Doctors and nurses. Referenced in encounters and procedures.

NUM_PROVIDERS = 50
specialties = [
    "Cardiology","Emergency Medicine","Internal Medicine","Pediatrics",
    "Orthopedics","Neurology","Oncology","Radiology","Surgery","Psychiatry"
]
departments = [
    "ICU","Emergency","Oncology","Cardiology","General Ward",
    "Pediatrics","Surgery","Radiology","Neurology","Outpatient"
]

providers = []
provider_ids = [uid() for _ in range(NUM_PROVIDERS)]

for prov_id in provider_ids:
    providers.append({
        "provider_id":  prov_id,
        "name":         "Dr. " + fake.name(),
        "specialty":    random.choice(specialties),
        "department":   random.choice(departments),
        "license_no":   "LIC-" + fake.bothify("##?####"),
        "npi":          fake.bothify("##########"),   # National Provider Identifier
        "hire_date":    fake.date_between(start_date="-20y", end_date="-1y").strftime("%Y-%m-%d"),
        "shift":        random.choice(["Morning","Afternoon","Night","Rotating"]),
    })

providers_df = pd.DataFrame(providers)

# ── 3. ENCOUNTERS (admissions / visits) ───────────────────────────────────────
# Every time a patient visits the hospital — ER, inpatient, outpatient.
# This is the CENTRAL table. Almost everything joins through encounter_id.
# Think of it as the "fact" table of the operational system.

encounter_types = ["Emergency","Inpatient","Outpatient","Urgent Care","Telehealth"]
discharge_dispositions = [
    "Discharged Home","Transfer to SNF","Left AMA",
    "Discharged to Rehab","Expired","Transfer to Another Hospital"
]

encounters = []
encounter_ids = []

for _ in range(2000):   # 2000 visits across 500 patients
    eid = uid()
    encounter_ids.append(eid)
    admit_dt  = random_date()
    los_days  = random.randint(0, 30)        # length of stay
    discharge_dt = admit_dt + timedelta(days=los_days)

    encounters.append({
        "encounter_id":        eid,
        "patient_id":          random.choice(patient_ids),
        "provider_id":         random.choice(provider_ids),
        "encounter_type":      random.choice(encounter_types),
        "department":          random.choice(departments),
        "admit_date":          admit_dt.strftime("%Y-%m-%d"),
        "discharge_date":      discharge_dt.strftime("%Y-%m-%d"),
        "length_of_stay_days": los_days,
        "discharge_disposition": random.choice(discharge_dispositions),
        "readmission_30day":   random.choice([True, False, False, False]),  # 25% readmit
        "bed_id":              "BED-" + str(random.randint(100, 999)),
        "admission_source":    random.choice(["ER","Referral","Direct","Transfer"]),
        "created_at":          admit_dt.strftime("%Y-%m-%d %H:%M:%S"),
    })

encounters_df = pd.DataFrame(encounters)

# ── 4. CONDITIONS (ICD-10 diagnoses) ──────────────────────────────────────────
# Every encounter can have multiple diagnoses (primary + secondary).
# ICD-10 codes are the international standard for medical diagnoses.
# In real data, you'd have thousands of codes — we use a representative sample.

icd10_codes = {
    "I10":   "Essential Hypertension",
    "E11.9": "Type 2 Diabetes",
    "J18.9": "Pneumonia",
    "I21.9": "Acute Myocardial Infarction",
    "N18.3": "Chronic Kidney Disease Stage 3",
    "F32.1": "Major Depressive Disorder",
    "J44.1": "COPD with Exacerbation",
    "Z87.891":"Personal History of Nicotine Dependence",
    "M54.5": "Low Back Pain",
    "I63.9": "Cerebral Infarction (Stroke)",
    "C34.10":"Lung Cancer",
    "K92.1": "Melena (GI Bleed)",
    "S72.001":"Femur Fracture",
    "E66.9": "Obesity",
    "G43.909":"Migraine",
}

conditions = []
for eid in encounter_ids:
    num_diagnoses = random.randint(1, 4)
    codes = random.sample(list(icd10_codes.keys()), num_diagnoses)
    for i, code in enumerate(codes):
        conditions.append({
            "condition_id":    uid(),
            "encounter_id":    eid,
            "patient_id":      encounters_df.loc[encounters_df.encounter_id == eid, "patient_id"].values[0],
            "icd10_code":      code,
            "description":     icd10_codes[code],
            "diagnosis_type":  "Primary" if i == 0 else "Secondary",
            "onset_date":      random_date().strftime("%Y-%m-%d"),
            "severity":        random.choice(["Mild","Moderate","Severe","Critical"]),
            "chronic":         random.choice([True, False]),
            "recorded_at":     fake.date_time_between(start_date="-3y").strftime("%Y-%m-%d %H:%M:%S"),
        })

conditions_df = pd.DataFrame(conditions)

# ── 5. MEDICATIONS ────────────────────────────────────────────────────────────
# Drugs prescribed during encounters.
# In real pipelines you'd normalize drug names (spelling variants, brand vs generic)
# — exactly the kind of PySpark transformation you'll write in Silver layer.

drug_names = [
    "Metformin","Lisinopril","Atorvastatin","Amlodipine","Omeprazole",
    "Metoprolol","Albuterol","Gabapentin","Sertraline","Levothyroxine",
    "Prednisone","Furosemide","Warfarin","Insulin Glargine","Amoxicillin",
]
routes = ["Oral","IV","Subcutaneous","Inhaled","Topical","IM"]
statuses = ["Active","Completed","Discontinued","On Hold"]

medications = []
for eid in random.sample(encounter_ids, 1500):   # most encounters have meds
    num_meds = random.randint(1, 5)
    for _ in range(num_meds):
        start_dt = random_date()
        medications.append({
            "medication_id":   uid(),
            "encounter_id":    eid,
            "patient_id":      encounters_df.loc[encounters_df.encounter_id == eid, "patient_id"].values[0],
            "drug_name":       random.choice(drug_names),
            "dosage_mg":       random.choice([5,10,20,25,40,50,75,100,250,500]),
            "route":           random.choice(routes),
            "frequency":       random.choice(["Once Daily","Twice Daily","Every 8hr","PRN","Weekly"]),
            "start_date":      start_dt.strftime("%Y-%m-%d"),
            "end_date":        (start_dt + timedelta(days=random.randint(1,90))).strftime("%Y-%m-%d"),
            "status":          random.choice(statuses),
            "prescribing_provider_id": random.choice(provider_ids),
            "created_at":      start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        })

medications_df = pd.DataFrame(medications)

# ── 6. LAB RESULTS (observations) ─────────────────────────────────────────────
# CBC panels, metabolic panels, etc.
# Has numeric values + normal ranges + a flag if abnormal.
# You'll write PySpark logic to flag abnormal results in Silver layer.

lab_tests = {
    "CBC_WBC":    ("White Blood Cell Count", 4.5, 11.0,  "K/uL"),
    "CBC_HGB":    ("Hemoglobin",             12.0, 17.5, "g/dL"),
    "CBC_PLT":    ("Platelet Count",         150,  400,  "K/uL"),
    "BMP_GLU":    ("Glucose",                70,   100,  "mg/dL"),
    "BMP_NA":     ("Sodium",                 136,  145,  "mEq/L"),
    "BMP_K":      ("Potassium",              3.5,  5.0,  "mEq/L"),
    "BMP_CREAT":  ("Creatinine",             0.6,  1.2,  "mg/dL"),
    "LFT_ALT":    ("ALT (liver enzyme)",     7,    56,   "U/L"),
    "LIPID_LDL":  ("LDL Cholesterol",        0,    100,  "mg/dL"),
    "TSH":        ("Thyroid Stimulating Hormone", 0.4, 4.0, "mIU/L"),
}

labs = []
for eid in random.sample(encounter_ids, 1800):
    num_tests = random.randint(1, 6)
    test_keys = random.sample(list(lab_tests.keys()), num_tests)
    for key in test_keys:
        name, low, high, unit = lab_tests[key]
        # Simulate realistic distribution — mostly normal, some abnormal
        value = round(random.uniform(low * 0.6, high * 1.4), 2)
        flag = "Normal" if low <= value <= high else ("Low" if value < low else "High")
        labs.append({
            "lab_id":        uid(),
            "encounter_id":  eid,
            "patient_id":    encounters_df.loc[encounters_df.encounter_id == eid, "patient_id"].values[0],
            "test_code":     key,
            "test_name":     name,
            "result_value":  value,
            "unit":          unit,
            "normal_low":    low,
            "normal_high":   high,
            "result_flag":   flag,
            "collected_at":  fake.date_time_between(start_date="-3y").strftime("%Y-%m-%d %H:%M:%S"),
            "resulted_at":   fake.date_time_between(start_date="-3y").strftime("%Y-%m-%d %H:%M:%S"),
        })

labs_df = pd.DataFrame(labs)

# ── 7. PROCEDURES / BILLING ───────────────────────────────────────────────────
# CPT codes = standard billing codes for every procedure done.
# This feeds the financial dashboards in Power BI.

cpt_codes = {
    "99213": ("Office Visit - Established Patient",    150),
    "99285": ("Emergency Dept Visit - High Complexity",800),
    "93000": ("Electrocardiogram (ECG)",               75),
    "71046": ("Chest X-Ray",                           200),
    "80053": ("Comprehensive Metabolic Panel",         120),
    "85025": ("Complete Blood Count (CBC)",            90),
    "45378": ("Colonoscopy",                           1200),
    "27447": ("Total Knee Replacement",                15000),
    "33533": ("CABG - Arterial",                       45000),
    "70553": ("MRI Brain with Contrast",               2500),
}
payment_statuses = ["Paid","Pending","Denied","Partially Paid","Written Off"]

billing = []
for eid in encounter_ids:
    num_procedures = random.randint(1, 4)
    codes = random.sample(list(cpt_codes.keys()), num_procedures)
    for code in codes:
        desc, base_charge = cpt_codes[code]
        charge = round(base_charge * random.uniform(0.9, 1.3), 2)
        allowed = round(charge * random.uniform(0.5, 0.85), 2)
        paid    = round(allowed * random.uniform(0.7, 1.0), 2) if random.random() > 0.2 else 0
        billing.append({
            "billing_id":       uid(),
            "encounter_id":     eid,
            "patient_id":       encounters_df.loc[encounters_df.encounter_id == eid, "patient_id"].values[0],
            "cpt_code":         code,
            "description":      desc,
            "charge_amount":    charge,
            "allowed_amount":   allowed,
            "paid_amount":      paid,
            "balance":          round(allowed - paid, 2),
            "payment_status":   random.choice(payment_statuses),
            "insurer":          random.choice(insurers),
            "claim_date":       random_date().strftime("%Y-%m-%d"),
            "created_at":       fake.date_time_between(start_date="-3y").strftime("%Y-%m-%d %H:%M:%S"),
        })

billing_df = pd.DataFrame(billing)

# ── SAVE ALL TABLES AS CSV ─────────────────────────────────────────────────────
# These go into a folder called 'raw_data/' — simulating your on-premises data.
# In Phase 1, ADF will pick these up and land them in ADLS (the Bronze layer).

import os
output_dir = "raw_data"
os.makedirs(output_dir, exist_ok=True)

tables = {
    "patients":   patients_df,
    "providers":  providers_df,
    "encounters": encounters_df,
    "conditions": conditions_df,
    "medications": medications_df,
    "labs":       labs_df,
    "billing":    billing_df,
}

for name, df in tables.items():
    path = f"{output_dir}/{name}.csv"
    df.to_csv(path, index=False)
    print(f"✓ {name:12s}  →  {len(df):>6,} rows   {os.path.getsize(path)/1024:.1f} KB   saved to {path}")

print("\n── Column overview ──────────────────────────────────────────────────")
for name, df in tables.items():
    print(f"\n{name.upper()} ({len(df.columns)} cols): {', '.join(df.columns)}")
