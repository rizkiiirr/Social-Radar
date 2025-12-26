import pandas as pd
import os
import io
import shutil

# --- KONFIGURASI PATH ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_SOURCE = BASE_DIR
LAKE_BRONZE = os.path.join(BASE_DIR, 'datalake', 'bronze')
LAKE_SILVER = os.path.join(BASE_DIR, 'datalake', 'silver')
LAKE_GOLD = os.path.join(BASE_DIR, 'datalake', 'gold')

os.makedirs(LAKE_BRONZE, exist_ok=True)
os.makedirs(LAKE_SILVER, exist_ok=True)
os.makedirs(LAKE_GOLD, exist_ok=True)

def clean_csv_quotes(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        cleaned_lines = []
        for line in lines:
            s_line = line.strip()
            if s_line.startswith('"') and s_line.endswith('"'):
                content = s_line[1:-1].replace('""', '"')
                cleaned_lines.append(content)
            else:
                cleaned_lines.append(s_line)
        return io.StringIO("\n".join(cleaned_lines))
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

def get_archetype(text):
    text = str(text).lower()
    if any(k in text for k in ['kaca', 'buku', 'laptop', 'seminar', 'kemeja']): 
        return 'Intellectual'
    if any(k in text for k in ['branded', 'heels', 'parfum', 'makeup', 'branded']): 
        return 'Social'
    if any(k in text for k in ['jersey', 'training', 'sneaker', 'running']): 
        return 'Sporty'
    if any(k in text for k in ['kamera', 'analog', 'seni', 'art', 'batik']): 
        return 'Creative'
    if any(k in text for k in ['organisasi', 'id card', 'kpu']):
        return 'Active'
    return 'General Type'

def run_elt():
    print("🚀 MEMULAI PROSES ELT...")

    # --- 1. EXTRACT & LOAD ---
    files = ['hasil_survey.csv', 'social_time_rules.csv', 'lokasi_bjm.json']
    for f in files:
        src = os.path.join(RAW_SOURCE, f)
        dst = os.path.join(LAKE_BRONZE, f)
        if os.path.exists(src):
            shutil.copy(src, dst)
            print(f"✅ [LOAD] {f} masuk ke Bronze.")

    # --- 2. TRANSFORM (Bronze ke Gold via Unpivot) ---
print("⚙️ [TRANSFORM] Memproses Data Survey...")
survey_path = os.path.join(LAKE_BRONZE, 'hasil_survey.csv')
csv_content = clean_csv_quotes(survey_path)

if csv_content:
    df_survey = pd.read_csv(csv_content)
    
    categories = [
        ('intel', 'Intellectual'), ('creative', 'Creative'), 
        ('social', 'Social'), ('sporty', 'Sporty'), 
        ('techie', 'Techie'), ('relig', 'Religius'), 
        ('active', 'Active')
    ]
    
    melted_data = []
    for _, row in df_survey.iterrows():
        for cat_prefix, cat_name in categories:
            # Menggabungkan ciri fisik cowo & cewe untuk kategori terkait
            t_cowo = str(row.get(f'{cat_prefix}_fisik_cowo', '')).replace('nan', '')
            t_cewe = str(row.get(f'{cat_prefix}_fisik_cewe', '')).replace('nan', '')
            traits = (t_cowo + ", " + t_cewe).strip(", ")
            
            # Mengambil lokasi spesifik kategori
            lokasi = str(row.get(f'{cat_prefix}_lokasi', '')).replace('nan', '')
            
            # Hanya masukkan ke Gold jika ada data (tidak kosong)
            if traits.strip() or lokasi.strip():
                melted_data.append({
                    'timestamp': row['timestamp'],
                    'gender': row['gender'],
                    'archetype': cat_name,
                    'ciri_fisik': traits,
                    'habitat_pilihan': lokasi
                })

    if melted_data:
        df_gold = pd.DataFrame(melted_data)
        # Simpan ke Gold Layer
        gold_path = os.path.join(LAKE_GOLD, 'locations.parquet')
        df_gold.to_parquet(gold_path, index=False)
        print(f"✅ [SUCCESS] Gold Layer tersimpan di: {gold_path}")

    # Proses Social Rules (Silver)
rules_path = os.path.join(LAKE_BRONZE, 'social_time_rules.csv')
csv_rules = clean_csv_quotes(rules_path)
if csv_rules:
        pd.read_csv(csv_rules).to_parquet(os.path.join(LAKE_SILVER, 'rules_data.parquet'), index=False)

print("🏁 ELT SELESAI.")

if __name__ == "__main__":
    run_elt()