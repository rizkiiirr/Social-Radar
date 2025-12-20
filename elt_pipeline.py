import pandas as pd
import os
import io
import shutil

# --- KONFIGURASI PATH ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_SOURCE = BASE_DIR  # File CSV ada di root folder
LAKE_BRONZE = os.path.join(BASE_DIR, 'datalake', 'bronze')
LAKE_SILVER = os.path.join(BASE_DIR, 'datalake', 'silver')

# Buat folder jika belum ada
os.makedirs(LAKE_BRONZE, exist_ok=True)
os.makedirs(LAKE_SILVER, exist_ok=True)

def clean_csv_quotes(file_path):
    """Membersihkan formatting CSV yang error (Double Quote issue)"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        cleaned_lines = []
        for line in lines:
            s_line = line.strip()
            # Hapus kutip pembungkus jika ada
            if s_line.startswith('"') and s_line.endswith('"'):
                content = s_line[1:-1]
                content = content.replace('""', '"') # Fix double escape
                cleaned_lines.append(content)
            else:
                cleaned_lines.append(s_line)
        return io.StringIO("\n".join(cleaned_lines))
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

def run_elt():
    print("🚀 MEMULAI PROSES ELT (Extract - Load - Transform)...")

    # --- 1. EXTRACT & LOAD (Pindahkan Raw ke Bronze) ---
    files = ['hasil_survey.csv', 'social_time_rules.csv']
    for f in files:
        src = os.path.join(RAW_SOURCE, f)
        dst = os.path.join(LAKE_BRONZE, f)
        if os.path.exists(src):
            shutil.copy(src, dst)
            print(f"✅ [LOAD] {f} berhasil masuk ke Bronze Layer.")
        else:
            print(f"❌ [ERROR] File {f} tidak ditemukan di root folder!")

    # --- 2. TRANSFORM (Bronze ke Silver Parquet) ---
    
    # A. Proses Data Survey
    print("⚙️ [TRANSFORM] Memproses Data Survey...")
    survey_path = os.path.join(LAKE_BRONZE, 'hasil_survey.csv')
    csv_content = clean_csv_quotes(survey_path)
    
    if csv_content:
        df_survey = pd.read_csv(csv_content)
        
        # Rename kolom agar masuk akal (Ciri fisik cowo -> Ciri fisik target)
        # Sesuai temuan kita sebelumnya bahwa data tertukar
        rename_map = {
            'intel_fisik_cowo': 'ciri_fisik',
            'intel_lokasi': 'habitat_pilihan',
            # Mapping kolom lain sesuai kebutuhan, ambil sampel utama dulu
        }
        # Kita ambil kolom penting saja agar rapi
        cols_to_keep = ['timestamp', 'gender', 'intel_fisik_cowo', 'intel_lokasi']
        df_clean = df_survey[cols_to_keep].copy()
        df_clean.rename(columns=rename_map, inplace=True)
        
        # Filter data kosong
        df_clean = df_clean.dropna(subset=['ciri_fisik'])
        
        # Tambahkan kolom Archetype (Logic sederhana untuk klasifikasi)
        # Di dunia nyata ini bisa pakai Machine Learning, di sini pakai Rule Based
        def get_archetype(text):
            text = str(text).lower()
            if 'kaca mata' in text or 'buku' in text: return 'The Intellectual'
            if 'branded' in text or 'heels' in text: return 'The Socialite'
            if 'jersey' in text or 'sneaker' in text: return 'The Sporty'
            return 'General Type'
            
        df_clean['archetype'] = df_clean['ciri_fisik'].apply(get_archetype)

        # SIMPAN KE PARQUET (Silver Layer)
        save_path = os.path.join(LAKE_SILVER, 'survey_data.parquet')
        df_clean.to_parquet(save_path, index=False)
        print(f"✅ [SUCCESS] Data Survey tersimpan di: {save_path}")

    # B. Proses Social Rules
    print("⚙️ [TRANSFORM] Memproses Social Rules...")
    rules_path = os.path.join(LAKE_BRONZE, 'social_time_rules.csv')
    csv_content_rules = clean_csv_quotes(rules_path)
    
    if csv_content_rules:
        df_rules = pd.read_csv(csv_content_rules)
        save_path_rules = os.path.join(LAKE_SILVER, 'rules_data.parquet')
        df_rules.to_parquet(save_path_rules, index=False)
        print(f"✅ [SUCCESS] Social Rules tersimpan di: {save_path_rules}")

    print("🏁 ELT SELESAI. SIAP UNTUK DASHBOARD.\n")

if __name__ == "__main__":
    run_elt()