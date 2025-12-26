import streamlit as st
import pandas as pd
import duckdb
import requests
import pytz
import os
import io
from datetime import datetime
from dotenv import load_dotenv

# ==========================================
# 1. KONFIGURASI & SYSTEM CHECK
# ==========================================
st.set_page_config(
    page_title="Social Radar Banjarmasin", 
    page_icon="📡", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .rec-card {
        background-color: #;
        padding: 20px;
        border-radius: 10px;
        border-left: 6px solid #000000;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
        margin-bottom: 20px;
    }
    </style>
""", unsafe_allow_html=True)

load_dotenv()

# Mengambil API Key secara aman
API_KEY_CUACA = os.getenv("API_KEY_CUACA")

# Pengecekan (Opsional: agar tidak error kalau lupa bikin .env)
if not API_KEY_CUACA:
    st.error("⚠️ API Key Cuaca belum disetting di file .env")
    st.stop()
KOTA = "Banjarmasin"
DB_FILE = "social_radar_olap.duckdb"

# ==========================================
# 2. FUNGSI INISIALISASI DATABASE (AUTO-FIX)
# ==========================================
def init_db():
    # Path ke file Gold Layer yang dihasilkan oleh elt_pipeline.py
    path_gold = os.path.join("datalake", "gold", "locations.parquet")
    path_rules_silver = os.path.join("datalake", "silver", "rules_data.parquet")
    path_osm_bronze = os.path.join("datalake", "bronze", "lokasi_bjm.json")

    # Pastikan pipeline sudah dijalankan
    if os.path.exists(path_gold):
        st.toast(f"✅ Memuat data dari: {path_gold}") 
    else:
        st.error("❌ File Gold Layer tidak ditemukan!")

    with st.spinner('⚙️ Sinkronisasi Database dari Gold Layer...'):
        con = duckdb.connect(DB_FILE)
        
        # --- A. LOAD DARI GOLD & SILVER LAYER (PARQUET) ---
        # Kita tidak lagi membersihkan CSV di sini, tapi langsung baca Parquet
        con.execute(f"CREATE OR REPLACE TABLE tb_survey AS SELECT * FROM read_parquet('{path_gold}')")
        con.execute(f"CREATE OR REPLACE TABLE tb_rules AS SELECT * FROM read_parquet('{path_rules_silver}')")
        con.execute("CREATE OR REPLACE VIEW v_time_rules AS SELECT * FROM tb_rules")

        # --- B. VIEW MAPPING (Gunakan nama kolom baru dari Silver/Gold) ---
        # Di elt_pipeline, kita sudah me-rename 'intel_fisik_cowo' menjadi 'ciri_fisik'
        con.execute("""
        CREATE OR REPLACE VIEW v_trait_mapping AS 
        SELECT archetype, ciri_fisik as traits 
        FROM tb_survey 
        WHERE ciri_fisik IS NOT NULL
        """)

        con.execute("""
        CREATE OR REPLACE VIEW v_female_locations AS
        SELECT archetype, habitat_pilihan as lokasi 
        FROM tb_survey 
        WHERE gender = 'Perempuan' AND habitat_pilihan IS NOT NULL
        """)

        # --- C. LOAD DATA OSM (Tetap sama) ---
        if os.path.exists(path_osm_bronze):
            import json
            # BACA DARI BRONZE, BUKAN ROOT
            with open(path_osm_bronze, 'r', encoding='utf-8') as f:
                data_osm = json.load(f)
            
            osm_places = []
            # Parsing format Overpass JSON
            elements = data_osm.get('elements', [])
            for el in elements:
                tags = el.get('tags', {})
                name = tags.get('name')
                
                # Kita butuh tempat yang ada namanya
                if name:
                    # Ambil Kategori (Amenity/Shop/Leisure)
                    kategori = tags.get('amenity') or tags.get('shop') or tags.get('leisure') or tags.get('tourism') or 'unknown'
                    
                    # Ambil Koordinat (Node vs Way)
                    lat = el.get('lat')
                    lon = el.get('lon')
                    
                    # Jika tipe 'way' (bangunan), biasanya koordinat ada di 'center' (jika diexport dengan center)
                    # atau kita skip jika tidak ada lat/lon langsung (untuk penyederhanaan)
                    if not lat and 'center' in el:
                        lat = el['center']['lat']
                        lon = el['center']['lon']
                    
                    if lat and lon:
                        osm_places.append({
                            'nama_tempat': name,
                            'lat': lat,
                            'lon': lon,
                            'kategori': kategori
                        })
            
            if osm_places:
                df_osm = pd.DataFrame(osm_places)
                con.register('temp_osm', df_osm)
                con.execute("CREATE OR REPLACE TABLE dim_gps AS SELECT nama_tempat, lat, lon, kategori FROM temp_osm")
            else:
                # Fallback jika JSON kosong/format salah
                con.execute("CREATE OR REPLACE TABLE dim_gps (nama_tempat VARCHAR, lat DOUBLE, lon DOUBLE, kategori VARCHAR)")
        else:
            st.warning("⚠️ File 'lokasi_bjm.json' belum ada. Menggunakan mode minimal.")
            con.execute("CREATE OR REPLACE TABLE dim_gps (nama_tempat VARCHAR, lat DOUBLE, lon DOUBLE, kategori VARCHAR)")

        # --- D. VIEW TRAITS ---
        con.execute("""
            CREATE OR REPLACE VIEW v_dim_traits AS
            SELECT DISTINCT unnest(str_split(ciri_fisik, ', ')) as nilai 
            FROM tb_survey WHERE ciri_fisik IS NOT NULL
        """)

        con.close()
        st.success("✅ Database & Peta OSM Siap!")

# --- CHECK OTOMATIS SAAT STARTUP ---
if not os.path.exists(DB_FILE):
    init_db()

# ==========================================
# 3. BACKEND LOGIC (RUNTIME)
# ==========================================

def get_db():
    # Sekarang aman dipanggil karena DB pasti sudah ada
    return duckdb.connect(DB_FILE, read_only=True)

def get_cuaca():
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={KOTA}&appid={API_KEY_CUACA}&units=metric&lang=id"
        d = requests.get(url, timeout=3).json()
        return d['weather'][0]['main'], d['weather'][0]['description'], d['main']['temp']
    except: return "Unknown", "Offline", 30

def get_time_context():
    con = get_db()
    try:
        tz_banjarmasin = pytz.timezone('Asia/Makassar')
        now_wita = datetime.now(tz_banjarmasin)
        cur_hour = now_wita.hour + (now_wita.minute / 60)
        days_map = {0: 'senin', 1: 'selasa', 2: 'rabu', 3: 'kamis', 4: 'jumat', 5: 'sabtu', 6: 'minggu'}
        today_name = days_map[now_wita.weekday()]
        
        query = f"SELECT * FROM v_time_rules WHERE lower(trim(day_category)) = '{today_name}'"
        df = con.execute(query).df()
        
        if df.empty: return None

        def jam_match(row):
            start = float(row['start_hour'])
            end = float(row['end_hour'])
            if start <= end: return start <= cur_hour <= end
            else: return cur_hour >= start or cur_hour <= end

        matched = df[df.apply(jam_match, axis=1)]
        return matched.iloc[0] if not matched.empty else None
    except Exception as e:
        return None
    finally:
        con.close()

# Tambahkan parameter 'context_waktu'
def cari_target(ciri_input, context_waktu=None):
    con = get_db()
    try:
        # 1. IDENTIFIKASI ARCHETYPE (Query ke View yang sudah diperbarui)
        df_traits = con.execute("SELECT archetype, traits FROM v_trait_mapping").df()
        scores = {}
        input_traits = [x.lower().strip() for x in ciri_input]

        for _, row in df_traits.iterrows():
            arch = row['archetype']
            db_traits = str(row['traits']).lower()
            
            # Hitung seberapa banyak ciri yang dipilih user ada di baris ini
            match_count = sum(1 for trait in input_traits if trait in db_traits)
            
            if match_count > 0:
                scores[arch] = scores.get(arch, 0) + match_count
        
        if not scores: return pd.DataFrame()
        best_archetype = max(scores, key=scores.get)
        
        # 2. CARI LOKASI PEREMPUAN
        query_loc = f"SELECT habitat_pilihan FROM tb_survey WHERE archetype = '{best_archetype}'"
        df_loc_raw = con.execute(query_loc).df()
            
        all_locations = []

        if not df_loc_raw.empty: 
            for loc_str in df_loc_raw['habitat_pilihan']:
                items = [x.strip() for x in str(loc_str).split(',') if x.strip()]
                all_locations.extend(items)
            
        # LOGIC FILTER WAKTU
        final_locations = all_locations 
        if context_waktu is not None:
            allowed_places = [x.strip().lower() for x in str(context_waktu['rekomendasi_prioritas']).split(',')]
            filtered = []
            for loc in all_locations:
                if any(rule in loc.lower() for rule in allowed_places):
                    filtered.append(loc)
            if filtered: final_locations = filtered

        from collections import Counter
        import random

        if not final_locations: return pd.DataFrame()
        
        counts = Counter(all_locations).most_common(3)        # Lokasi Generik dari Survey (Contoh: "Art Gallery")
        most_common_loc = random.choice(counts)[0]        
        # 3. MAPPING KE KOORDINAT OSM
        cat_map = {
            'Intellectual': ['library', 'book_shop', 'university', 'college'],
            'Social': ['cafe', 'restaurant', 'fast_food', 'mall', 'clothing'],
            'Sporty': ['gym', 'park', 'pitch', 'stadium'],
            'Creative': ['arts_centre', 'gallery', 'cafe', 'museum'],
            'Active': ['university', 'office', 'park'],
            'Professional/Active': ['office', 'bank', 'cafe']
        }
        relevant_cats = cat_map.get(best_archetype, ['cafe', 'restaurant'])
        cat_sql = "', '".join(relevant_cats)

        query_gps = f"""
            SELECT * FROM dim_gps 
            WHERE lower(nama_tempat) LIKE '%' || lower('{most_common_loc}') || '%'
            OR lower('{most_common_loc}') LIKE '%' || lower(nama_tempat) || '%'
            LIMIT 1
        """
        df_gps = con.execute(query_gps).df()
        
        # --- PERBAIKAN DI SINI ---
        display_name = most_common_loc # Default awal
        
        if not df_gps.empty:
            # SKENARIO 1: Ketemu Nama Persis
            lat, lon = df_gps.iloc[0]['lat'], df_gps.iloc[0]['lon']
            final_loc_name = df_gps.iloc[0]['nama_tempat']
            display_name = final_loc_name # Update display jadi nama spesifik
        else:
            # SKENARIO 2: Nama Gak Ketemu, Cari Backup Kategori
            query_backup = f"""
                SELECT * FROM dim_gps 
                WHERE kategori IN ('{cat_sql}') 
                ORDER BY random() 
                LIMIT 1
            """
            df_backup = con.execute(query_backup).df()
            
            if not df_backup.empty:
                lat, lon = df_backup.iloc[0]['lat'], df_backup.iloc[0]['lon']
                real_osm_name = df_backup.iloc[0]['nama_tempat']
                
                # Judul Besar: "Starbucks (Rekomendasi Creative)"
                final_loc_name = f"{real_osm_name} (Rekomendasi {best_archetype})"
                
                # Lokasi Kecil: "Starbucks" (Bukan "Art Gallery" lagi)
                display_name = real_osm_name 
            else:
                # SKENARIO 3: Nyerah (Pusat Kota)
                lat, lon = -3.3194, 114.5928
                final_loc_name = f"{most_common_loc} (Area Umum)"
                display_name = most_common_loc

        # 4. SUSUN HASIL
        hasil = [{
            "Profil": f"Tipe {best_archetype}",
            "Skor": scores[best_archetype],
            "Lokasi_Nama": final_loc_name,   # Judul Besar
            "Lokasi_Full": display_name,     # FIX: Menampilkan nama tempat spesifik
            "lat": lat,
            "lon": lon,
            "Match": ciri_input
        }]
        return pd.DataFrame(hasil)
    finally:
        con.close()

# ==========================================
# 4. UI SIDEBAR (CONTROL PANEL)
# ==========================================
with st.sidebar:
    st.title("🎛️ Control Panel")
    st.markdown("---")
    st.caption("📍 Lokasi Sistem: Banjarmasin (WITA)")
    
    # Dropdown Ciri Fisik
    con = get_db()
    try:
        # Ambil list unik dari traits untuk dropdown
        opsi = con.execute("SELECT nilai FROM v_dim_traits ORDER BY nilai").df()['nilai'].tolist()
        # Bersihkan opsi kosong/aneh
        opsi = [x for x in opsi if x and len(str(x)) > 2]
    except: 
        opsi = ["Kacamata", "Jas", "Tas Ransel"] # Fallback
    con.close()
    
    user_input = st.multiselect("Pilih Ciri Fisik:", opsi, placeholder="Misal: Kacamata...")
    st.markdown("---")
    btn_scan = st.button("📡 SCAN TARGET", use_container_width=True, type="primary")

# ==========================================
# 5. MAIN DASHBOARD
# ==========================================

st.title("📡 Social Radar: Banjarmasin Intelligence")
st.markdown("Sistem pendukung keputusan pencarian habitat sosial berbasis **Data Lakehouse**.")
st.divider()

cuaca_main, cuaca_desc, suhu = get_cuaca()
ctx = get_time_context()
tz_banjarmasin = pytz.timezone('Asia/Makassar')
now_wita_display = datetime.now(tz_banjarmasin)

if not btn_scan:
    # --- STATUS AWAL ---
    c1, c2, c3 = st.columns(3)
    c1.metric("🌤️ Cuaca", f"{suhu}°C", cuaca_desc.title())
    c2.metric("⌚ Waktu (WITA)", now_wita_display.strftime("%H:%M"), ctx['phase_name'] if ctx is not None else "-")
    c3.metric("🚦 Status Kota", ctx['status_sosial'] if ctx is not None else "Normal")
    st.info("👈 Silakan pilih ciri fisik di sidebar.")
else:
    # --- HASIL SCAN ---
    if user_input:
        res = cari_target(user_input, context_waktu=ctx)
        
        if not res.empty:
            top = res.iloc[0]
            
            # --- LOGIC PRESCRIPTIVE ---
            rekomendasi_lokasi = top['Lokasi_Nama']
            
            # 1. Analisis Berdasarkan Cuaca Spesifik
            if "Rain" in cuaca_main or "Drizzle" in cuaca_main or "Thunderstorm" in cuaca_main:
                # KONDISI HUJAN
                alasan_cuaca = f"⚠️ <strong>HUJAN ({cuaca_desc.title()}):</strong> Hindari area terbuka. Prioritaskan lokasi Indoor."
                
                # Jika rekomendasinya Outdoor, berikan peringatan keras/alternatif
                if any(x in rekomendasi_lokasi.lower() for x in ['taman', 'siring', 'pasar', 'lapangan']):
                    rekomendasi_lokasi += " ☔ (Cari Shelter/Cafe Terdekat!)"
                else:
                    rekomendasi_lokasi += " 🏢 (Aman - Indoor)"

            elif "Clear" in cuaca_main:
                # KONDISI CERAH / PANAS
                alasan_cuaca = "☀️ <strong>CUACA CERAH:</strong> Waktu yang tepat untuk foto-foto atau aktivitas Outdoor."
                if "taman" in rekomendasi_lokasi.lower() or "siring" in rekomendasi_lokasi.lower():
                    alasan_cuaca += " Jangan lupa gunakan sunblock/topi."

            elif "Clouds" in cuaca_main:
                # KONDISI BERAWAN (Adem)
                alasan_cuaca = "☁️ <strong>BERAWAN/SEJUK:</strong> Cuaca paling nyaman untuk nongkrong (Indoor/Outdoor aman)."
            
            else:
                # KONDISI LAINNYA (Mist, Haze, dll)
                alasan_cuaca = f"ℹ️ <strong>KONDISI {cuaca_main.upper()}:</strong> Tetap waspada terhadap perubahan cuaca."

            rekomendasi_waktu = ""
            if ctx is not None:
                nama_fase = ctx['phase_name']
                jam_mulai = str(ctx['start_hour']).replace('.', ':')
                jam_selesai = str(ctx['end_hour']).replace('.', ':')
                rekomendasi_waktu = f"{nama_fase} ({jam_mulai} - {jam_selesai})"
            else:
                rekomendasi_waktu = "Diluar Jam Operasional Utama"
            # UI Metrics
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Target", top['Profil'])
            c2.metric("Cuaca", cuaca_main, f"{suhu}°C")
            c3.metric("Akurasi", f"{top['Skor']} Poin")
            c4.metric("Fase Waktu", ctx['phase_name'] if ctx is not None else "-")

            # UI Card Rekomendasi
            st.markdown(f"""
                <div class="rec-card">
                    <h2 style="color: #0d47a1; margin-top:0;">REKOMENDASI: {rekomendasi_lokasi.upper()}</h2>
                    <p><strong>📍 Lokasi:</strong> {top['Lokasi_Full']}</p>
                    <p><strong>🛠️ Strategi:</strong> {alasan_cuaca}</p>
                    <p><strong>🕒 Waktu:</strong> {rekomendasi_waktu}</p>
                </div>
            """, unsafe_allow_html=True)
            
            # UI Peta & Navigasi
            cm, cd = st.columns([2, 1])
            
            with cm:
                st.subheader(f"📍 Radar: {top['Lokasi_Nama']}")
                
                # 1. Tampilkan Peta
                st.map(pd.DataFrame({'lat': [top['lat']], 'lon': [top['lon']]}))
                
                # --- TAMBAHAN BARU ---
                # 2. Tampilkan Koordinat Teks
                st.caption(f"📌 Koordinat GPS: {top['lat']}, {top['lon']}")
                
                # 3. Tombol Link ke Google Maps
                # Membuat URL dinamis berdasarkan latitude & longitude target
                link_gmaps = f"https://www.google.com/maps?q={top['lat']},{top['lon']}"
                
                # Menampilkan tombol merah yang bisa diklik
                st.link_button(
                    label="🚀 Buka Rute di Google Maps", 
                    url=link_gmaps, 
                    type="primary",      # Membuat tombol berwarna menonjol
                    use_container_width=True
                )
                # ---------------------

            with cd:
                st.subheader("📋 Match Traits")
                for t in top['Match']:
                    st.write(f"✅ {t.title()}")
        else:
            st.error("❌ Tidak ada data cocok.")
    else:
        st.warning("⚠️ Pilih minimal satu ciri fisik.")