import streamlit as st
import pandas as pd
import duckdb
import requests
import pytz
import os
import io
from datetime import datetime

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

API_KEY_CUACA = "3eb5b834d98ca8ba49d53ac0c3a83569" 
KOTA = "Banjarmasin"
DB_FILE = "social_radar_olap.duckdb"

# ==========================================
# 2. FUNGSI INISIALISASI DATABASE (AUTO-FIX)
# ==========================================
def init_db():
    if not os.path.exists("hasil_survey.csv") or not os.path.exists("social_time_rules.csv"):
        st.error("❌ File CSV tidak ditemukan!")
        st.stop()

    with st.spinner('⚙️ Membangun Ulang Knowledge Base & Peta Lokasi...'):
        con = duckdb.connect(DB_FILE)
        
        # --- A. LOAD RAW DATA ---
        with open("hasil_survey.csv", "r", encoding='utf-8') as f:
            lines = f.readlines()
        cleaned = [l.strip()[1:-1].replace('""', '"') if l.strip().startswith('"') else l.strip() for l in lines]
        con.register('temp_survey', pd.read_csv(io.StringIO("\n".join(cleaned))))
        con.execute("CREATE OR REPLACE TABLE tb_survey AS SELECT * FROM temp_survey")

        with open("social_time_rules.csv", "r", encoding='utf-8') as f:
            lines_r = f.readlines()
        cleaned_r = [l.strip()[1:-1].replace('""', '"') if l.strip().startswith('"') else l.strip() for l in lines_r]
        con.register('temp_rules', pd.read_csv(io.StringIO("\n".join(cleaned_r))))
        con.execute("CREATE OR REPLACE TABLE tb_rules AS SELECT * FROM temp_rules")

        # 🔥 BAGIAN YANG HILANG (WAJIB DITAMBAHKAN) 🔥
        # Membuat View v_time_rules agar bisa dibaca oleh fungsi get_time_context
        con.execute("CREATE OR REPLACE VIEW v_time_rules AS SELECT * FROM tb_rules")

        # --- B. VIEW MAPPING CIRI FISIK ---
        con.execute("""
            CREATE OR REPLACE VIEW v_trait_mapping AS 
            SELECT 'Intellectual' as archetype, intel_fisik_cowo as traits FROM tb_survey WHERE intel_fisik_cowo IS NOT NULL
            UNION ALL SELECT 'Creative', creative_fisik_cowo FROM tb_survey WHERE creative_fisik_cowo IS NOT NULL
            UNION ALL SELECT 'Social', social_fisik_cowo FROM tb_survey WHERE social_fisik_cowo IS NOT NULL
            UNION ALL SELECT 'Sporty', sporty_fisik_cowo FROM tb_survey WHERE sporty_fisik_cowo IS NOT NULL
            UNION ALL SELECT 'Techie', techie_fisik_cowo FROM tb_survey WHERE techie_fisik_cowo IS NOT NULL
            UNION ALL SELECT 'Religius', relig_fisik_cowo FROM tb_survey WHERE relig_fisik_cowo IS NOT NULL
            UNION ALL SELECT 'Active', active_fisik_cowo FROM tb_survey WHERE active_fisik_cowo IS NOT NULL
        """)

        # --- C. VIEW LOKASI PEREMPUAN ---
        con.execute("""
            CREATE OR REPLACE VIEW v_female_locations AS
            SELECT 'Intellectual' as archetype, intel_lokasi as lokasi FROM tb_survey WHERE gender = 'Perempuan' AND intel_lokasi IS NOT NULL
            UNION ALL SELECT 'Creative', creative_lokasi FROM tb_survey WHERE gender = 'Perempuan' AND creative_lokasi IS NOT NULL
            UNION ALL SELECT 'Social', social_lokasi FROM tb_survey WHERE gender = 'Perempuan' AND social_lokasi IS NOT NULL
            UNION ALL SELECT 'Sporty', sporty_lokasi FROM tb_survey WHERE gender = 'Perempuan' AND sporty_lokasi IS NOT NULL
            UNION ALL SELECT 'Techie', techie_lokasi FROM tb_survey WHERE gender = 'Perempuan' AND techie_lokasi IS NOT NULL
            UNION ALL SELECT 'Religius', relig_lokasi FROM tb_survey WHERE gender = 'Perempuan' AND relig_lokasi IS NOT NULL
            UNION ALL SELECT 'Active', active_lokasi FROM tb_survey WHERE gender = 'Perempuan' AND active_lokasi IS NOT NULL
        """)

        # --- D. DATABASE KOORDINAT BANJARMASIN ---
        con.execute("""
            CREATE OR REPLACE TABLE dim_gps (nama_tempat VARCHAR, lat DOUBLE, lon DOUBLE);
            INSERT INTO dim_gps VALUES 
            ('Kampus', -3.2980, 114.5820), ('ULM', -3.2980, 114.5820), ('Unlam', -3.2980, 114.5820),
            ('Perpustakaan', -3.3321, 114.6050), ('Perpus', -3.3321, 114.6050), ('Palnam', -3.3321, 114.6050),
            ('Siring', -3.3204, 114.5910), ('Menara Pandang', -3.3204, 114.5910), ('Tendean', -3.3204, 114.5910),
            ('Duta Mall', -3.3285, 114.5982), ('DM', -3.3285, 114.5982), ('XXI', -3.3285, 114.5982),
            ('Taman Kamboja', -3.3245, 114.5890), ('Kamboja', -3.3245, 114.5890),
            ('Toko Buku', -3.3250, 114.5920), ('Gramedia', -3.3250, 114.5920),
            ('Cafe', -3.3150, 114.5950), ('Kopi', -3.3150, 114.5950), ('Nongkrong', -3.3150, 114.5950),
            ('Masjid', -3.3260, 114.5960), ('Sabilal', -3.3260, 114.5960), ('Raya', -3.3260, 114.5960),
            ('Gym', -3.3350, 114.6000), ('Studio', -3.3350, 114.6000),
            ('Art Gallery', -3.3290, 114.5900), ('Taman Budaya', -3.3290, 114.5900),
            ('Pasar', -3.3230, 114.5850), ('Thrift', -3.3230, 114.5850)
        """)

        # --- E. VIEW TRAITS ---
        con.execute("""
            CREATE OR REPLACE VIEW v_dim_traits AS
            SELECT DISTINCT unnest(str_split(intel_fisik_cowo, ', ')) as nilai 
            FROM tb_survey WHERE intel_fisik_cowo IS NOT NULL
        """)

        con.close()
        st.success("✅ Database Siap! Reset Selesai.")

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
        # 1. IDENTIFIKASI ARCHETYPE
        df_traits = con.execute("SELECT * FROM v_trait_mapping").df()
        scores = {}
        for _, row in df_traits.iterrows():
            archetype = row['archetype']
            db_traits = [x.strip().lower() for x in str(row['traits']).split(',')]
            input_traits = [x.lower() for x in ciri_input]
            match_count = len(set(db_traits).intersection(input_traits))
            if match_count > 0:
                scores[archetype] = scores.get(archetype, 0) + match_count
        
        if not scores: return pd.DataFrame()
        best_archetype = max(scores, key=scores.get)
        
        # 2. CARI LOKASI PEREMPUAN
        query_loc = f"SELECT lokasi FROM v_female_locations WHERE archetype = '{best_archetype}'"
        df_loc_raw = con.execute(query_loc).df()
        
        if df_loc_raw.empty: return pd.DataFrame()
            
        all_locations = []
        for raw in df_loc_raw['lokasi']:
            items = [x.strip() for x in str(raw).split(',')]
            all_locations.extend(items)
            
        # --- LOGIC BARU: FILTER BERDASARKAN WAKTU ---
        final_locations = all_locations # Default: semua lokasi
        
        if context_waktu is not None:
            # Ambil daftar tempat yang valid di jam ini (dari Rules CSV)
            # Contoh Malam Minggu: "Cafe, Mall, Taman Kota, Thrift Shop"
            allowed_places = [x.strip().lower() for x in str(context_waktu['rekomendasi_prioritas']).split(',')]
            
            # Filter: Hanya ambil lokasi survey yang ADA di daftar Allowed Places
            # Contoh: Intellectual suka [Perpus, Cafe, Taman]. 
            #         Malam Minggu boleh [Cafe, Mall].
            #         Irisan: [Cafe]. (Perpus dibuang).
            
            filtered = []
            for loc in all_locations:
                # Cek fuzzy match (misal "Cafe Sunyi" cocok dengan rule "Cafe")
                if any(rule in loc.lower() for rule in allowed_places):
                    filtered.append(loc)
            
            # Jika ada hasil filter, gunakan itu. Jika kosong (misal target gak suka mall), 
            # terpaksa kembali ke default (all_locations) tapi nanti diperingatkan.
            if filtered:
                final_locations = filtered

        # Hitung Frekuensi dari lokasi yang SUDAH DIFIFFER
        from collections import Counter
        if not final_locations: return pd.DataFrame() # Safety check
        
        most_common_loc = Counter(final_locations).most_common(1)[0][0]
        
        # 3. MAPPING KE KOORDINAT
        query_gps = f"SELECT * FROM dim_gps WHERE '{most_common_loc.lower()}' LIKE '%' || lower(nama_tempat) || '%'"
        df_gps = con.execute(query_gps).df()
        
        if not df_gps.empty:
            lat, lon = df_gps.iloc[0]['lat'], df_gps.iloc[0]['lon']
            final_loc_name = df_gps.iloc[0]['nama_tempat']
        else:
            lat, lon = -3.316694, 114.590111
            final_loc_name = most_common_loc

        # 4. SUSUN HASIL
        hasil = [{
            "Profil": f"Tipe {best_archetype}",
            "Skor": scores[best_archetype],
            "Lokasi_Nama": final_loc_name,
            "Lokasi_Full": most_common_loc,
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
            
            # UI Peta
            cm, cd = st.columns([2, 1])
            with cm:
                st.subheader(f"📍 Radar: {top['Lokasi_Nama']}")
                st.map(pd.DataFrame({'lat': [top['lat']], 'lon': [top['lon']]}))
            with cd:
                st.subheader("📋 Match Traits")
                for t in top['Match']:
                    st.write(f"✅ {t.title()}")
        else:
            st.error("❌ Tidak ada data cocok.")
    else:
        st.warning("⚠️ Pilih minimal satu ciri fisik.")