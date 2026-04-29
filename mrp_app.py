import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import time
from io import BytesIO
import unicodedata
import os
import shutil

# ---------------------------- GÜVENLİK / YARDIMCI ----------------------------
def validate_lot_no(lot_no, prefix="IN"):
    if not lot_no or str(lot_no).upper() in ["NAN", "NONE", "NULL", ""]:
        return generate_lot(prefix)
    return str(lot_no).upper().strip()

def generate_lot(prefix):
    return f"{prefix}-{datetime.now().strftime('%d%m%H%M%S')}"

def get_available_lots_for_process(stok_kodu=None, mevcut_asama=None):
    query = """
        SELECT 
            S.id as stok_id, S.kod as stok_kodu, S.ad as stok_adi, L.lot_no, L.miktar,
            COALESCE(T.asama, 'KALITE') as mevcut_asama,
            COALESCE(T.son_guncelleme, '-') as son_guncelleme,
            COUNT(DISTINCT U.id) as uretim_sayisi
        FROM LotStok L
        JOIN Stoklar S ON S.id = L.stok_id
        LEFT JOIN LotAsamaTakip T ON T.stok_id = L.stok_id AND T.lot_no = L.lot_no
        LEFT JOIN UretimKayitlari U ON U.mamul_id = L.stok_id
        WHERE L.miktar > 0 AND UPPER(COALESCE(S.tip, '')) NOT IN ('HAM', 'HAMMADDE')
    """
    params = []
    if stok_kodu:
        query += " AND UPPER(S.kod) LIKE ?"
        params.append(f"%{stok_kodu.upper()}%")
    if mevcut_asama:
        query += " AND COALESCE(T.asama, 'KALITE') = ?"
        params.append(mevcut_asama)
    query += " GROUP BY S.kod, L.lot_no ORDER BY S.kod, L.lot_no"
    return pd.read_sql_query(query, conn, params=params)

def get_lot_process_history(lot_no, stok_kodu):
    return pd.read_sql_query("""
        SELECT G.asama, G.tarih, G.aciklama
        FROM LotAsamaGecmis G
        JOIN Stoklar S ON S.id = G.stok_id
        WHERE G.lot_no = ? AND S.kod = ?
        ORDER BY G.id DESC
    """, conn, params=(lot_no, stok_kodu))

# ---------------------------- VERİTABANI ----------------------------
def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def get_shift_id_and_name(ts: datetime):
    t = ts.time()
    if t >= datetime.strptime("07:00", "%H:%M").time() and t < datetime.strptime("15:00", "%H:%M").time():
        return 1, "07:00-15:00"
    if t >= datetime.strptime("15:00", "%H:%M").time() and t <= datetime.strptime("23:00", "%H:%M").time():
        return 2, "15:00-23:00"
    return 3, "23:00-07:00"

def get_operator_assignment_for_day(atama_tarihi, vardiya_id, tezgah_id):
    tarih_str = atama_tarihi.strftime("%Y-%m-%d")
    birebir = cursor.execute("""
        SELECT operator_id FROM VardiyaAtamalari
        WHERE tarih=? AND vardiya_id=? AND tezgah_id=?
    """, (tarih_str, vardiya_id, tezgah_id)).fetchone()
    if birebir:
        return birebir
    if vardiya_id in (1, 2):
        rot = cursor.execute("""
            SELECT operator_a_id, operator_b_id, baslangic_tarihi
            FROM HaftalikRotasyonlar WHERE tezgah_id=?
        """, (tezgah_id,)).fetchone()
        if rot:
            op_a, op_b, bas_tarih_str = int(rot[0]), int(rot[1]), str(rot[2])
            bas_tarih = datetime.strptime(bas_tarih_str, "%Y-%m-%d").date()
            atama_tarihi_date = atama_tarihi.date() if hasattr(atama_tarihi, 'date') else atama_tarihi
            hafta_farki = (atama_tarihi_date - bas_tarih).days // 7
            cift_hafta = (hafta_farki % 2 == 0)
            if vardiya_id == 1:
                return (op_a,) if cift_hafta else (op_b,)
            return (op_b,) if cift_hafta else (op_a,)
    return cursor.execute("""
        SELECT operator_id FROM VardiyaAtamalari
        WHERE vardiya_id=3 AND tezgah_id=? AND tarih<=?
        ORDER BY tarih DESC LIMIT 1
    """, (tezgah_id, tarih_str)).fetchone()

def init_db():
    conn = sqlite3.connect("mrp_final_sistem.db", check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS Kullanicilar(username TEXT UNIQUE, password TEXT)')
    cursor.execute("SELECT * FROM Kullanicilar WHERE username='admin'")
    if not cursor.fetchone():
        cursor.execute("INSERT INTO Kullanicilar(username, password) VALUES ('admin', ?)", (make_hashes("admin123"),))
    cursor.execute('''CREATE TABLE IF NOT EXISTS Stoklar (
        id INTEGER PRIMARY KEY, kod TEXT UNIQUE, ad TEXT, tip TEXT, birim TEXT, miktar REAL DEFAULT 0)''')
    cursor.execute('CREATE TABLE IF NOT EXISTS Receteler (id INTEGER PRIMARY KEY, mamul_id INTEGER, hammadde_id INTEGER, miktar REAL)')
    cursor.execute('''CREATE TABLE IF NOT EXISTS IsEmirleri (
        id INTEGER PRIMARY KEY, mamul_id INTEGER, adet REAL, lot_no TEXT, durum TEXT, 
        baslangic_tarihi TEXT, bitis_tarihi TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Hareketler (
        id INTEGER PRIMARY KEY, stok_id INTEGER, hareket_miktari REAL, tip TEXT, 
        lot_no TEXT, tarih TEXT, firma_adi TEXT, irsaliye_no TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS LotStok (
        id INTEGER PRIMARY KEY, stok_id INTEGER, lot_no TEXT, miktar REAL DEFAULT 0, UNIQUE(stok_id, lot_no))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS LotAsamaTakip (
        id INTEGER PRIMARY KEY, stok_id INTEGER, lot_no TEXT, asama TEXT, son_guncelleme TEXT, UNIQUE(stok_id, lot_no))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS LotAsamaGecmis (
        id INTEGER PRIMARY KEY, stok_id INTEGER, lot_no TEXT, asama TEXT, tarih TEXT, aciklama TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Operatorler (id INTEGER PRIMARY KEY, ad TEXT UNIQUE NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Tezgahlar (id INTEGER PRIMARY KEY, kod TEXT UNIQUE NOT NULL, ad TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Vardiyalar (id INTEGER PRIMARY KEY, ad TEXT, baslangic TEXT, bitis TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS VardiyaAtamalari (
        id INTEGER PRIMARY KEY, tarih TEXT, vardiya_id INTEGER, tezgah_id INTEGER, operator_id INTEGER, UNIQUE(tarih, vardiya_id, tezgah_id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS HaftalikRotasyonlar (
        id INTEGER PRIMARY KEY, tezgah_id INTEGER UNIQUE, operator_a_id INTEGER, operator_b_id INTEGER, baslangic_tarihi TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS UretimKayitlari (
        id INTEGER PRIMARY KEY, is_emri_id INTEGER, mamul_id INTEGER, tezgah_id INTEGER, vardiya_id INTEGER, operator_id INTEGER, miktar REAL, tarih TEXT)''')
    cursor.execute("INSERT OR IGNORE INTO Vardiyalar (id, ad, baslangic, bitis) VALUES (1, '07:00-15:00', '07:00', '15:00')")
    cursor.execute("INSERT OR IGNORE INTO Vardiyalar (id, ad, baslangic, bitis) VALUES (2, '15:00-23:00', '15:00', '23:00')")
    cursor.execute("INSERT OR IGNORE INTO Vardiyalar (id, ad, baslangic, bitis) VALUES (3, '23:00-07:00', '23:00', '07:00')")
    cursor.execute('CREATE TABLE IF NOT EXISTS SistemAyarlari (anahtar TEXT UNIQUE, deger TEXT)')
    cursor.execute("INSERT OR IGNORE INTO SistemAyarlari (anahtar, deger) VALUES ('sirket_adi', 'PRO MRP SİSTEMLERİ')")
    cursor.execute("INSERT OR IGNORE INTO SistemAyarlari (anahtar, deger) VALUES ('versiyon', 'v1.0.0')")
    kolonlar = [c[1] for c in cursor.execute("PRAGMA table_info(IsEmirleri)").fetchall()]
    if "sarf_lot_no" not in kolonlar:
        cursor.execute("ALTER TABLE IsEmirleri ADD COLUMN sarf_lot_no TEXT")
    if "tezgah_id" not in kolonlar:
        cursor.execute("ALTER TABLE IsEmirleri ADD COLUMN tezgah_id INTEGER")
    if "operator_id" not in kolonlar:
        cursor.execute("ALTER TABLE IsEmirleri ADD COLUMN operator_id INTEGER")
    hareket_kolonlar = [c[1] for c in cursor.execute("PRAGMA table_info(Hareketler)").fetchall()]
    if "firma_adi" not in hareket_kolonlar:
        cursor.execute("ALTER TABLE Hareketler ADD COLUMN firma_adi TEXT")
    if "irsaliye_no" not in hareket_kolonlar:
        cursor.execute("ALTER TABLE Hareketler ADD COLUMN irsaliye_no TEXT")
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_hareketler_tarih ON Hareketler(tarih)",
        "CREATE INDEX IF NOT EXISTS idx_hareketler_stok ON Hareketler(stok_id)",
        "CREATE INDEX IF NOT EXISTS idx_lotstok_stok ON LotStok(stok_id)",
        "CREATE INDEX IF NOT EXISTS idx_isemirleri_durum ON IsEmirleri(durum)",
    ]
    for idx_query in indexes:
        try:
            cursor.execute(idx_query)
        except:
            pass
    conn.commit()
    return conn

conn = init_db()
cursor = conn.cursor()

# ---------------------------- GİRİŞ KONTROLÜ ----------------------------
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

sirket_adi = cursor.execute("SELECT deger FROM SistemAyarlari WHERE anahtar='sirket_adi'").fetchone()[0]
versiyon = cursor.execute("SELECT deger FROM SistemAyarlari WHERE anahtar='versiyon'").fetchone()[0]

if not st.session_state['logged_in']:
    st.markdown(f"""
        <style>
        .login-container {{
            background-color: #1e1e1e;
            padding: 2rem;
            border-radius: 10px;
            border: 1px solid #333;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
            text-align: center;
        }}
        .stTitle {{ color: #00ffcc !important; font-size: 2.5rem !important; }}
        </style>
    """, unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.markdown(f"<div class='login-container'>", unsafe_allow_html=True)
        st.title(f"🔒 {sirket_adi}")
        st.caption(f"Versiyon: {versiyon}")
        with st.form("login"):
            u = st.text_input("Kullanıcı Adı")
            p = st.text_input("Şifre", type='password')
            if st.form_submit_button("SİSTEME GİRİŞ YAP", use_container_width=True):
                if cursor.execute('SELECT * FROM Kullanicilar WHERE username=? AND password=?', (u, make_hashes(p))).fetchone():
                    st.session_state['logged_in'] = True
                    st.session_state['user'] = u
                    st.rerun()
                else:
                    st.error("Hatalı Giriş Bilgileri!")
        st.markdown("</div>", unsafe_allow_html=True)
    st.stop()

def sync_stocks_from_production():
    produced_ids = [int(r[0]) for r in cursor.execute("SELECT DISTINCT mamul_id FROM UretimKayitlari").fetchall()]
    if not produced_ids:
        return 0, 0
    stok_guncel_sayisi = 0
    lot_guncel_sayisi = 0
    for stok_id in produced_ids:
        toplam_uretim = float(cursor.execute("SELECT COALESCE(SUM(miktar), 0) FROM UretimKayitlari WHERE mamul_id=?", (stok_id,)).fetchone()[0])
        toplam_giris_devir = float(cursor.execute("SELECT COALESCE(SUM(hareket_miktari), 0) FROM Hareketler WHERE stok_id=? AND tip IN ('GIRIS','DEVIR')", (stok_id,)).fetchone()[0])
        toplam_cikis = float(cursor.execute("SELECT COALESCE(SUM(hareket_miktari), 0) FROM Hareketler WHERE stok_id=? AND tip IN ('SEVK','SARF')", (stok_id,)).fetchone()[0])
        yeni_stok = max(toplam_uretim + toplam_giris_devir - toplam_cikis, 0.0)
        eski_stok = float(cursor.execute("SELECT COALESCE(miktar, 0) FROM Stoklar WHERE id=?", (stok_id,)).fetchone()[0])
        if abs(eski_stok - yeni_stok) > 1e-9:
            cursor.execute("UPDATE Stoklar SET miktar=? WHERE id=?", (yeni_stok, stok_id))
            stok_guncel_sayisi += 1
        uretim_lotlari = {str(lot_no): float(mik) for lot_no, mik in cursor.execute("""
            SELECT I.lot_no, COALESCE(SUM(U.miktar), 0)
            FROM UretimKayitlari U JOIN IsEmirleri I ON I.id = U.is_emri_id
            WHERE U.mamul_id=? AND I.lot_no IS NOT NULL AND TRIM(I.lot_no)!='' GROUP BY I.lot_no
        """, (stok_id,)).fetchall()}
        giris_devir_lotlari = {str(lot_no): float(mik) for lot_no, mik in cursor.execute("""
            SELECT lot_no, COALESCE(SUM(hareket_miktari), 0)
            FROM Hareketler WHERE stok_id=? AND tip IN ('GIRIS','DEVIR') AND lot_no IS NOT NULL AND TRIM(lot_no)!='' GROUP BY lot_no
        """, (stok_id,)).fetchall()}
        cikis_lotlari = {str(lot_no): float(mik) for lot_no, mik in cursor.execute("""
            SELECT lot_no, COALESCE(SUM(hareket_miktari), 0)
            FROM Hareketler WHERE stok_id=? AND tip IN ('SEVK','SARF') AND lot_no IS NOT NULL AND TRIM(lot_no)!='' GROUP BY lot_no
        """, (stok_id,)).fetchall()}
        tum_lotlar = set(uretim_lotlari.keys()) | set(giris_devir_lotlari.keys()) | set(cikis_lotlari.keys())
        for lot_no in tum_lotlar:
            yeni_lot_miktar = max(uretim_lotlari.get(lot_no,0.0)+giris_devir_lotlari.get(lot_no,0.0)-cikis_lotlari.get(lot_no,0.0),0.0)
            mevcut = cursor.execute("SELECT id, COALESCE(miktar,0) FROM LotStok WHERE stok_id=? AND lot_no=?",(stok_id,lot_no)).fetchone()
            if mevcut:
                lot_id, eski_lot_miktar = int(mevcut[0]), float(mevcut[1])
                if abs(eski_lot_miktar - yeni_lot_miktar) > 1e-9:
                    if yeni_lot_miktar>0:
                        cursor.execute("UPDATE LotStok SET miktar=? WHERE id=?",(yeni_lot_miktar,lot_id))
                    else:
                        cursor.execute("DELETE FROM LotStok WHERE id=?",(lot_id,))
                    lot_guncel_sayisi +=1
            elif yeni_lot_miktar>0:
                cursor.execute("INSERT INTO LotStok(stok_id,lot_no,miktar) VALUES(?,?,?)",(stok_id,lot_no,yeni_lot_miktar))
                lot_guncel_sayisi +=1
    conn.commit()
    return stok_guncel_sayisi, lot_guncel_sayisi

st.set_page_config(page_title=f"{sirket_adi}", layout="wide")

st.markdown("""
    <style>
    [data-testid="stSidebar"] { background-color: #0e1117; }
    .stMetric { background-color: #1e2130; padding: 15px; border-radius: 10px; border-left: 5px solid #00ffcc; }
    .stButton>button { border-radius: 5px; font-weight: bold; }
    footer {visibility: hidden;}
    .main-footer { position: fixed; bottom: 10px; right: 10px; color: #555; font-size: 0.8rem; }
    </style>
    <div class="main-footer">Powered by Pro MRP Systems | v1.0.0</div>
""", unsafe_allow_html=True)

st.sidebar.markdown(f"### 🏭 {sirket_adi}")
st.sidebar.caption(f"Sistem Durumu: Çevrimiçi | {versiyon}")
st.sidebar.divider()

if st.sidebar.button("🚪 Güvenli Çıkış", use_container_width=True):
    st.session_state['logged_in'] = False
    st.rerun()

menu = st.sidebar.radio("MENÜ NAVİGASYON", ["📊 Dashboard", "📦 Stok Yönetimi", "📜 Reçete Yönetimi", "🛠️ İş Emirleri", "🏭 Proses Takip", "🚚 Sevkiyat", "⚙️ Ayarlar & Yedek"])

# ---------------------------- 📊 DASHBOARD ----------------------------
if menu == "📊 Dashboard":
    st.header("📊 Genel Durum")
    search_q = st.text_input("🔍 Stok Filtrele...").lower()
    t1, t2, t3 = st.tabs(["📦 Mevcut Stoklar", "📜 Hareket Geçmişi", "🏷️ Lot Bazlı Stok"])
    with t1:
        df_stok = pd.read_sql_query("SELECT kod, ad, miktar, birim, tip FROM Stoklar", conn)
        if search_q:
            df_stok = df_stok[df_stok['kod'].str.lower().str.contains(search_q) | df_stok['ad'].str.lower().str.contains(search_q)]
        st.dataframe(df_stok, use_container_width=True)
    with t2:
        df_h = pd.read_sql_query("SELECT H.id, H.tarih, H.lot_no, S.kod, H.hareket_miktari, H.tip, H.firma_adi, H.irsaliye_no FROM Hareketler H JOIN Stoklar S ON H.stok_id = S.id ORDER BY H.id DESC", conn)
        for _, row in df_h.iterrows():
            c1, c2 = st.columns([0.85, 0.15])
            c1.write(f"{row['tarih']} | {row['kod']} | {row['hareket_miktari']} | {row['tip']} | Lot: {row['lot_no']}")
            c2.write(f"🏢 {row['firma_adi'] or '-'} | 📄 {row['irsaliye_no'] or '-'}")
            if c2.button("Sil", key=f"h_del_{row['id']}"):
                cursor.execute("DELETE FROM Hareketler WHERE id=?", (row['id'],))
                conn.commit(); st.rerun()
    with t3:
        df_lot = pd.read_sql_query("SELECT S.kod, S.ad, L.lot_no, L.miktar as kalan_miktar, S.birim, S.tip FROM LotStok L JOIN Stoklar S ON S.id = L.stok_id WHERE L.miktar > 0 ORDER BY S.kod, L.lot_no", conn)
        if search_q:
            df_lot = df_lot[df_lot['kod'].str.lower().str.contains(search_q) | df_lot['ad'].fillna("").str.lower().str.contains(search_q) | df_lot['lot_no'].str.lower().str.contains(search_q)]
        st.dataframe(df_lot, use_container_width=True)

# ---------------------------- 📦 STOK YÖNETİMİ ----------------------------
elif menu == "📦 Stok Yönetimi":
    st.header("📦 Stok Yönetimi")
    if st.sidebar.button("⚠️ TÜM STOKLARI SIFIRLA"):
        cursor.execute("UPDATE Stoklar SET miktar = 0")
        conn.commit(); st.rerun()
    t1, t2, t3, t4 = st.tabs(["✍️ Stok Kartı", "📥 Stok Girişi", "📋 Stok Giriş Geçmişi", "📂 Excel'den Yükle"])
    df_stoklar = pd.read_sql_query("SELECT * FROM Stoklar", conn)
    with t1:
        secilen = st.selectbox("Ürün Seç", ["YENİ"] + df_stoklar['kod'].tolist())
        with st.form("stok_f"):
            row = df_stoklar[df_stoklar['kod'] == secilen].iloc[0] if secilen != "YENİ" else None
            k = st.text_input("Ürün Kodu", value=str(row['kod']) if row is not None else "").strip().upper()
            a = st.text_input("Ürün Adı", value=str(row['ad']) if row is not None else "")
            t = st.selectbox("Tip", ["HAM", "MAM"], index=0 if row is None or "HAM" in str(row['tip']).upper() else 1)
            birim_varsayilan = str(row['birim']).strip().upper() if row is not None and pd.notna(row['birim']) else "KG"
            birim_secenekleri = ["KG", "ADET", "MT", "LT"]
            if birim_varsayilan not in birim_secenekleri:
                birim_secenekleri = [birim_varsayilan] + birim_secenekleri
            b = st.selectbox("Birim", birim_secenekleri, index=birim_secenekleri.index(birim_varsayilan))
            if st.form_submit_button("Kaydet"):
                if k:
                    cursor.execute("INSERT OR REPLACE INTO Stoklar (kod, ad, tip, birim) VALUES (?,?,?,?)", (k, a, t, b))
                    conn.commit(); st.success("Kaydedildi"); st.rerun()
                else:
                    st.error("Kod boş olamaz!")
    with t2:
        with st.form("giris_f"):
            g_kod = st.selectbox("Ürün", df_stoklar['kod'].tolist())
            g_mik = st.number_input("Miktar", min_value=0.0001)
            giris_lot = st.text_input("Lot No (boşsa otomatik)", value="").strip().upper()
            giris_tarihi = st.date_input("Giriş Tarihi", value=datetime.now().date())
            giris_firma = st.text_input("Tedarikçi / Firma Adı")
            giris_irsaliye = st.text_input("İrsaliye No")
            if st.form_submit_button("Giriş Yap"):
                lot_no = validate_lot_no(giris_lot, "IN")
                tarih_str = giris_tarihi.strftime("%Y-%m-%d %H:%M")
                cursor.execute("UPDATE Stoklar SET miktar = miktar + ? WHERE kod = ?", (g_mik, g_kod))
                sid = cursor.execute("SELECT id FROM Stoklar WHERE kod=?", (g_kod,)).fetchone()[0]
                cursor.execute("INSERT INTO LotStok (stok_id, lot_no, miktar) VALUES (?,?,?) ON CONFLICT(stok_id, lot_no) DO UPDATE SET miktar = miktar + excluded.miktar", (sid, lot_no, g_mik))
                cursor.execute("INSERT INTO Hareketler (stok_id, hareket_miktari, tip, lot_no, tarih, firma_adi, irsaliye_no) VALUES (?,?,'GIRIS',?,?,?,?)", (sid, g_mik, lot_no, tarih_str, giris_firma, giris_irsaliye))
                conn.commit(); st.success("Stok Girişi Yapıldı"); st.rerun()
    with t3:
        st.subheader("📋 Stok Giriş Geçmişi")
        df_gecmis = pd.read_sql_query("""
            SELECT H.tarih, S.kod, S.ad, H.hareket_miktari, H.lot_no, H.firma_adi, H.irsaliye_no
            FROM Hareketler H JOIN Stoklar S ON S.id = H.stok_id
            WHERE H.tip = 'GIRIS' ORDER BY H.tarih DESC LIMIT 100
        """, conn)
        for _, row in df_gecmis.iterrows():
            with st.container(border=True):
                st.markdown(f"**{row['tarih']}** | {row['kod']} - {row['ad']}")
                st.caption(f"Lot: {row['lot_no']} | Miktar: {row['hareket_miktari']}")
                if row['firma_adi']:
                    st.write(f"🏢 {row['firma_adi']} | 📄 {row['irsaliye_no']}")
    with t4:
        up_stok = st.file_uploader("Stok Exceli", type="xlsx")
        if up_stok and st.button("Stokları Aktar"):
            try:
                df_up = pd.read_excel(up_stok, engine='openpyxl')
                for _, r in df_up.iterrows():
                    kod = str(r.iloc[0]).strip().upper()
                    if not kod or kod == "NAN":
                        continue
                    ad = str(r.iloc[1]) if len(df_up.columns) > 1 else ""
                    tip = "HAM" if "HAM" in str(r.iloc[2]).upper() else "MAM" if len(df_up.columns) > 2 else "HAM"
                    birim = str(r.iloc[3]) if len(df_up.columns) > 3 else "KG"
                    cursor.execute("INSERT OR REPLACE INTO Stoklar (kod, ad, tip, birim) VALUES (?,?,?,?)", (kod, ad, tip, birim))
                conn.commit(); st.success("Aktarım Başarılı"); st.rerun()
            except Exception as e:
                st.error(f"Excel Hatası: {e}")

# ---------------------------- 📜 REÇETE YÖNETİMİ ----------------------------
elif menu == "📜 Reçete Yönetimi":
    st.header("📜 Reçete Yönetimi")
    t1, t2 = st.tabs(["➕ Manuel Reçete Girişi", "📂 Excel'den Toplu Yükleme"])
    df_all = pd.read_sql_query("SELECT id, kod, tip FROM Stoklar", conn)
    with t1:
        with st.form("manuel_recete"):
            m_kod = st.selectbox("Üretilecek Ürün", df_all['kod'].tolist())
            h_kod = st.selectbox("Bileşen", df_all['kod'].tolist())
            k_miktar = st.number_input("Birim Kullanım", min_value=0.000001, format="%.6f")
            if st.form_submit_button("Ekle"):
                mid = df_all[df_all['kod'] == m_kod]['id'].values[0]
                hid = df_all[df_all['kod'] == h_kod]['id'].values[0]
                cursor.execute("INSERT OR REPLACE INTO Receteler (mamul_id, hammadde_id, miktar) VALUES (?,?,?)", (int(mid), int(hid), k_miktar))
                cursor.execute("UPDATE Stoklar SET tip='MAM' WHERE id=?", (int(mid),))
                conn.commit(); st.success("Eklendi!"); st.rerun()
    with t2:
        up_rec = st.file_uploader("Reçete Exceli", type="xlsx")
        if up_rec and st.button("Excel'den Yükle"):
            try:
                df_r = pd.read_excel(up_rec, engine='openpyxl')
                for _, r in df_r.iterrows():
                    ukod = str(r.iloc[0]).strip().upper()
                    hkod = str(r.iloc[1]).strip().upper()
                    miktar_val = float(r.iloc[2])
                    if not ukod or ukod == "NAN" or not hkod or hkod == "NAN":
                        continue
                    cursor.execute("INSERT OR IGNORE INTO Stoklar (kod, tip, birim) VALUES (?,'MAM','KG')", (ukod,))
                    cursor.execute("INSERT OR IGNORE INTO Stoklar (kod, tip, birim) VALUES (?,'HAM','KG')", (hkod,))
                    mid = cursor.execute("SELECT id FROM Stoklar WHERE kod=?", (ukod,)).fetchone()[0]
                    hid = cursor.execute("SELECT id FROM Stoklar WHERE kod=?", (hkod,)).fetchone()[0]
                    cursor.execute("INSERT OR REPLACE INTO Receteler (mamul_id, hammadde_id, miktar) VALUES (?,?,?)", (mid, hid, miktar_val))
                conn.commit(); st.success("Reçeteler Yüklendi"); st.rerun()
            except Exception as e:
                st.error(f"Hata: {e}")
    df_list = pd.read_sql_query("SELECT S1.kod as Mamul, S2.kod as Bilesen, R.miktar FROM Receteler R JOIN Stoklar S1 ON R.mamul_id=S1.id JOIN Stoklar S2 ON R.hammadde_id=S2.id", conn)
    st.dataframe(df_list, use_container_width=True)

# ---------------------------- 🛠️ İŞ EMİRLERİ ----------------------------
elif menu == "🛠️ İş Emirleri":
    st.header("🛠️ İş Emirleri")
    t1, t2, t3, t4 = st.tabs(["🚀 Yeni İş Emri", "✅ Açık Emirler", "🏁 Biten/İptal", "👥 Vardiya/Tezgah"])
    is_emri_listesi = pd.read_sql_query("SELECT DISTINCT S.kod FROM Stoklar S JOIN Receteler R ON S.id = R.mamul_id", conn)['kod'].tolist()
    
    with t1:
        if not is_emri_listesi:
            st.warning("Üretilecek reçeteli ürün bulunamadı.")
        tezgahlar_df = pd.read_sql_query("SELECT id, kod, COALESCE(ad, '') as ad FROM Tezgahlar ORDER BY kod", conn)
        operatorler_df = pd.read_sql_query("SELECT id, ad FROM Operatorler ORDER BY ad", conn)
        tezgah_ops = tezgahlar_df.apply(lambda r: f"{r['id']} | {r['kod']} {r['ad']}".strip(), axis=1).tolist()
        with st.form("is_f"):
            m_sec = st.selectbox("Üretilecek Kod", is_emri_listesi)
            miktar = st.number_input("Planlanan Adet", min_value=1.0)
            sarf_lot = st.text_input("Sarf Lot No (opsiyonel)", value="").strip().upper()
            uretilen_lot = st.text_input("Üretim Lot No (boşsa otomatik)", value="").strip().upper()
            sec_tezgah = st.selectbox("Tezgah (opsiyonel)", [""] + tezgah_ops) if tezgah_ops else st.selectbox("Tezgah", ["Tezgah yok"])
            sec_operator = st.selectbox("Operatör (opsiyonel)", [""] + operatorler_df.apply(lambda r: f"{r['id']} | {r['ad']}", axis=1).tolist()) if not operatorler_df.empty else st.selectbox("Operatör", ["Operatör yok"])
            if st.form_submit_button("Üretimi Başlat"):
                sec_tezgah_id = None
                if sec_tezgah and sec_tezgah != "" and sec_tezgah != "Tezgah yok":
                    sec_tezgah_id = int(sec_tezgah.split("|")[0].strip())
                sec_operator_id = None
                if sec_operator and sec_operator != "" and sec_operator != "Operatör yok":
                    sec_operator_id = int(sec_operator.split("|")[0].strip())
                mid = cursor.execute("SELECT id FROM Stoklar WHERE kod=?", (m_sec,)).fetchone()[0]
                is_lot = validate_lot_no(uretilen_lot, "PRD")
                cursor.execute("INSERT INTO IsEmirleri (mamul_id, adet, lot_no, sarf_lot_no, tezgah_id, operator_id, durum, baslangic_tarihi) VALUES (?,?,?,?,?,?, 'AÇIK',?)", (mid, miktar, is_lot, sarf_lot if sarf_lot else None, sec_tezgah_id, sec_operator_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                conn.commit()
                st.success(f"✅ Üretim emri açıldı. Üretim Lotu: {is_lot}")
                st.rerun()
    
    with t2:
        if st.button("🔁 Üretimden Stoğu Senkronize Et"):
            sync_stocks_from_production()
            st.rerun()
        df_acik = pd.read_sql_query("""
            SELECT I.id, S.kod, I.adet, I.lot_no, I.sarf_lot_no, I.tezgah_id, I.operator_id,
                   T.kod as tezgah_kod, O.ad as operator_ad, I.baslangic_tarihi
            FROM IsEmirleri I
            JOIN Stoklar S ON I.mamul_id = S.id
            LEFT JOIN Tezgahlar T ON T.id = I.tezgah_id
            LEFT JOIN Operatorler O ON O.id = I.operator_id
            WHERE I.durum='AÇIK'
        """, conn)
        all_ops_df = pd.read_sql_query("SELECT id, ad FROM Operatorler ORDER BY ad", conn)
        op_list_for_select = all_ops_df.apply(lambda r: f"{r['id']} | {r['ad']}", axis=1).tolist() if not all_ops_df.empty else []
        for _, row in df_acik.iterrows():
            with st.container(border=True):
                c1, c2, c3 = st.columns([0.54, 0.23, 0.23])
                toplam_uret = cursor.execute("SELECT COALESCE(SUM(miktar), 0) FROM UretimKayitlari WHERE is_emri_id=?", (int(row['id']),)).fetchone()[0]
                c1.write(f"**{row['kod']}** | Plan: {row['adet']} | Üretilen: {float(toplam_uret):.3f} | Lot: {row['lot_no']} | Tezgah: {row['tezgah_kod'] or '-'} | Operatör: {row['operator_ad'] or '-'}")
                
                with c2.popover("➕ Üretim Gir"):
                    p_miktar = st.number_input("Üretim Miktarı", min_value=0.001, value=1.0, step=0.1, format="%.3f", key=f"pm_{row['id']}")
                    p_tarih = st.date_input("Tarih", value=datetime.now().date(), key=f"pt_{row['id']}")
                    p_saat = st.time_input("Saat", value=datetime.now().time(), key=f"ps_{row['id']}")
                    p_operator = st.selectbox("Operatör", op_list_for_select, key=f"pop_{row['id']}") if op_list_for_select else None
                    if st.button("Kaydet", key=f"pk_{row['id']}"):
                        ts = datetime.combine(p_tarih, p_saat)
                        vardiya_id, _ = get_shift_id_and_name(ts)
                        op_id = int(p_operator.split("|")[0].strip()) if p_operator else None
                        try:
                            cursor.execute("INSERT INTO UretimKayitlari (is_emri_id, mamul_id, tezgah_id, vardiya_id, operator_id, miktar, tarih) VALUES (?, (SELECT id FROM Stoklar WHERE kod=?), ?, ?, ?, ?, ?)", (int(row['id']), row['kod'], row['tezgah_id'], vardiya_id, op_id, float(p_miktar), ts.strftime("%Y-%m-%d %H:%M:%S")))
                            cursor.execute("UPDATE Stoklar SET miktar = miktar + ? WHERE kod=?", (float(p_miktar), row['kod']))
                            cursor.execute("INSERT INTO LotStok (stok_id, lot_no, miktar) VALUES ((SELECT id FROM Stoklar WHERE kod=?),?,?) ON CONFLICT(stok_id, lot_no) DO UPDATE SET miktar = miktar + excluded.miktar", (row['kod'], row['lot_no'], float(p_miktar)))
                            cursor.execute("INSERT INTO Hareketler (stok_id, hareket_miktari, tip, lot_no, tarih) VALUES ((SELECT id FROM Stoklar WHERE kod=?),?,'URETIM',?,?)", (row['kod'], float(p_miktar), row['lot_no'], ts.strftime("%Y-%m-%d %H:%M:%S")))
                            conn.commit()
                            st.success("Üretim kaydı eklendi.")
                            st.rerun()
                        except Exception as e:
                            conn.rollback()
                            st.error(f"Hata: {e}")
                
                           if c2.button("✅ Bitir", key=f"b_{row['id']}"):
                    try:
                        uretilen_toplam = float(cursor.execute("SELECT COALESCE(SUM(miktar), 0) FROM UretimKayitlari WHERE is_emri_id=?", (int(row['id']),)).fetchone()[0])
                        if uretilen_toplam <= 0:
                            st.error("İş emri kapatılamaz: önce üretim kaydı girin.")
                            st.stop()
                        
                        mid = cursor.execute("SELECT id FROM Stoklar WHERE kod=?", (row['kod'],)).fetchone()[0]
                        
                        # GÜVENLİ RECURSIVE FONKSİYON (derinlik sınırlı)
                        hammaddeler = []
                        ziyaret_edilenler = set()
                        
                        def hammadde_topla_güvenli(urun_id, miktar, derinlik=0):
                            if derinlik > 10:
                                st.warning(f"⚠️ Reçete çok derin ({derinlik} seviye), işlem durduruldu.")
                                return
                            if (urun_id, miktar) in ziyaret_edilenler:
                                st.warning(f"⚠️ Reçetede döngü tespit edildi! (ID: {urun_id})")
                                return
                            ziyaret_edilenler.add((urun_id, miktar))
                            
                            recete = cursor.execute("""
                                SELECT R.hammadde_id, R.miktar, S.tip
                                FROM Receteler R
                                JOIN Stoklar S ON S.id = R.hammadde_id
                                WHERE R.mamul_id = ?
                            """, (urun_id,)).fetchall()
                            
                            if not recete:
                                return
                            
                            for hid, birim_miktar, tip in recete:
                                toplam_gereken = birim_miktar * miktar
                                if tip == 'MAM':
                                    hammadde_topla_güvenli(hid, toplam_gereken, derinlik + 1)
                                else:
                                    bulundu = False
                                    for hm in hammaddeler:
                                        if hm['id'] == hid:
                                            hm['gereken'] += toplam_gereken
                                            bulundu = True
                                            break
                                    if not bulundu:
                                        hammaddeler.append({'id': hid, 'gereken': toplam_gereken})
                        
                        hammadde_topla_güvenli(mid, uretilen_toplam)
                        
                        if not hammaddeler:
                            st.warning("⚠️ Bu ürün için reçete tanımlı değil! Stok düşüşü yapılmadı.")
                        else:
                            st.info(f"📦 Toplam {len(hammaddeler)} farklı hammadde stoktan düşülecek:")
                            
                            for hm in hammaddeler:
                                hm_id = hm['id']
                                hm_gereken = hm['gereken']
                                
                                mevcut_stok = cursor.execute("SELECT COALESCE(miktar, 0) FROM Stoklar WHERE id=?", (hm_id,)).fetchone()[0]
                                if mevcut_stok < hm_gereken:
                                    st.warning(f"⚠️ Yetersiz stok! Gereken: {hm_gereken:.2f}, mevcut: {mevcut_stok:.2f}")
                                    continue
                                
                                cursor.execute("UPDATE Stoklar SET miktar = miktar - ? WHERE id=?", (hm_gereken, hm_id))
                                
                                kalan = hm_gereken
                                lot_satirlari = cursor.execute("""
                                    SELECT id, lot_no, miktar FROM LotStok
                                    WHERE stok_id=? AND miktar > 0
                                    ORDER BY id
                                """, (hm_id,)).fetchall()
                                
                                düşülen = 0
                                for lot_id, lot_no, lot_miktar in lot_satirlari:
                                    if kalan <= 0:
                                        break
                                    kullan = min(kalan, lot_miktar)
                                    cursor.execute("UPDATE LotStok SET miktar = miktar - ? WHERE id=?", (kullan, lot_id))
                                    cursor.execute("""
                                        INSERT INTO Hareketler (stok_id, hareket_miktari, tip, lot_no, tarih)
                                        VALUES (?,?,'SARF',?,?)
                                    """, (hm_id, kullan, lot_no, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                                    kalan -= kullan
                                    düşülen += kullan
                                
                                if düşülen > 0:
                                    st.write(f"   ✓ {düşülen:.2f} birim düşüldü")
                                if kalan > 0:
                                    st.warning(f"   ⚠️ {kalan:.2f} birim düşülemedi (lot yetersiz)")
                            
                            st.success(f"✅ İş emri tamamlandı!")
                        
                        cursor.execute("UPDATE IsEmirleri SET durum='BİTTİ', bitis_tarihi=? WHERE id=?", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), row['id']))
                        conn.commit()
                        st.rerun()
                        
                    except Exception as e:
                        conn.rollback()
                        st.error(f"İş emri bitirme hatası: {e}")
                

        st.info("💡 İpucu: Reçetelerinizde döngü olup olmadığını kontrol edin (A->B, B->A gibi)")

                if c3.button("❌ İptal Et", key=f"i_{row['id']}"):
                    cursor.execute("UPDATE IsEmirleri SET durum='İPTAL', bitis_tarihi=? WHERE id=?", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), row['id']))
                    conn.commit()
                    st.rerun()
        if df_acik.empty:
            st.info("📭 Açık iş emri bulunmuyor.")
    
    with t3:
        df_kapali = pd.read_sql_query("""
            SELECT I.id, S.kod, I.adet, I.lot_no, I.durum, I.baslangic_tarihi, I.bitis_tarihi,
                   COALESCE(U.toplam_uretim, 0) AS gerceklesen_uretim
            FROM IsEmirleri I JOIN Stoklar S ON I.mamul_id = S.id
            LEFT JOIN (SELECT is_emri_id, SUM(miktar) AS toplam_uretim FROM UretimKayitlari GROUP BY is_emri_id) U ON U.is_emri_id = I.id
            WHERE I.durum != 'AÇIK' ORDER BY I.id DESC
        """, conn)
        if not df_kapali.empty:
            df_kapali["plana_uyum_%"] = df_kapali.apply(lambda r: (float(r["gerceklesen_uretim"]) / float(r["adet"]) * 100.0) if float(r["adet"]) > 0 else 0.0, axis=1)
            st.dataframe(df_kapali[['kod','adet','gerceklesen_uretim','plana_uyum_%','lot_no','durum','baslangic_tarihi','bitis_tarihi']], use_container_width=True)
        else:
            st.info("📭 Tamamlanmış iş emri yok.")
    
    with t4:
        st.subheader("Operatör ve Tezgah Tanımları")
        c_op, c_tz = st.columns(2)
        with c_op:
            with st.form("op_form"):
                op_ad = st.text_input("Operatör Adı").strip()
                if st.form_submit_button("Operatör Ekle"):
                    if op_ad:
                        cursor.execute("INSERT OR IGNORE INTO Operatorler (ad) VALUES (?)", (op_ad,))
                        conn.commit()
                        st.success("Operatör kaydedildi.")
                        st.rerun()
            op_list_df = pd.read_sql_query("SELECT id, ad FROM Operatorler ORDER BY ad", conn)
            if not op_list_df.empty:
                st.markdown("##### Operatör Düzelt / Sil")
                sec_op = st.selectbox("Operatör seç", op_list_df.apply(lambda r: f"{r['id']} | {r['ad']}", axis=1).tolist(), key="op_duzenle_sec")
                sec_op_id = int(sec_op.split("|")[0].strip())
                sec_op_ad = op_list_df[op_list_df['id'] == sec_op_id]['ad'].values[0]
                with st.form("op_duzelt_form"):
                    yeni_op_ad = st.text_input("Yeni Operatör Adı", value=str(sec_op_ad)).strip()
                    c_op1, c_op2 = st.columns(2)
                    guncel = c_op1.form_submit_button("Güncelle")
                    sil = c_op2.form_submit_button("Sil", type="secondary")
                    if guncel:
                        if yeni_op_ad:
                            try:
                                cursor.execute("UPDATE Operatorler SET ad=? WHERE id=?", (yeni_op_ad, sec_op_id))
                                conn.commit()
                                st.success("Operatör güncellendi.")
                                st.rerun()
                            except sqlite3.IntegrityError:
                                st.error("Bu operatör adı zaten mevcut.")
                    if sil:
                        st.warning("Silme işlemi geri alınamaz.")
                        op_onay = st.checkbox(f"{sec_op_ad} kaydını silmeyi onaylıyorum", key=f"op_sil_onay_{sec_op_id}")
                        if op_onay:
                            try:
                                cursor.execute("DELETE FROM Operatorler WHERE id=?", (sec_op_id,))
                                conn.commit()
                                st.success("Operatör silindi.")
                                st.rerun()
                            except sqlite3.IntegrityError:
                                st.error("Bu operatör atama/üretim kayıtlarında kullanılıyor.")
                        else:
                            st.info("Silmek için onay kutusunu işaretleyin.")
        with c_tz:
            with st.form("tz_form"):
                tz_kod = st.text_input("Tezgah Kodu").strip().upper()
                tz_ad = st.text_input("Tezgah Adı").strip()
                if st.form_submit_button("Tezgah Ekle"):
                    if tz_kod:
                        cursor.execute("INSERT OR IGNORE INTO Tezgahlar (kod, ad) VALUES (?,?)", (tz_kod, tz_ad))
                        conn.commit()
                        st.success("Tezgah kaydedildi.")
                        st.rerun()
            tz_list_df = pd.read_sql_query("SELECT id, kod, COALESCE(ad, '') as ad FROM Tezgahlar ORDER BY kod", conn)
            if not tz_list_df.empty:
                st.markdown("##### Tezgah Düzelt / Sil")
                sec_tz = st.selectbox("Tezgah seç", tz_list_df.apply(lambda r: f"{r['id']} | {r['kod']} {r['ad']}".strip(), axis=1).tolist(), key="tz_duzenle_sec")
                sec_tz_id = int(sec_tz.split("|")[0].strip())
                sec_tz_satir = tz_list_df[tz_list_df['id'] == sec_tz_id].iloc[0]
                with st.form("tz_duzelt_form"):
                    yeni_tz_kod = st.text_input("Yeni Tezgah Kodu", value=str(sec_tz_satir['kod'])).strip().upper()
                    yeni_tz_ad = st.text_input("Yeni Tezgah Adı", value=str(sec_tz_satir['ad'])).strip()
                    c_tz1, c_tz2 = st.columns(2)
                    tz_guncel = c_tz1.form_submit_button("Güncelle")
                    tz_sil = c_tz2.form_submit_button("Sil", type="secondary")
                    if tz_guncel:
                        if yeni_tz_kod:
                            try:
                                cursor.execute("UPDATE Tezgahlar SET kod=?, ad=? WHERE id=?", (yeni_tz_kod, yeni_tz_ad, sec_tz_id))
                                conn.commit()
                                st.success("Tezgah güncellendi.")
                                st.rerun()
                            except sqlite3.IntegrityError:
                                st.error("Bu tezgah kodu zaten mevcut.")
                    if tz_sil:
                        st.warning("Silme işlemi geri alınamaz.")
                        tz_onay = st.checkbox(f"{sec_tz_satir['kod']} kaydını silmeyi onaylıyorum", key=f"tz_sil_onay_{sec_tz_id}")
                        if tz_onay:
                            try:
                                cursor.execute("DELETE FROM Tezgahlar WHERE id=?", (sec_tz_id,))
                                conn.commit()
                                st.success("Tezgah silindi.")
                                st.rerun()
                            except sqlite3.IntegrityError:
                                st.error("Bu tezgah atama/üretim kayıtlarında kullanılıyor.")
                        else:
                            st.info("Silmek için onay kutusunu işaretleyin.")
        
        st.subheader("Haftalık Rotasyon (Sabah/Aksam)")
        ops_df = pd.read_sql_query("SELECT id, ad FROM Operatorler ORDER BY ad", conn)
        tez_df = pd.read_sql_query("SELECT id, kod, COALESCE(ad, '') as ad FROM Tezgahlar ORDER BY kod", conn)
        if ops_df.empty or tez_df.empty:
            st.info("Rotasyon tanımlamak için önce operatör ve tezgah tanımlayın.")
        else:
            with st.form("rotasyon_form"):
                r_tez = st.selectbox("Tezgah", tez_df.apply(lambda r: f"{r['id']} | {r['kod']} {r['ad']}".strip(), axis=1).tolist(), key="rot_tez")
                r_op_a = st.selectbox("Operatör A (başlangıç haftasında SABAH)", ops_df.apply(lambda r: f"{r['id']} | {r['ad']}", axis=1).tolist(), key="rot_op_a")
                r_op_b = st.selectbox("Operatör B (başlangıç haftasında AKŞAM)", ops_df.apply(lambda r: f"{r['id']} | {r['ad']}", axis=1).tolist(), key="rot_op_b")
                r_bas = st.date_input("Rotasyon Başlangıç Tarihi", value=datetime.now().date(), key="rot_bas")
                if st.form_submit_button("Rotasyonu Kaydet"):
                    tez_id = int(r_tez.split("|")[0].strip())
                    op_a_id = int(r_op_a.split("|")[0].strip())
                    op_b_id = int(r_op_b.split("|")[0].strip())
                    if op_a_id == op_b_id:
                        st.error("Operatör A ve B farklı olmalıdır.")
                    else:
                        cursor.execute("INSERT INTO HaftalikRotasyonlar (tezgah_id, operator_a_id, operator_b_id, baslangic_tarihi) VALUES (?,?,?,?) ON CONFLICT(tezgah_id) DO UPDATE SET operator_a_id=excluded.operator_a_id, operator_b_id=excluded.operator_b_id, baslangic_tarihi=excluded.baslangic_tarihi", (tez_id, op_a_id, op_b_id, r_bas.strftime("%Y-%m-%d")))
                        conn.commit()
                        st.success("Haftalık rotasyon kaydedildi.")
                        st.rerun()
            rot_df = pd.read_sql_query("SELECT R.id, T.kod as tezgah_kod, COALESCE(T.ad, '') as tezgah_ad, OA.ad as operator_a, OB.ad as operator_b, R.baslangic_tarihi FROM HaftalikRotasyonlar R JOIN Tezgahlar T ON T.id = R.tezgah_id JOIN Operatorler OA ON OA.id = R.operator_a_id JOIN Operatorler OB ON OB.id = R.operator_b_id ORDER BY T.kod", conn)
            if not rot_df.empty:
                st.caption("Kural: çift haftada sabah=A / akşam=B, tek haftada sabah=B / akşam=A")
                st.dataframe(rot_df[['id','tezgah_kod','tezgah_ad','operator_a','operator_b','baslangic_tarihi']], use_container_width=True)
                st.markdown("##### Rotasyon Önizleme (Bu Hafta + 4 Hafta)")
                bugun = datetime.now().date()
                hafta_basi = bugun - timedelta(days=bugun.weekday())
                onizleme_satirlari = []
                for _, rr in rot_df.iterrows():
                    bas_tarih = datetime.strptime(str(rr['baslangic_tarihi']), "%Y-%m-%d").date()
                    for i in range(5):
                        h_bas = hafta_basi + timedelta(days=7*i)
                        hafta_farki = (h_bas - bas_tarih).days // 7
                        cift_hafta = (hafta_farki % 2 == 0)
                        sabah_op = rr['operator_a'] if cift_hafta else rr['operator_b']
                        aksam_op = rr['operator_b'] if cift_hafta else rr['operator_a']
                        onizleme_satirlari.append({"tezgah_kod":rr['tezgah_kod'], "tezgah_ad":rr['tezgah_ad'], "hafta_baslangici":h_bas.strftime("%Y-%m-%d"), "sabah_07_15":sabah_op, "aksam_15_23":aksam_op})
                df_rot_oniz = pd.DataFrame(onizleme_satirlari).sort_values(["tezgah_kod","hafta_baslangici"])
                st.dataframe(df_rot_oniz, use_container_width=True)
                sec_rot = st.selectbox("Silinecek rotasyon", rot_df['id'].tolist(), format_func=lambda x: f"ID {x} - {rot_df[rot_df['id']==x]['tezgah_kod'].values[0]}", key="rot_sil_sec")
                if st.button("🗑️ Rotasyonu Sil", key="rot_sil_btn", type="secondary"):
                    cursor.execute("DELETE FROM HaftalikRotasyonlar WHERE id=?", (int(sec_rot),))
                    conn.commit()
                    st.success("Rotasyon silindi.")
                    st.rerun()
        
        st.subheader("Vardiya Ataması")
        ops_df = pd.read_sql_query("SELECT id, ad FROM Operatorler ORDER BY ad", conn)
        tez_df = pd.read_sql_query("SELECT id, kod, COALESCE(ad, '') as ad FROM Tezgahlar ORDER BY kod", conn)
        vard_df = pd.read_sql_query("SELECT id, ad FROM Vardiyalar ORDER BY id", conn)
        if ops_df.empty or tez_df.empty:
            st.warning("Atama için önce operatör ve tezgah tanımlayın.")
        else:
            with st.form("atama_form"):
                a_tarih = st.date_input("Atama Tarihi", value=datetime.now().date())
                a_vardiyalar = st.multiselect("Vardiyalar", vard_df.apply(lambda r: f"{r['id']} | {r['ad']}", axis=1).tolist())
                a_tezler = st.multiselect("Tezgahlar", tez_df.apply(lambda r: f"{r['id']} | {r['kod']} {r['ad']}".strip(), axis=1).tolist())
                a_op = st.selectbox("Operatör", ops_df.apply(lambda r: f"{r['id']} | {r['ad']}", axis=1).tolist())
                if st.form_submit_button("Atamayı Kaydet"):
                    op_id = int(a_op.split("|")[0].strip())
                    if not a_vardiyalar:
                        st.error("En az bir vardiya secin.")
                    elif not a_tezler:
                        st.error("En az bir tezgah secin.")
                    else:
                        for a_vard in a_vardiyalar:
                            vard_id = int(a_vard.split("|")[0].strip())
                            for a_tez in a_tezler:
                                tez_id = int(a_tez.split("|")[0].strip())
                                cursor.execute("INSERT INTO VardiyaAtamalari (tarih, vardiya_id, tezgah_id, operator_id) VALUES (?,?,?,?) ON CONFLICT(tarih, vardiya_id, tezgah_id) DO UPDATE SET operator_id=excluded.operator_id", (a_tarih.strftime("%Y-%m-%d"), vard_id, tez_id, op_id))
                        conn.commit()
                        st.success(f"Vardiya atamasi kaydedildi. Vardiya: {len(a_vardiyalar)} | Tezgah: {len(a_tezler)}")
                        st.rerun()
            st.markdown("#### Atama Düzelt / Sil")
            atama_df = pd.read_sql_query("SELECT A.id, A.tarih, A.vardiya_id, V.ad as vardiya_ad, A.tezgah_id, T.kod as tezgah_kod, COALESCE(T.ad, '') as tezgah_ad, A.operator_id, O.ad as operator_ad FROM VardiyaAtamalari A JOIN Vardiyalar V ON V.id = A.vardiya_id JOIN Tezgahlar T ON T.id = A.tezgah_id JOIN Operatorler O ON O.id = A.operator_id ORDER BY A.tarih DESC, A.vardiya_id, T.kod", conn)
            if atama_df.empty:
                st.info("Henüz atama kaydı yok.")
            else:
                st.dataframe(atama_df[['id','tarih','vardiya_ad','tezgah_kod','tezgah_ad','operator_ad']], use_container_width=True)
                secenekler = atama_df.apply(lambda r: f"{r['id']} | {r['tarih']} | {r['vardiya_ad']} | {r['tezgah_kod']} | {r['operator_ad']}", axis=1).tolist()
                secim = st.selectbox("Düzenlenecek/Silinecek atama", secenekler, key="atama_duzenle_sec")
                sec_id = int(secim.split("|")[0].strip())
                sec_satir = atama_df[atama_df['id']==sec_id].iloc[0]
                col_duz, col_sil = st.columns([0.7,0.3])
                with col_duz:
                    with st.form("atama_duzelt_form"):
                        n_tarih = st.date_input("Yeni Tarih", value=datetime.strptime(sec_satir['tarih'],"%Y-%m-%d").date(), key="atama_duz_tarih")
                        n_vard = st.selectbox("Yeni Vardiya", vard_df.apply(lambda r: f"{r['id']}|{r['ad']}",axis=1).tolist(), index=max(int(sec_satir['vardiya_id'])-1,0), key="atama_duz_vard")
                        n_tez = st.selectbox("Yeni Tezgah", tez_df.apply(lambda r: f"{r['id']}|{r['kod']} {r['ad']}".strip(), axis=1).tolist(), index=tez_df.index[tez_df['id']==int(sec_satir['tezgah_id'])][0], key="atama_duz_tez")
                        n_op = st.selectbox("Yeni Operatör", ops_df.apply(lambda r: f"{r['id']}|{r['ad']}",axis=1).tolist(), index=ops_df.index[ops_df['id']==int(sec_satir['operator_id'])][0], key="atama_duz_op")
                        if st.form_submit_button("Atamayı Güncelle"):
                            n_vard_id = int(n_vard.split("|")[0])
                            n_tez_id = int(n_tez.split("|")[0])
                            n_op_id = int(n_op.split("|")[0])
                            try:
                                cursor.execute("UPDATE VardiyaAtamalari SET tarih=?, vardiya_id=?, tezgah_id=?, operator_id=? WHERE id=?", (n_tarih.strftime("%Y-%m-%d"), n_vard_id, n_tez_id, n_op_id, sec_id))
                                conn.commit()
                                st.success("Atama güncellendi.")
                                st.rerun()
                            except sqlite3.IntegrityError:
                                st.error("Bu tarih-vardiya-tezgah için zaten başka bir atama var.")
                with col_sil:
                    st.write("")
                    st.write("")
                    atama_onay = st.checkbox(f"ID {sec_id} atamasını silmeyi onaylıyorum", key=f"atama_sil_onay_{sec_id}")
                    if st.button("🗑️ Atamayı Sil", key="atama_sil_btn", type="secondary"):
                        if atama_onay:
                            cursor.execute("DELETE FROM VardiyaAtamalari WHERE id=?", (sec_id,))
                            conn.commit()
                            st.success("Atama silindi.")
                            st.rerun()
                        else:
                            st.warning("Silme işlemi için önce onay kutusunu işaretleyin.")
        
        st.subheader("İş Emri - Vardiya Operatör Üretim Özeti")
        ozet_df = pd.read_sql_query("SELECT U.is_emri_id, S.kod as urun_kod, V.ad as vardiya, O.ad as operator, SUM(U.miktar) as uretim_miktari FROM UretimKayitlari U JOIN IsEmirleri I ON I.id = U.is_emri_id JOIN Stoklar S ON S.id = I.mamul_id JOIN Vardiyalar V ON V.id = U.vardiya_id JOIN Operatorler O ON O.id = U.operator_id GROUP BY U.is_emri_id, S.kod, V.ad, O.ad ORDER BY U.is_emri_id DESC, V.id, O.ad", conn)
        st.dataframe(ozet_df, use_container_width=True)

# ---------------------------- 🏭 PROSES TAKİP ----------------------------
elif menu == "🏭 Proses Takip":
    st.header("🏭 Lot Bazlı Proses Takip")
    asama_turkce = {"KALITE": "🔬 Kalite", "BUKUM": "🔄 Büküm", "ISIL_ISLEM": "🔥 Isıl İşlem", "KAPLAMA": "🎨 Kaplama", "SEVK": "🚚 Sevk"}
    df_lotlar = get_available_lots_for_process()
    if df_lotlar.empty:
        st.info("📭 Proses takibi için lot bulunmuyor.")
    else:
        for _, row in df_lotlar.iterrows():
            with st.container(border=True):
                col1, col2 = st.columns([3,1])
                with col1:
                    st.markdown(f"**{row['stok_kodu']}** - {row['lot_no']}")
                    st.caption(f"Miktar: {row['miktar']:.2f} | Aşama: {asama_turkce.get(row['mevcut_asama'], row['mevcut_asama'])}")
                with col2:
                    if st.button(f"➡️ İlerlet", key=f"ilerlet_{row['lot_no']}"):
                        asama_sirasi = ["KALITE","BUKUM","ISIL_ISLEM","KAPLAMA","SEVK"]
                        mevcut_idx = asama_sirasi.index(row['mevcut_asama']) if row['mevcut_asama'] in asama_sirasi else 0
                        if mevcut_idx+1 < len(asama_sirasi):
                            yeni_asama = asama_sirasi[mevcut_idx+1]
                            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            cursor.execute("INSERT INTO LotAsamaTakip (stok_id, lot_no, asama, son_guncelleme) VALUES (?,?,?,?) ON CONFLICT(stok_id, lot_no) DO UPDATE SET asama=?, son_guncelleme=?", (row['stok_id'], row['lot_no'], yeni_asama, now_str, yeni_asama, now_str))
                            cursor.execute("INSERT INTO LotAsamaGecmis (stok_id, lot_no, asama, tarih, aciklama) VALUES (?,?,?,?,?)", (row['stok_id'], row['lot_no'], yeni_asama, now_str, f"{row['mevcut_asama']} → {yeni_asama}"))
                            conn.commit()
                            st.rerun()

# ---------------------------- 🚚 SEVKİYAT ----------------------------
elif menu == "🚚 Sevkiyat":
    st.header("🚚 Sevkiyat")
    
    # Proses takibinden gelen yönlendirmeyi kontrol et
    if 'sevk_urun' in st.session_state:
        default_urun = st.session_state['sevk_urun']
        default_lot = st.session_state.get('sevk_lot', '')
        st.info(f"🎯 Proses takibinden yönlendirildiniz: **{default_urun}** - **{default_lot}**")
    else:
        default_urun = None
        default_lot = None
    
    st.subheader("📋 Sevk Geçmişi")
    sevk_urun_ara = st.text_input("Ürün kodu ile filtrele", value="", key="sevk_gecmis_urun").strip().upper()
    sevk_bas = st.date_input("Başlangıç Tarihi", value=datetime.now().date() - timedelta(days=30), key="sevk_gecmis_bas")
    sevk_bit = st.date_input("Bitiş Tarihi", value=datetime.now().date(), key="sevk_gecmis_bit")
    sevk_sql = """
        SELECT
            H.id,
            H.tarih,
            S.kod AS urun_kod,
            S.ad AS urun_adi,
            H.lot_no,
            H.hareket_miktari AS sevk_miktari
        FROM Hareketler H
        JOIN Stoklar S ON S.id = H.stok_id
        WHERE H.tip = 'SEVK'
          AND DATE(H.tarih) BETWEEN ? AND ?
    """
    sevk_params = [sevk_bas.strftime("%Y-%m-%d"), sevk_bit.strftime("%Y-%m-%d")]
    if sevk_urun_ara:
        sevk_sql += " AND UPPER(S.kod) LIKE ?"
        sevk_params.append(f"%{sevk_urun_ara}%")
    sevk_sql += " ORDER BY H.id DESC"
    df_sevk_gecmis = pd.read_sql_query(sevk_sql, conn, params=sevk_params)
    st.dataframe(
        df_sevk_gecmis[['tarih', 'urun_kod', 'urun_adi', 'lot_no', 'sevk_miktari']],
        use_container_width=True
    )
    if not df_sevk_gecmis.empty:
        toplam_sevk = float(df_sevk_gecmis['sevk_miktari'].sum())
        st.caption(f"Toplam sevk miktarı: {toplam_sevk:.3f}")

    st.divider()
    st.subheader("🚚 Yeni Sevkiyat")
    df_m = pd.read_sql_query("SELECT kod, miktar FROM Stoklar", conn)
    urun_ops = [k for k in df_m['kod'].tolist() if pd.notna(k) and str(k).strip() != ""]
    if not urun_ops:
        st.warning("Sevkiyat icin secilebilir urun yok. Once stok karti olusturun.")
        st.stop()

    # Default ürün seçimi
    if default_urun and default_urun in urun_ops:
        default_index = urun_ops.index(default_urun)
    else:
        default_index = 0
    
    s_kod = st.selectbox("Ürün", urun_ops, index=default_index, key="sev_urun")
    
    lot_df = pd.read_sql_query("""
        SELECT L.lot_no, L.miktar, COALESCE(T.asama, 'KALITE') as asama
        FROM LotStok L
        JOIN Stoklar S ON S.id = L.stok_id
        LEFT JOIN LotAsamaTakip T ON T.stok_id = L.stok_id AND T.lot_no = L.lot_no
        WHERE S.kod=? AND L.miktar > 0
        ORDER BY 
            CASE WHEN L.lot_no = ? THEN 0 ELSE 1 END,
            L.id
    """, conn, params=(s_kod, default_lot if default_lot else ''))
    
    stok_row = cursor.execute("SELECT id, miktar FROM Stoklar WHERE kod=?", (s_kod,)).fetchone()
    if not stok_row:
        st.error("Secilen urun stok kaydinda bulunamadi.")
        st.stop()
    stok_id = int(stok_row[0])
    mevcut = float(stok_row[1] if stok_row[1] is not None else 0.0)
    lot_toplam = float(lot_df['miktar'].sum()) if not lot_df.empty else 0.0
    devir_miktar = max(float(mevcut) - float(lot_toplam), 0.0)

    if lot_df.empty and mevcut > 0:
        st.info(f"Bu urunde lot kaydi yok ama toplam stok var: {mevcut:.3f}.")
        st.caption("Eski/lotsuz stoklari sevk edebilmek icin devir lotu olusturabilirsiniz.")
        if st.button("🔁 Mevcut Stoğu Devire Aktar", key="devire_aktar"):
            devir_lot = f"DEVIR-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            cursor.execute(
                "INSERT INTO LotStok (stok_id, lot_no, miktar) VALUES (?,?,?) ON CONFLICT(stok_id, lot_no) DO UPDATE SET miktar = miktar + excluded.miktar",
                (stok_id, devir_lot, float(mevcut))
            )
            cursor.execute(
                "INSERT INTO Hareketler (stok_id, hareket_miktari, tip, lot_no, tarih) VALUES (?,?,'DEVIR',?,?)",
                (stok_id, float(mevcut), devir_lot, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )
            conn.commit()
            st.success(f"Devir lotu olusturuldu: {devir_lot}")
            st.rerun()

    if devir_miktar > 0:
        st.info(f"Lot disi kalan stok tespit edildi: {devir_miktar:.3f}")
        if st.button("➕ Lot Dışı Stoğu Devire Ekle", key="devir_ekle"):
            devir_lot = f"DEVIR-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            cursor.execute(
                "INSERT INTO LotStok (stok_id, lot_no, miktar) VALUES (?,?,?) ON CONFLICT(stok_id, lot_no) DO UPDATE SET miktar = miktar + excluded.miktar",
                (stok_id, devir_lot, devir_miktar)
            )
            cursor.execute(
                "INSERT INTO Hareketler (stok_id, hareket_miktari, tip, lot_no, tarih) VALUES (?,?,'DEVIR',?,?)",
                (stok_id, devir_miktar, devir_lot, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )
            conn.commit()
            st.success(f"Lot disi stok devire eklendi: {devir_lot}")
            st.rerun()

    with st.form("sev"):
        lot_var = not lot_df.empty
        if lot_var:
            # Sadece KAPLAMA veya SEVK aşamasındaki lotları göster
            uygun_lotlar = lot_df[lot_df['asama'].isin(['KAPLAMA', 'SEVK'])]
            
            if uygun_lotlar.empty:
                st.warning("Sevke hazır lot bulunmuyor. (KAPLAMA veya SEVK aşamasında olmalı)")
                s_lot = None
                lot_miktar = 0.0
                s_mik = st.number_input("Miktar", min_value=0.1, value=0.1, step=0.1, format="%.3f", disabled=True)
            else:
                lot_ops = uygun_lotlar['lot_no'].tolist()
                # Default lot seçimi
                if default_lot and default_lot in lot_ops:
                    default_lot_index = lot_ops.index(default_lot)
                else:
                    default_lot_index = 0
                s_lot = st.selectbox("Sevk Lot No", lot_ops, index=default_lot_index)
                lot_miktar = float(uygun_lotlar[uygun_lotlar['lot_no'] == s_lot]['miktar'].values[0])
                s_mik = st.number_input("Miktar", min_value=0.1, max_value=lot_miktar, value=min(1.0, lot_miktar), step=0.1, format="%.3f")
        else:
            st.warning("Bu ürün için sevk edilebilir lot yok. Önce lot bazlı stok girişi/üretim yapın.")
            s_lot = None
            lot_miktar = 0.0
            s_mik = st.number_input("Miktar", min_value=0.1, value=0.1, step=0.1, format="%.3f", disabled=True)
        
        if st.form_submit_button("Gönder", use_container_width=True):
            if not s_lot:
                st.error("Sevkiyat için uygun lot bulunamadı!")
            else:
                asama_kayit = cursor.execute("""
                    SELECT asama FROM LotAsamaTakip
                    WHERE stok_id=(SELECT id FROM Stoklar WHERE kod=?) AND lot_no=?
                """, (s_kod, s_lot)).fetchone()
                mevcut_asama = asama_kayit[0] if asama_kayit else "KALITE"
                if mevcut_asama not in ("KAPLAMA", "SEVK"):
                    st.error(f"Bu lot sevke hazir degil. Mevcut asama: {mevcut_asama}. Sevk icin KAPLAMA veya SEVK olmalidir.")
                elif mevcut >= s_mik and lot_miktar >= s_mik:
                    try:
                        cursor.execute("UPDATE Stoklar SET miktar = miktar - ? WHERE kod=?", (s_mik, s_kod))
                        cursor.execute("UPDATE LotStok SET miktar = miktar - ? WHERE stok_id=(SELECT id FROM Stoklar WHERE kod=?) AND lot_no=?", (s_mik, s_kod, s_lot))
                        cursor.execute("INSERT INTO Hareketler (stok_id, hareket_miktari, tip, lot_no, tarih) VALUES ((SELECT id FROM Stoklar WHERE kod=?),?,'SEVK',?,?)", (s_kod, s_mik, s_lot, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        cursor.execute("""
                            INSERT INTO LotAsamaTakip (stok_id, lot_no, asama, son_guncelleme)
                            VALUES ((SELECT id FROM Stoklar WHERE kod=?), ?, 'SEVK', ?)
                            ON CONFLICT(stok_id, lot_no) DO UPDATE SET
                                asama='SEVK',
                                son_guncelleme=excluded.son_guncelleme
                        """, (s_kod, s_lot, now_str))
                        cursor.execute("""
                            INSERT INTO LotAsamaGecmis (stok_id, lot_no, asama, tarih, aciklama)
                            VALUES ((SELECT id FROM Stoklar WHERE kod=?), ?, 'SEVK', ?, 'KAPLAMA asamasindan sevke cikis')
                        """, (s_kod, s_lot, now_str))
                        conn.commit()
                        st.success("Sevkiyat tamamlandı.")
                        # Session temizle
                        if 'sevk_urun' in st.session_state:
                            del st.session_state['sevk_urun']
                        if 'sevk_lot' in st.session_state:
                            del st.session_state['sevk_lot']
                        st.rerun()
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Sevkiyat hatası: {e}")
                else:
                    st.error("Stok yetersiz!")

# ---------------------------- ⚙️ AYARLAR & YEDEK ----------------------------
elif menu == "⚙️ Ayarlar & Yedek":
    st.header("⚙️ Ayarlar ve Yedekleme")
    
    t1, t2 = st.tabs(["💾 Yedekleme İşlemleri", "ℹ️ Sistem Bilgisi"])
    
    db_file = "mrp_final_sistem.db"
    
    with t1:
        st.subheader("📊 Veritabanı Yedekleme")
        st.write("Verilerinizi güvende tutmak için düzenli olarak yedek almanız önerilir.")
        
        c1, c2 = st.columns(2)
        
        with c1:
            st.markdown("### 📥 Veritabanını İndir")
            st.write("Mevcut veritabanı dosyasını bilgisayarınıza indirmek için aşağıdaki butonu kullanın.")
            if os.path.exists(db_file):
                with open(db_file, "rb") as f:
                    db_bytes = f.read()
                st.download_button(
                    label="💾 Veritabanını (.db) İndir",
                    data=db_bytes,
                    file_name=f"mrp_yedek_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db",
                    mime="application/x-sqlite3",
                    use_container_width=True
                )
            else:
                st.error("Veritabanı dosyası bulunamadı!")
                
        with c2:
            st.markdown("### 📂 Yerel Yedek Oluştur")
            st.write("Uygulama klasöründe tarih damgalı bir kopya oluşturur.")
            if st.button("🚀 Yerel Yedek Al", use_container_width=True):
                if not os.path.exists("backups"):
                    os.makedirs("backups")
                
                backup_name = f"backups/mrp_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
                try:
                    shutil.copy2(db_file, backup_name)
                    st.success(f"Yedek başarıyla oluşturuldu: `{backup_name}`")
                except Exception as e:
                    st.error(f"Yedekleme hatası: {e}")
        
        st.divider()
        st.markdown("### 📖 Manuel Yedekleme Nasıl Yapılır?")
        st.info("""
        1. Uygulamanın kurulu olduğu klasörü açın.
        2. **`mrp_final_sistem.db`** dosyasını bulun.
        3. Bu dosyayı kopyalayıp güvenli bir yere (USB bellek, Cloud sürücü vb.) yapıştırın.
        4. Ayrıca kaynak kodun yedeği için **`mrp_app.py`** dosyasını da yedekleyebilirsiniz.
        """)

    with t2:
        st.write(f"**Uygulama Adı:** {sirket_adi}")
        st.write(f"**Yazılım Versiyonu:** `{versiyon}`")
        st.write(f"**Veritabanı Dosyası:** `{db_file}`")
        if os.path.exists(db_file):
            size_mb = os.path.getsize(db_file) / (1024 * 1024)
            st.write(f"**Veritabanı Boyutu:** {size_mb:.2f} MB")
        
        st.divider()
        st.subheader("🏢 Şirket Yapılandırması")
        with st.form("sirket_ayar_f"):
            yeni_ad = st.text_input("Şirket/Sistem Adı", value=sirket_adi)
            if st.form_submit_button("Ayarları Güncelle"):
                cursor.execute("UPDATE SistemAyarlari SET deger=? WHERE anahtar='sirket_adi'", (yeni_ad,))
                conn.commit()
                st.success("Sistem ayarları güncellendi. Lütfen sayfayı yenileyin.")
                st.rerun()

        st.subheader("🧹 Veri Yönetimi")
        st.warning("Dikkat: Bu işlem tüm hareket ve üretim verilerini temizler!")
        if st.button("🔴 SİSTEMİ SIFIRLA (Demo Verilerini Sil)", use_container_width=True):
            if st.checkbox("Tüm verileri silmeyi onaylıyorum"):
                cursor.execute("DELETE FROM Hareketler")
                cursor.execute("DELETE FROM UretimKayitlari")
                cursor.execute("DELETE FROM IsEmirleri")
                cursor.execute("DELETE FROM LotStok")
                cursor.execute("DELETE FROM LotAsamaTakip")
                cursor.execute("DELETE FROM LotAsamaGecmis")
                cursor.execute("UPDATE Stoklar SET miktar=0")
                conn.commit()
                st.success("Sistem demo verilerinden temizlendi.")
                st.rerun()
        
        st.divider()
        st.write("**Geliştirici Notu:** Bu sistem işletmenizin üretim ve stok süreçlerini profesyonel olarak takip etmek için yapılandırılmıştır.")

# Session temizleme
if 'hedef_asama' in st.session_state:
    del st.session_state['hedef_asama']
if 'selected_lot' in st.session_state:
    del st.session_state['selected_lot']
if 'selected_stok' in st.session_state:
    del st.session_state['selected_stok']

conn.close()
