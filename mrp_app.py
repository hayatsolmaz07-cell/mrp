            uygun_lotlar = lot_df[lot_df['asama'].isin(['KAPLAMA', 'SEVK'])]
            if uygun_lotlar.empty:
                st.warning("Sevke hazır lot bulunmuyor. (KAPLAMA veya SEVK aşamasında olmalı)")
                s_lot = None
                lot_miktar = 0.0
                s_mik = st.number_input("Miktar", min_value=0.1, value=0.1, step=0.1, format="%.3f", disabled=True)
            else:
                lot_ops = uygun_lotlar['lot_no'].tolist()
                s_lot = st.selectbox("Sevk Lot No", lot_ops)
                lot_miktar = float(uygun_lotlar[uygun_lotlar['lot_no'] == s_lot]['miktar'].values[0])
                s_mik = st.number_input("Miktar", min_value=0.1, max_value=lot_miktar, value=min(0.1, lot_miktar), step=0.1, format="%.3f")
            
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
                            st.rerun()
                        except Exception as e:
                            conn.rollback()
                            st.error(f"Sevkiyat hatası: {e}")
                    else:
                        st.error("Stok yetersiz!")
    else:
        st.warning("Sevkiyat için mamul bulunmuyor.")

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
