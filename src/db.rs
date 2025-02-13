use crate::models::Rekening;
use anyhow::Result;
use log::{info, error};
use r2d2_oracle::OracleConnectionManager;
use r2d2::Pool;

pub struct DatabaseHandler {
    pool: Pool<OracleConnectionManager>,
}

impl DatabaseHandler {
    pub fn new(connection_string: &str) -> Result<Self> {
        // Parse connection string (format: username/password@tns_name)
        let parts: Vec<&str> = connection_string.split(['/', '@']).collect();
        let username = parts[0];
        let password = parts[1];
        let tns_name = parts[2];
        
        let manager = OracleConnectionManager::new(username, password, tns_name);
        let pool = Pool::new(manager)?;

        Ok(Self { pool })
    }

    pub fn insert_rekening(&self, rekening: &Rekening) -> Result<()> {
        info!("Starting insert for rekening: {:?}", rekening);
        let conn = self.pool.get()?;
        
        match conn.execute(
            "MERGE INTO V_BEN_REKONREK_SPRINT target
            USING (
                SELECT 
                    :1 as KODE,
                    :2 as KODE_SATKER,
                    :3 as NAMA_BANK,
                    :4 as NAMA_REK,
                    :5 as NO_IZIN,
                    :6 as NOREK,
                    TO_DATE(:7, 'YYYY-MM-DD') as TGL_IZIN,
                    :8 as OWNER,
                    :9 as KODE_UNIT_TEKNIS,
                    :10 as DESC_STATUS_REKENING,
                    :11 as STATUS_REKENING,
                    :12 as MATA_UANG
                FROM dual
            ) source
            ON (target.NOREK = source.NOREK)
            WHEN MATCHED THEN
                UPDATE SET
                    NAMA_BANK = source.NAMA_BANK,
                    NAMA_REK = source.NAMA_REK,
                    NO_IZIN = source.NO_IZIN,
                    TGL_IZIN = source.TGL_IZIN,
                    OWNER = source.OWNER,
                    KODE_UNIT_TEKNIS = source.KODE_UNIT_TEKNIS,
                    DESC_STATUS_REKENING = source.DESC_STATUS_REKENING,
                    STATUS_REKENING = source.STATUS_REKENING,
                    MATA_UANG = source.MATA_UANG,
                    MODIFIED_BY = 'SYSTEM',
                    MODIFIED_DATE = CURRENT_TIMESTAMP,
                    VERSION = VERSION + 1
            WHEN NOT MATCHED THEN
                INSERT (
                    KODE, KODE_SATKER, NAMA_BANK, NAMA_REK,
                    NO_IZIN, NOREK, TGL_IZIN, OWNER,
                    KODE_UNIT_TEKNIS, DESC_STATUS_REKENING,
                    STATUS_REKENING, MATA_UANG,
                    CREATED_BY, CREATED_DATE, VERSION, DELETED
                ) VALUES (
                    source.KODE, source.KODE_SATKER, source.NAMA_BANK,
                    source.NAMA_REK, source.NO_IZIN, source.NOREK,
                    source.TGL_IZIN, source.OWNER, source.KODE_UNIT_TEKNIS,
                    source.DESC_STATUS_REKENING, source.STATUS_REKENING,
                    source.MATA_UANG, 'SYSTEM', CURRENT_TIMESTAMP, 1, 0
                )",
            &[
                &rekening.kdjenis,           // KODE
                &rekening.kd_satker,         // KODE_SATKER
                &rekening.nama_bank,         // NAMA_BANK
                &rekening.nama_rekening,     // NAMA_REK
                &rekening.no_izin,          // NO_IZIN
                &rekening.no_rekening,      // NOREK
                &rekening.tgl_izin,         // TGL_IZIN
                &"1",                       // OWNER
                &"",                        // KODE_UNIT_TEKNIS
                &rekening.desc_status_rekening, // DESC_STATUS_REKENING
                &0,                         // STATUS_REKENING (default to 0)
                &"IDR",                     // MATA_UANG
            ],
        ) {
            Ok(rows) => {
                info!("Affected rows for {}: {:?}", rekening.no_rekening, rows);
                Ok(())
            }
            Err(e) => {
                error!("Error during upsert for {}: {:?}", rekening.no_rekening, e);
                Err(e.into())
            }
        }
    }

    pub fn get_active_satkers(&self, limit: i64) -> Result<Vec<String>> {
        let conn = self.pool.get()?;
        let rows = conn.query(
            "SELECT kd_satker FROM V_BEN_REKON_REK_SATKER 
             WHERE is_active = 1 
             ORDER BY last_fetch_date ASC NULLS FIRST 
             FETCH FIRST :1 ROWS ONLY",
            &[&limit],
        )?;

        let mut satkers = Vec::new();
        for row_result in rows {
            let row = row_result?;
            let kd_satker: String = row.get(0)?;
            satkers.push(kd_satker);
        }

        Ok(satkers)
    }

    pub fn update_last_fetch_date(&self, kd_satker: &str) -> Result<()> {
        let conn = self.pool.get()?;
        conn.execute(
            "UPDATE V_BEN_REKON_REK_SATKER 
             SET last_fetch_date = CURRENT_TIMESTAMP 
             WHERE kd_satker = :1",
            &[&kd_satker],
        )?;

        Ok(())
    }

    pub fn get_rekening_count(&self) -> Result<i64> {
        let conn = self.pool.get()?;
        let mut stmt = conn.prepare(
            "SELECT COUNT(*) FROM V_BEN_REKONREK_SPRINT",
            &[]
        )?;
        
        let row = stmt.query_row(&[])?;
        let count: i64 = row.get(0)?;
        Ok(count)
    }

    pub fn verify_rekening(&self, norek: &str) -> Result<bool> {
        let conn = self.pool.get()?;
        let mut stmt = conn.prepare(
            "SELECT COUNT(*) FROM V_BEN_REKONREK_SPRINT WHERE NOREK = :1",
            &[]
        )?;
        
        let row = stmt.query_row(&[&norek])?;
        let count: i64 = row.get(0)?;
        Ok(count > 0)
    }
} 