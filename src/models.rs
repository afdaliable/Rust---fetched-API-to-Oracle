use serde::{Deserialize, Serialize};
use chrono::NaiveDateTime;
use log::{info, error};

#[derive(Debug, Serialize, Deserialize)]
pub struct RekeningResponse {
    pub success: bool,
    pub message: String,
    pub code: String,
    #[serde(default)]
    pub data: Vec<RekeningData>,
    pub length: i32,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RekeningData {
    pub kdjenis: Option<String>,
    pub nmjenis: Option<String>,
    pub kdsatker: Option<String>,
    pub kdbank: Option<String>,
    pub kdjenbank: Option<String>,
    pub nmbank: Option<String>,
    pub nmcabang: Option<String>,
    pub nmrek: Option<String>,
    pub norek: Option<String>,
    pub noizin: Option<String>,
    pub tglizin: Option<String>,
    pub kdstatus: Option<String>,
    pub nmstatus: Option<String>,
}

impl RekeningData {
    pub fn to_rekening(&self) -> Option<Rekening> {
        // Add validation logging
        info!("Converting RekeningData to Rekening: {:?}", self);
        
        // First validate NOREK since it's the most important field
        let no_rekening = match &self.norek {
            Some(norek) if !norek.trim().is_empty() => norek.clone(),
            _ => {
                error!("Missing or empty NOREK");
                return None;
            }
        };

        let tgl_izin = match self.tglizin.as_ref().and_then(|date_str| {
            NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                .ok()
                .map(|dt| dt.format("%Y-%m-%d").to_string())
        }) {
            Some(date) => date,
            None => {
                error!("Failed to parse tglizin: {:?}", self.tglizin);
                return None;
            }
        };

        // Validate required fields
        if self.kdsatker.is_none() {
            error!("Missing kdsatker");
            return None;
        }

        Some(Rekening {
            kdjenis: self.kdjenis.clone().unwrap_or_default(),
            kd_satker: self.kdsatker.clone()?,
            nama_bank: self.nmbank.clone().unwrap_or_default(),
            nama_rekening: self.nmrek.clone().unwrap_or_default(),
            no_izin: self.noizin.clone().unwrap_or_default(),
            no_rekening,
            tgl_izin,
            desc_status_rekening: self.nmstatus.clone().unwrap_or_default(),
        })
    }
}

#[derive(Debug)]
pub struct Rekening {
    pub kdjenis: String,           // Used as KODE
    pub kd_satker: String,         // KODE_SATKER
    pub nama_bank: String,         // NAMA_BANK
    pub nama_rekening: String,     // NAMA_REK
    pub no_izin: String,          // NO_IZIN
    pub no_rekening: String,      // NOREK
    pub tgl_izin: String,         // TGL_IZIN
    pub desc_status_rekening: String, // DESC_STATUS_REKENING
} 