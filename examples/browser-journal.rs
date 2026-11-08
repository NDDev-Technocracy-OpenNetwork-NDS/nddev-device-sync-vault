//! Test composition of the actual journal worker API, with opaque owned rows.
//! It qualifies storage/lifecycle, not authentication or ciphertext validity.
#![cfg(target_arch = "wasm32")]
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use nddev_device_sync_accounts::sync_wire as wire;
use nddev_device_sync_vault::browser;
use wasm_bindgen::prelude::*;

fn unavailable(_: nddev_device_sync_vault::Error) -> JsValue {
    JsValue::from_str("fixture_journal_unavailable")
}

#[wasm_bindgen]
pub async fn nds_journal_open(scope: String) -> Result<(), JsValue> {
    browser::worker_open(scope).await.map_err(unavailable)
}
#[wasm_bindgen]
pub async fn nds_journal_resume() -> Result<(), JsValue> {
    browser::worker_resume().await.map_err(unavailable)
}
#[wasm_bindgen]
pub fn nds_journal_execute(command: &str) -> Result<String, JsValue> {
    browser::worker_execute(command).map_err(unavailable)
}
#[wasm_bindgen]
pub fn nds_journal_pause() -> Result<(), JsValue> {
    browser::worker_pause().map_err(unavailable)
}

#[wasm_bindgen]
pub fn fixture_originals() -> Result<String, JsValue> {
    let mut rows = Vec::new();
    for number in 1..=3_u8 {
        // Use the generated wire shape accepted by the real storage command.
        let operation = wire::SyncOperation {
            schema_version: 2,
            operation_id: format!("fixture-original-{number}"),
            device_id: "fixture-device".into(),
            entity_type: wire::EntityType::VaultRecord,
            entity_id: format!("fixture-record-{number}"),
            base_revision: 0,
            idempotency_key: format!("fixture-original-{number}"),
            payload: wire::EncryptedPayload {
                algorithm: wire::Algorithm::Aes256Gcm,
                key_id: "11111111-1111-4111-8111-111111111111".into(),
                nonce: URL_SAFE_NO_PAD.encode([number; 12]),
                ciphertext: URL_SAFE_NO_PAD.encode([number; 32]),
            },
        };
        rows.push(serde_json::to_vec(&operation).map_err(|_| JsValue::from_str("fixture_body"))?);
    }
    serde_json::to_string(&rows).map_err(|_| JsValue::from_str("fixture_rows"))
}
