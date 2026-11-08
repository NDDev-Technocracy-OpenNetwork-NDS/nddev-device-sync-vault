//! Test composition of the actual journal worker API, with opaque owned rows.
//! It qualifies storage/lifecycle, not authentication or ciphertext validity.
#![cfg(target_arch = "wasm32")]
use nddev_device_sync_domain::{
    DeviceId,
    sync::{EncryptedPayload, EntityKind, Operation},
};
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
        let operation = Operation {
            id: format!("fixture-original-{number}"),
            device: DeviceId::new("fixture-device").map_err(|_| JsValue::from_str("fixture_id"))?,
            entity_kind: EntityKind::VaultRecord,
            entity_id: format!("fixture-record-{number}"),
            base_revision: 0,
            idempotency_key: format!("fixture-original-{number}"),
            payload: EncryptedPayload {
                key_id: "11111111-1111-4111-8111-111111111111".into(),
                nonce: [number; 12],
                ciphertext: vec![number; 32],
            },
        };
        rows.push(serde_json::to_vec(&operation).map_err(|_| JsValue::from_str("fixture_body"))?);
    }
    serde_json::to_string(&rows).map_err(|_| JsValue::from_str("fixture_rows"))
}
