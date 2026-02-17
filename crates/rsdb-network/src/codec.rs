//! Substrait codec for encoding/decoding plan fragments

use rsdb_common::{Result, RsdbError};
use substrait::proto::Plan;

use prost::Message;

/// Encode a Substrait plan to bytes
pub fn encode_plan(plan: &Plan) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    plan.encode(&mut bytes)
        .map_err(|e| RsdbError::Planner(format!("Failed to encode plan: {}", e)))?;
    Ok(bytes)
}

/// Decode bytes to a Substrait plan
pub fn decode_plan(bytes: &[u8]) -> Result<Plan> {
    Plan::decode(bytes).map_err(|e| RsdbError::Planner(format!("Failed to decode plan: {}", e)))
}

/// Substrait codec helper
pub struct SubstraitCodec;

impl SubstraitCodec {
    pub fn encode(plan: &Plan) -> Result<Vec<u8>> {
        encode_plan(plan)
    }

    pub fn decode(bytes: &[u8]) -> Result<Plan> {
        decode_plan(bytes)
    }
}
