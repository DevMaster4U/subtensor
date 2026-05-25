/// Processor tests.
/// The block-stream behaviour is covered by integration tests.
#[cfg(test)]
mod tests {
    use codec::Encode;
    use k256::ecdsa::SigningKey;
    use sp_core::{H160, U256};
    use subtensor_bot::transact::{TxConfig, prebuild};

    fn dummy_config() -> TxConfig {
        let signing_key = SigningKey::from_slice(&[1u8; 32]).expect("valid test key");
        TxConfig {
            signing_key,
            from: H160::from_low_u64_be(1),
            to: H160::from_low_u64_be(2),
            chain_id: 964,
            gas_limit: 21_000,
            max_fee_per_gas: 1_000_000_000,
            max_priority_fee_per_gas: 1_000_000_000,
        }
    }

    #[test]
    fn prebuild_produces_submittable_extrinsic() {
        let tx = prebuild(&dummy_config(), U256::zero(), vec![]);
        assert!(!tx.extrinsic.encode().is_empty());
    }
}
