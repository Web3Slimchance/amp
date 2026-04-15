use amp_providers_bitcoin_rpc::tables::{blocks, inputs, outputs, transactions};
use datasets_common::network_id::NetworkId;
use datasets_raw::dataset::Table;

pub fn all(network: &NetworkId) -> Vec<Table> {
    vec![
        blocks::table(network.clone()),
        transactions::table(network.clone()),
        inputs::table(network.clone()),
        outputs::table(network.clone()),
    ]
}
