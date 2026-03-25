use std::sync::Arc;

use amp_providers_common::{config::ProviderConfigRaw, provider_name::ProviderName};
use object_store::{ObjectStore, memory::InMemory};

use crate::{ProvidersRegistry, store::ProviderConfigsStore};

fn create_test_providers_store() -> (
    ProvidersRegistry,
    ProviderConfigsStore<Arc<dyn ObjectStore>>,
) {
    let in_memory_store = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = in_memory_store.clone();
    let configs_store = ProviderConfigsStore::new(store.clone());
    let providers_registry = ProvidersRegistry::new(configs_store.clone());
    (providers_registry, configs_store)
}

fn create_test_evm_provider(name: &str) -> (ProviderName, ProviderConfigRaw) {
    let toml_str = indoc::formatdoc! {r#"
        kind = "evm-rpc"
        network = "mainnet"
        url = "http://localhost:8545"
    "#};
    let config = toml::from_str(&toml_str).expect("should parse valid EVM provider TOML");
    let provider_name = name
        .parse::<ProviderName>()
        .expect("should be valid provider name");
    (provider_name, config)
}

#[tokio::test]
async fn find_providers_with_multiple_providers_returns_in_lexicographic_order() {
    //* Given
    let (registry, _store) = create_test_providers_store();

    // Register providers in non-alphabetical order
    for name in ["charlie", "alpha", "bravo"] {
        let (provider_name, config) = create_test_evm_provider(name);
        registry
            .register(provider_name, config)
            .await
            .expect("should register provider");
    }

    let network = "mainnet".parse().expect("should parse valid network id");

    //* When
    let providers = registry.find_providers("evm-rpc", &network).await;

    //* Then
    let names: Vec<&str> = providers.iter().map(|(n, _)| n.as_ref()).collect();
    assert_eq!(names, vec!["alpha", "bravo", "charlie"]);
}
