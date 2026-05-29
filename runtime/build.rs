fn main() {
    #[cfg(all(
        feature = "std",
        not(feature = "skip-wasm-builder"),
        not(feature = "metadata-hash")
    ))]
    {
        substrate_wasm_builder::WasmBuilder::new()
            .with_current_project()
            .export_heap_base()
            .import_memory()
            .build();
    }
    #[cfg(all(
        feature = "std",
        not(feature = "skip-wasm-builder"),
        feature = "metadata-hash"
    ))]
    {
        substrate_wasm_builder::WasmBuilder::new()
            .with_current_project()
            .export_heap_base()
            .import_memory()
            .enable_metadata_hash("TAO", 9)
            .build();
    }
}
