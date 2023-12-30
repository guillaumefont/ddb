use std::sync::Once;

static INIT: Once = Once::new();

use tracing_subscriber::prelude::*;

pub fn init_tracer() {
    INIT.call_once(|| {
        let tracer = opentelemetry_jaeger::new_agent_pipeline()
            .with_service_name("ddb-storage")
            .install_simple()
            .unwrap();
        let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        tracing_subscriber::registry()
            .with(opentelemetry)
            .try_init()
            .unwrap();
    });
}
