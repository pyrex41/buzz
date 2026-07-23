//! In-process SharedState backend vs the shared conformance suite.

use std::sync::Arc;

use buzz_state::InProcessSharedState;

#[tokio::test]
async fn inproc_shared_state_meets_the_contract() {
    buzz_backend_conformance::check_shared_state_contract(Arc::new(InProcessSharedState::new()))
        .await;
}
