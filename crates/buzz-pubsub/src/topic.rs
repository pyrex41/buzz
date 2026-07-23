//! Community-scoped event topics — re-exported from `buzz-messaging-api`.
//!
//! The topic types and wire-name codecs are transport-neutral and moved to
//! [`buzz_messaging_api::topic`] (Hive plan Phase 0). This module keeps the
//! original paths alive for existing callers.

pub use buzz_messaging_api::topic::{
    channel_key, global_key, EventTopic, EventTopicKey, TopicError, BUZZ_PREFIX,
};
