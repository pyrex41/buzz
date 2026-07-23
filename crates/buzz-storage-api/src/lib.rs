//! Backend-neutral storage contract for Buzz relays.
//!
//! This crate is the home of the storage abstraction the Hive implementation
//! plan (docs/hive-implementation-plan.md §3.1) builds out: backend-neutral
//! request DTOs plus lifecycle traits shared by every storage backend.
//!
//! The domain-store traits (`EventStore`, `ChannelStore`, `CommunityStore`,
//! `ModerationStore`, `WorkflowStore`, `AuditStore`, `SearchIndex`,
//! `MediaStore`) land here in Phase 2 as `buzz_db::Db` is split along its
//! module seams; per ADR 0001 every trait method is a whole operation that
//! owns its transaction — no backend handle, connection, or transaction type
//! ever appears in this crate's signatures.
//!
//! What lives here today:
//! - [`EventQuery`] — the backend-neutral event query DTO (moved from
//!   `buzz-db`, which re-exports it).
//! - [`StoreMaintenance`] — backend lifecycle hooks (migrations and periodic
//!   upkeep), keeping partition management, WAL checkpoints, and similar
//!   backend-internal machinery invisible to the relay.

pub mod event;
pub mod maintenance;

pub use event::EventQuery;
pub use maintenance::{StorageError, StoreMaintenance};
