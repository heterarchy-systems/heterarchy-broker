use crate::{SessionOwnerEpoch, SessionOwnerInstanceId};

/// Committed owner identity of one command session at a linearizable read point.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommandSessionOwnerRecord {
    /// Broker-authoritative owner epoch.
    pub owner_epoch: SessionOwnerEpoch,
    /// Owner instance that acquired `owner_epoch`; absent for a session that was created by a
    /// legacy identified mutation before any acquisition.
    pub owner_instance_id: Option<SessionOwnerInstanceId>,
}

/// Read-only result of `owner.status`.
///
/// The value is evidence about committed state only. It never carries mutation authority and
/// producing it never changes the owner epoch or owner instance.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommandSessionOwnerStatus {
    /// Committed owner record, or `None` when the session has never been committed.
    pub owner: Option<CommandSessionOwnerRecord>,
    /// Replicated log index the state machine had applied when the read was confirmed. It is a
    /// monotonic committed-state revision of the replicated log for comparing two reads.
    pub applied_index: u64,
}
