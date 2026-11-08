use std::collections::BTreeMap;

use crate::{AuthorityFenceKey, AuthorityId, AuthorityOwnerId, AuthorityRevision};

/// Default bound on retained authority fence records, including tombstones.
pub const DEFAULT_MAX_AUTHORITY_FENCES: usize = 4_096;

/// Broker-authoritative record for one generic authority fence key.
///
/// A fence is either a live [`AuthorityFence::Record`] binding `revision` to one
/// `authority_id`/`owner_instance_id`, or a [`AuthorityFence::Released`] tombstone that cleared the
/// owner but retains the last revision as a permanent floor for ABA protection.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AuthorityFence {
    /// Live binding of one key to an authority incarnation.
    Record {
        /// The caller's exact monotonic record value.
        revision: AuthorityRevision,
        /// Authority identity that recorded the fence.
        authority_id: AuthorityId,
        /// Process/incarnation identity that owns the record.
        owner_instance_id: AuthorityOwnerId,
    },
    /// Tombstone left by `authority.release`; the retained revision is a floor, not a live record.
    Released {
        /// Retained floor revision; later records at or below it are rejected.
        revision: AuthorityRevision,
    },
}

impl AuthorityFence {
    /// Return the fence revision: the live record revision or the tombstone floor.
    #[must_use]
    pub const fn revision(&self) -> AuthorityRevision {
        match self {
            Self::Record { revision, .. } | Self::Released { revision } => *revision,
        }
    }
}

/// Deterministic rejection categories for authority fence transitions.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AuthorityFenceError {
    /// Same authority re-recording the same key with a different revision or
    /// owner instance.
    ReplayConflict {
        key: AuthorityFenceKey,
        recorded: AuthorityRevision,
        requested: AuthorityRevision,
    },
    /// The key already carries a record/floor at or above the requested revision, or a release
    /// expected a revision other than the recorded one.
    Superseded {
        key: AuthorityFenceKey,
        recorded: AuthorityRevision,
        requested: AuthorityRevision,
    },
    /// Release was submitted by an instance that does not own the live record.
    NotOwner { key: AuthorityFenceKey },
    /// Release targeted a key that has no record or tombstone.
    NotFound(AuthorityFenceKey),
    /// A new key was admitted beyond the bounded map capacity.
    CapacityExceeded { max: usize },
}

/// Deterministic `authority.record` transition shared by standalone, one-node, and cluster Raft.
///
/// Rules, in order:
/// 1. an absent key records `{revision, authority_id, owner_instance_id}` (capacity applies);
/// 2. the same authority and owner at the exact same revision is an idempotent replay (`Ok(None)`,
///    no change);
/// 3. the same authority with a different revision or owner is a replay-binding conflict;
/// 4. a different authority succeeds only with a strictly greater revision (succession);
///    otherwise the key is already superseded;
/// 5. a tombstoned key accepts only a strictly greater revision, which re-creates a live record.
///
/// `Ok(None)` means the map must not change (idempotent replay); `Ok(Some(fence))` is the new
/// authoritative fence to insert.
///
/// # Errors
///
/// Returns [`AuthorityFenceError`] for the rule 3/4/5 rejections and for exhausted capacity.
pub fn record_authority_fence(
    fences: &BTreeMap<AuthorityFenceKey, AuthorityFence>,
    key: AuthorityFenceKey,
    revision: AuthorityRevision,
    authority_id: AuthorityId,
    owner_instance_id: AuthorityOwnerId,
    max_fences: usize,
) -> Result<Option<AuthorityFence>, AuthorityFenceError> {
    match fences.get(&key) {
        None => {
            if fences.len() >= max_fences {
                return Err(AuthorityFenceError::CapacityExceeded { max: max_fences });
            }
            Ok(Some(AuthorityFence::Record {
                revision,
                authority_id,
                owner_instance_id,
            }))
        }
        Some(AuthorityFence::Record {
            revision: recorded,
            authority_id: recorded_authority,
            owner_instance_id: recorded_owner,
        }) => {
            if *recorded_authority == authority_id {
                if *recorded == revision && *recorded_owner == owner_instance_id {
                    Ok(None)
                } else {
                    Err(AuthorityFenceError::ReplayConflict {
                        key,
                        recorded: *recorded,
                        requested: revision,
                    })
                }
            } else if revision > *recorded {
                Ok(Some(AuthorityFence::Record {
                    revision,
                    authority_id,
                    owner_instance_id,
                }))
            } else {
                Err(AuthorityFenceError::Superseded {
                    key,
                    recorded: *recorded,
                    requested: revision,
                })
            }
        }
        Some(AuthorityFence::Released { revision: floor }) => {
            if revision > *floor {
                Ok(Some(AuthorityFence::Record {
                    revision,
                    authority_id,
                    owner_instance_id,
                }))
            } else {
                Err(AuthorityFenceError::Superseded {
                    key,
                    recorded: *floor,
                    requested: revision,
                })
            }
        }
    }
}

/// Deterministic `authority.release` transition shared by every consensus path.
///
/// A live record may be released only by its recorded `owner_instance_id` at the exact recorded
/// revision; the release tombstones the key, clearing the owner and retaining the revision as an
/// ABA floor. Releasing an already tombstoned key at the exact floor is an idempotent replay.
///
/// `Ok(None)` means the tombstone already exists (no change); `Ok(Some(fence))` is the tombstone
/// to insert.
///
/// # Errors
///
/// Returns [`AuthorityFenceError::NotFound`] for unknown keys and
/// [`AuthorityFenceError::NotOwner`]/[`AuthorityFenceError::Superseded`] for fence violations.
pub fn release_authority_fence(
    fences: &BTreeMap<AuthorityFenceKey, AuthorityFence>,
    key: AuthorityFenceKey,
    expected_revision: AuthorityRevision,
    owner_instance_id: &AuthorityOwnerId,
) -> Result<Option<AuthorityFence>, AuthorityFenceError> {
    match fences.get(&key) {
        None => Err(AuthorityFenceError::NotFound(key)),
        Some(AuthorityFence::Record {
            revision,
            owner_instance_id: recorded_owner,
            ..
        }) => {
            if recorded_owner != owner_instance_id {
                return Err(AuthorityFenceError::NotOwner { key });
            }
            if *revision != expected_revision {
                return Err(AuthorityFenceError::Superseded {
                    key,
                    recorded: *revision,
                    requested: expected_revision,
                });
            }
            Ok(Some(AuthorityFence::Released {
                revision: expected_revision,
            }))
        }
        Some(AuthorityFence::Released { revision: floor }) => {
            if *floor == expected_revision {
                Ok(None)
            } else {
                Err(AuthorityFenceError::Superseded {
                    key,
                    recorded: *floor,
                    requested: expected_revision,
                })
            }
        }
    }
}
