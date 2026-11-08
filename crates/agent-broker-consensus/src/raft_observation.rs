use std::collections::BTreeMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use agent_broker_application::{
    CommandSessionId, CommandSessionOwnerRecord, CommandSessionOwnerStatus,
};
use agent_broker_domain::{BrokerState, ConsumerGroupDirectory, Revision, Term};

/// Read-only projection of the committed Broker state owned by `OpenRaft`.
///
/// The Raft state machine remains the sole mutable owner. These atomics expose only scalar
/// observability needed by the synchronous `ConsensusAdapter` facade; callers can never mutate
/// authoritative state through this projection.
#[derive(Debug)]
pub(crate) struct RaftBrokerObservation {
    term: AtomicU64,
    revision: AtomicU64,
    group_directory: RwLock<ConsumerGroupDirectory>,
    session_owners: RwLock<SessionOwnerProjection>,
    has_applied_index: AtomicBool,
    applied_index: AtomicU64,
}

#[derive(Debug)]
struct SessionOwnerProjection {
    owners: BTreeMap<CommandSessionId, CommandSessionOwnerRecord>,
    applied_index: Option<u64>,
}

impl RaftBrokerObservation {
    pub(crate) fn new(state: &BrokerState, applied_index: Option<u64>) -> Self {
        Self {
            term: AtomicU64::new(state.term().get()),
            revision: AtomicU64::new(state.revision().get()),
            group_directory: RwLock::new(state.group_directory()),
            session_owners: RwLock::new(SessionOwnerProjection {
                owners: BTreeMap::new(),
                applied_index,
            }),
            has_applied_index: AtomicBool::new(applied_index.is_some()),
            applied_index: AtomicU64::new(applied_index.unwrap_or(0)),
        }
    }

    pub(crate) fn update_state(&self, state: &BrokerState) {
        let directory = state.group_directory();
        match self.group_directory.write() {
            Ok(mut current) => *current = directory,
            Err(poisoned) => *poisoned.into_inner() = directory,
        }
        self.term.store(state.term().get(), Ordering::Release);
        self.revision
            .store(state.revision().get(), Ordering::Release);
    }

    /// Publish one applied log entry with its touched owner in the same projection write.
    pub(crate) fn publish_applied_entry(
        &self,
        applied_index: u64,
        touched_owner: Option<(&CommandSessionId, Option<CommandSessionOwnerRecord>)>,
    ) {
        let mut guard = match self.session_owners.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some((session_id, record)) = touched_owner {
            match record {
                Some(record) => {
                    guard.owners.insert(session_id.clone(), record);
                }
                None => {
                    guard.owners.remove(session_id);
                }
            }
        }
        guard.applied_index = Some(applied_index);
        self.applied_index.store(applied_index, Ordering::Release);
        self.has_applied_index.store(true, Ordering::Release);
    }

    /// Replace owners and their applied index together after load or snapshot installation.
    pub(crate) fn replace_session_owner_projection(
        &self,
        owners: BTreeMap<CommandSessionId, CommandSessionOwnerRecord>,
        applied_index: Option<u64>,
    ) {
        let mut guard = match self.session_owners.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.owners = owners;
        guard.applied_index = applied_index;
        if let Some(index) = applied_index {
            self.applied_index.store(index, Ordering::Release);
            self.has_applied_index.store(true, Ordering::Release);
        } else {
            self.has_applied_index.store(false, Ordering::Release);
        }
    }

    pub(crate) fn session_owner_status(
        &self,
        session_id: &CommandSessionId,
    ) -> CommandSessionOwnerStatus {
        let guard = match self.session_owners.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        CommandSessionOwnerStatus {
            owner: guard.owners.get(session_id).cloned(),
            applied_index: guard.applied_index.unwrap_or(0),
        }
    }

    pub(crate) fn term_value(&self) -> u64 {
        self.term.load(Ordering::Acquire)
    }

    pub(crate) fn revision(&self) -> Revision {
        Revision::new(self.revision.load(Ordering::Acquire))
    }

    pub(crate) fn group_directory(&self) -> ConsumerGroupDirectory {
        match self.group_directory.read() {
            Ok(directory) => directory.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    pub(crate) fn applied_index(&self) -> Option<u64> {
        if self.has_applied_index.load(Ordering::Acquire) {
            Some(self.applied_index.load(Ordering::Acquire))
        } else {
            None
        }
    }

    pub(crate) fn validated_term(&self) -> Result<Term, agent_broker_domain::FencingValueError> {
        Term::new(self.term_value())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use agent_broker_application::{
        CommandSessionId, CommandSessionOwnerRecord, SessionOwnerEpoch, SessionOwnerInstanceId,
    };
    use agent_broker_domain::BrokerState;

    use super::RaftBrokerObservation;

    #[test]
    fn owner_status_reads_observe_one_atomic_owner_index_projection()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_id = CommandSessionId::new("owner-status-barrier-session")?;
        let old_owner = CommandSessionOwnerRecord {
            owner_epoch: SessionOwnerEpoch::new(1)?,
            owner_instance_id: Some(SessionOwnerInstanceId::new("owner-before")?),
        };
        let new_owner = CommandSessionOwnerRecord {
            owner_epoch: SessionOwnerEpoch::new(2)?,
            owner_instance_id: Some(SessionOwnerInstanceId::new("owner-after")?),
        };
        let observation = Arc::new(RaftBrokerObservation::new(
            &BrokerState::default(),
            Some(10),
        ));
        observation.replace_session_owner_projection(
            BTreeMap::from([(session_id.clone(), old_owner.clone())]),
            Some(10),
        );

        let before_publication = Arc::new(Barrier::new(2));
        let after_publication = Arc::new(Barrier::new(2));
        let reader_observation = Arc::clone(&observation);
        let reader_session_id = session_id.clone();
        let reader_before_publication = Arc::clone(&before_publication);
        let reader_after_publication = Arc::clone(&after_publication);
        let reader = thread::spawn(move || {
            let before = reader_observation.session_owner_status(&reader_session_id);
            reader_before_publication.wait();
            reader_after_publication.wait();
            let after = reader_observation.session_owner_status(&reader_session_id);
            (before, after)
        });

        before_publication.wait();
        observation.publish_applied_entry(11, Some((&session_id, Some(new_owner.clone()))));
        after_publication.wait();
        let (before, after) = reader
            .join()
            .map_err(|_| std::io::Error::other("owner-status reader panicked"))?;

        let before_is_coherent = (before.owner.as_ref() == Some(&old_owner)
            && before.applied_index == 10)
            || (before.owner.as_ref() == Some(&new_owner) && before.applied_index == 11);
        assert!(
            before_is_coherent,
            "owner-status read mixed owner {:?} with applied index {}",
            before.owner, before.applied_index
        );
        assert_eq!(after.owner.as_ref(), Some(&new_owner));
        assert_eq!(after.applied_index, 11);
        observation.publish_applied_entry(12, None);
        let after_unrelated_entry = observation.session_owner_status(&session_id);
        assert_eq!(after_unrelated_entry.owner, Some(new_owner));
        assert_eq!(after_unrelated_entry.applied_index, 12);
        Ok(())
    }

    #[test]
    fn owner_status_reads_after_projection_write_lock_releases()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_id = CommandSessionId::new("owner-status-write-barrier-session")?;
        let owner = CommandSessionOwnerRecord {
            owner_epoch: SessionOwnerEpoch::new(4)?,
            owner_instance_id: Some(SessionOwnerInstanceId::new("owner-written")?),
        };
        let observation = Arc::new(RaftBrokerObservation::new(
            &BrokerState::default(),
            Some(20),
        ));
        let mut projection = observation.session_owners.write().map_err(|error| {
            std::io::Error::other(format!("owner projection lock poisoned: {error}"))
        })?;
        let (read_attempting_sender, read_attempting_receiver) = mpsc::channel();
        let reader_observation = Arc::clone(&observation);
        let reader_session_id = session_id.clone();
        let reader = thread::spawn(move || {
            let writer_holds_projection = matches!(
                reader_observation.session_owners.try_read(),
                Err(std::sync::TryLockError::WouldBlock)
            );
            let _send_result = read_attempting_sender.send(writer_holds_projection);
            reader_observation.session_owner_status(&reader_session_id)
        });

        assert!(read_attempting_receiver.recv()?);
        projection.owners.insert(session_id, owner.clone());
        projection.applied_index = Some(21);
        observation.applied_index.store(21, Ordering::Release);
        observation.has_applied_index.store(true, Ordering::Release);
        drop(projection);

        let status = reader
            .join()
            .map_err(|_| std::io::Error::other("owner-status reader panicked"))?;
        assert_eq!(status.owner, Some(owner));
        assert_eq!(status.applied_index, 21);
        Ok(())
    }
}
