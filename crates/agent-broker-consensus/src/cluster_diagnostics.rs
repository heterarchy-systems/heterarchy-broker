//! Best-effort observations, independent of consensus admission and results.
use std::collections::BTreeSet;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use agent_broker_application::{BrokerError, BrokerErrorCode};
use serde_json::{Value, json};

use super::{ClusterRaftProgress, ClusterRaftReadiness, ClusterRaftReadinessStatus};
use crate::raft_type_config::AgentBrokerRaftNodeId;

const QUEUE_CAPACITY: usize = 64;
static OUTPUT: OnceLock<Output> = OnceLock::new();

pub(super) struct Record {
    node_id: AgentBrokerRaftNodeId,
    elapsed_ms: u128,
    event: &'static str,
    fields: Value,
}

impl Record {
    fn value(self, dropped: u64) -> Value {
        json!({
            "event": self.event,
            "node_id": self.node_id,
            "elapsed_ms": self.elapsed_ms,
            "dropped_events": dropped,
            "fields": self.fields,
        })
    }
}

struct Output {
    sender: Option<SyncSender<Record>>,
    dropped: Arc<AtomicU64>,
}

impl Output {
    fn open() -> Self {
        let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let worker_dropped = Arc::clone(&dropped);
        // One diagnostics-only thread per process, never per controller/restart.
        // stderr may remain blocked: no join/drain guarantee is made at exit.
        // This explicit process-lifetime exception owns no correctness work.
        let worker = std::thread::Builder::new()
            .name("broker-diagnostics".to_owned())
            .spawn(move || {
                drain(&receiver, &worker_dropped, |record| {
                    let mut stderr = std::io::stderr().lock();
                    writeln!(stderr, "{record}")
                });
            });
        Self {
            sender: worker.ok().map(|_process_lifetime_worker| sender),
            dropped,
        }
    }

    fn push(&self, record: Record) -> bool {
        if let Some(sender) = &self.sender
            && sender.try_send(record).is_ok()
        {
            return true;
        }
        self.dropped.fetch_add(1, Ordering::Relaxed);
        false
    }
}

fn drain(
    receiver: &Receiver<Record>,
    dropped: &AtomicU64,
    mut write_record: impl FnMut(&Value) -> std::io::Result<()>,
) {
    while let Ok(record) = receiver.recv() {
        let lost = dropped.swap(0, Ordering::Relaxed);
        if write_record(&record.value(lost)).is_err() {
            dropped.fetch_add(lost.saturating_add(1), Ordering::Relaxed);
        }
    }
}

/// Admission never waits for stderr. A disabled/full sink only loses diagnostics.
pub(super) fn enqueue(record: Record) -> bool {
    OUTPUT.get_or_init(Output::open).push(record)
}

// Membership contributes to authority even when the leader and terms stay put.
// Copy the sets only when an observation is accepted by the bounded sink.
struct AuthorityObservation {
    raft_term: u64,
    leader: Option<AgentBrokerRaftNodeId>,
    broker_term: u64,
    voters: BTreeSet<AgentBrokerRaftNodeId>,
    learners: BTreeSet<AgentBrokerRaftNodeId>,
}

impl AuthorityObservation {
    fn matches(&self, progress: &ClusterRaftProgress) -> bool {
        self.raft_term == progress.raft_term
            && self.leader == progress.current_leader
            && self.broker_term == progress.broker_term.get()
            && self.voters == progress.voters
            && self.learners == progress.learners
    }
}

impl From<&ClusterRaftProgress> for AuthorityObservation {
    fn from(progress: &ClusterRaftProgress) -> Self {
        Self {
            raft_term: progress.raft_term,
            leader: progress.current_leader,
            broker_term: progress.broker_term.get(),
            voters: progress.voters.clone(),
            learners: progress.learners.clone(),
        }
    }
}

pub(super) struct Diagnostics {
    node_id: AgentBrokerRaftNodeId,
    started_at: Instant,
    last_authority: Option<AuthorityObservation>,
    last_readiness: Option<ClusterRaftReadinessStatus>,
}

impl Diagnostics {
    pub(super) fn new(node_id: AgentBrokerRaftNodeId) -> Self {
        Self {
            node_id,
            started_at: Instant::now(),
            last_authority: None,
            last_readiness: None,
        }
    }

    pub(super) fn startup_failure(
        &self,
        code: BrokerErrorCode,
        output: &mut impl FnMut(Record) -> bool,
    ) {
        self.emit(
            "broker_consensus_start_failed",
            json!({"error_code": code.as_str()}),
            output,
        );
    }

    pub(super) fn startup(
        &mut self,
        result: &Result<ClusterRaftProgress, BrokerError>,
        output: &mut impl FnMut(Record) -> bool,
    ) {
        match result {
            Ok(progress) => {
                self.emit("broker_consensus_started", json!({}), output);
                self.progress(progress, output);
            }
            Err(error) => self.startup_failure(error.code(), output),
        }
    }

    pub(super) fn progress(
        &mut self,
        progress: &ClusterRaftProgress,
        output: &mut impl FnMut(Record) -> bool,
    ) {
        if self
            .last_authority
            .as_ref()
            .is_some_and(|last| last.matches(progress))
        {
            return;
        }
        if self.emit(
            "broker_consensus_authority_observed",
            json!({
                "raft_term": progress.raft_term,
                "leader_node_id": progress.current_leader,
                "broker_term": progress.broker_term.get(),
                "voters": progress.voters,
                "learners": progress.learners,
                "committed_index": progress.committed_index,
                "applied_index": progress.applied_index,
            }),
            output,
        ) {
            self.last_authority = Some(AuthorityObservation::from(progress));
        }
    }

    pub(super) fn readiness(
        &mut self,
        readiness: &ClusterRaftReadiness,
        output: &mut impl FnMut(Record) -> bool,
    ) {
        if let Some(progress) = &readiness.progress {
            self.progress(progress, output);
        }
        if self.last_readiness == Some(readiness.status) {
            return;
        }
        if self.emit(
            "broker_consensus_readiness_changed",
            json!({"status": readiness.status.as_str()}),
            output,
        ) {
            self.last_readiness = Some(readiness.status);
        }
    }

    pub(super) fn shutdown(
        &self,
        result: &Result<(), BrokerError>,
        output: &mut impl FnMut(Record) -> bool,
    ) {
        match result {
            Ok(()) => self.emit("broker_consensus_stopped", json!({}), output),
            Err(error) => self.emit(
                "broker_consensus_stop_failed",
                json!({"error_code": error.code().as_str()}),
                output,
            ),
        };
    }

    fn emit(
        &self,
        event: &'static str,
        fields: Value,
        output: &mut impl FnMut(Record) -> bool,
    ) -> bool {
        output(Record {
            node_id: self.node_id,
            elapsed_ms: self.started_at.elapsed().as_millis(),
            event,
            fields,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_broker_domain::{Revision, Term};
    use std::collections::BTreeSet;

    fn progress() -> Result<ClusterRaftProgress, Box<dyn std::error::Error>> {
        Ok(ClusterRaftProgress {
            node_id: 1,
            raft_rpc_addr: "127.0.0.1:9000".parse()?,
            raft_rpc_queued_connections: 0,
            raft_rpc_active_connections: 0,
            raft_term: 2,
            last_log_index: Some(4),
            committed_index: Some(4),
            applied_index: Some(4),
            snapshot_index: None,
            purged_index: None,
            broker_term: Term::INITIAL,
            broker_revision: Revision::default(),
            current_leader: Some(1),
            voters: BTreeSet::from([1, 2, 3]),
            learners: BTreeSet::new(),
        })
    }

    #[test]
    fn repeated_probes_and_log_index_updates_do_not_repeat_state_events()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut diagnostics = Diagnostics::new(1);
        let mut records = Vec::new();
        let mut observation = ClusterRaftReadiness {
            status: ClusterRaftReadinessStatus::Ready,
            progress: Some(progress()?),
        };
        let mut capture = |record: Record| {
            records.push(record.value(0));
            true
        };
        diagnostics.readiness(&observation, &mut capture);
        for _ in 0..100 {
            diagnostics.readiness(&observation, &mut capture);
        }
        if let Some(progress) = &mut observation.progress {
            progress.committed_index = Some(5);
            progress.applied_index = Some(5);
        }
        diagnostics.readiness(&observation, &mut capture);
        observation.status = ClusterRaftReadinessStatus::QuorumUnavailable;
        diagnostics.readiness(&observation, &mut capture);
        if let Some(progress) = &mut observation.progress {
            progress.raft_term = 3;
            progress.current_leader = Some(2);
        }
        observation.status = ClusterRaftReadinessStatus::Follower;
        diagnostics.readiness(&observation, &mut capture);
        assert_eq!(records.len(), 5);
        assert_eq!(records[2]["fields"]["status"], "quorum_unavailable");
        assert_eq!(records[3]["fields"]["leader_node_id"], 2);
        assert_eq!(records[4]["fields"]["status"], "follower");
        assert!(records.iter().all(|record| record["node_id"] == 1));
        Ok(())
    }

    #[test]
    fn full_queue_is_nonblocking_reports_loss_and_retries_state_observation()
    -> Result<(), Box<dyn std::error::Error>> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let dropped = Arc::new(AtomicU64::new(0));
        let output = Output {
            sender: Some(sender),
            dropped: Arc::clone(&dropped),
        };
        let mut diagnostics = Diagnostics::new(1);
        diagnostics.startup_failure(BrokerErrorCode::InternalError, &mut |record| {
            output.push(record)
        });
        // No receiver drains the full queue: this must return without waiting.
        diagnostics.progress(&progress()?, &mut |record| output.push(record));
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        assert!(diagnostics.last_authority.is_none());
        let _first = receiver.try_recv()?;
        diagnostics.progress(&progress()?, &mut |record| output.push(record));
        assert!(diagnostics.last_authority.is_some());
        drop(output);
        let mut records = Vec::new();
        drain(&receiver, &dropped, |record| {
            records.push(record.clone());
            Ok(())
        });
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["dropped_events"], 1);
        assert_eq!(records[0]["event"], "broker_consensus_authority_observed");
        Ok(())
    }

    #[test]
    fn lifecycle_logs_redact_error_messages_and_count_failed_output()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut diagnostics = Diagnostics::new(1);
        let error = BrokerError::new(
            BrokerErrorCode::PersistenceError,
            "private-token secret-path",
        );
        let (sender, receiver) = mpsc::sync_channel(2);
        diagnostics.startup(&Err(error.clone()), &mut |record| {
            sender.try_send(record).is_ok()
        });
        diagnostics.shutdown(&Err(error), &mut |record| sender.try_send(record).is_ok());
        drop(sender);
        let lost = AtomicU64::new(0);
        let mut records = Vec::new();
        drain(&receiver, &lost, |record| {
            records.push(record.clone());
            Err(std::io::ErrorKind::BrokenPipe.into())
        });
        let text = serde_json::to_string(&records)?;
        assert!(!text.contains("private-token"));
        assert!(!text.contains("secret-path"));
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["event"], "broker_consensus_start_failed");
        assert_eq!(records[1]["fields"]["error_code"], "PERSISTENCE_ERROR");
        assert_eq!(lost.load(Ordering::Relaxed), 2);
        Ok(())
    }

    #[test]
    fn membership_changes_with_stable_terms_are_observed_and_retry_after_rejection()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut diagnostics = Diagnostics::new(1);
        let mut observed = progress()?;
        let mut records = Vec::new();
        diagnostics.progress(&observed, &mut |record| {
            records.push(record.value(0));
            true
        });
        observed.voters.remove(&3);
        observed.voters.insert(4);
        observed.learners.insert(5);
        diagnostics.progress(&observed, &mut |record| {
            records.push(record.value(0));
            true
        });
        assert_eq!(
            records.len(),
            2,
            "same-size voter replacement must be visible without a term change"
        );
        assert_eq!(records[1]["fields"]["voters"], json!([1, 2, 4]));
        assert_eq!(records[1]["fields"]["learners"], json!([5]));
        assert_eq!(records[1]["fields"]["raft_term"], 2);
        assert_eq!(records[1]["fields"]["leader_node_id"], 1);
        observed.applied_index = Some(5);
        diagnostics.progress(&observed, &mut |record| {
            records.push(record.value(0));
            true
        });
        assert_eq!(
            records.len(),
            2,
            "unchanged membership must not emit on every probe"
        );
        observed.learners.clear();
        diagnostics.progress(&observed, &mut |_| false);
        diagnostics.progress(&observed, &mut |record| {
            records.push(record.value(0));
            true
        });
        assert_eq!(
            records.len(),
            3,
            "rejected observation must not advance the dedup cache"
        );
        assert_eq!(records[2]["fields"]["learners"], json!([]));
        Ok(())
    }
}
