use std::error::Error;

use agent_broker_application::{BrokerErrorCode, ConsensusAdapter};
use agent_broker_consensus::StandaloneConsensusAdapter;
use agent_broker_domain::commands::{
    BrokerCommand, EnsureConsumerGroupCommand, EnsureNamespaceCommand, JoinConsumerGroupCommand,
    PublishTaskCommand,
};
use agent_broker_domain::results::StateChangeSet;
use agent_broker_domain::{
    BrokerCheckpoint, BrokerState, BrokerStateMachine, Capabilities, ConsumerGroupId, ConsumerId,
    NamespaceId, Revision, TaskId, TaskObjective, TimestampMs,
};
use agent_broker_storage::{
    BrokerStateRepository, JournalCompactionPolicy, JournaledBrokerStateRepository, RepositoryError,
};
use tempfile::tempdir;

#[derive(Debug)]
struct FakeRepository {
    checkpoint: BrokerCheckpoint,
    fail_load: bool,
    fail_commit: bool,
}

impl BrokerStateRepository for FakeRepository {
    fn load(&mut self) -> Result<BrokerCheckpoint, RepositoryError> {
        if self.fail_load {
            return Err(RepositoryError::InvalidConfiguration(
                "injected load failure",
            ));
        }
        Ok(self.checkpoint.clone())
    }

    fn commit(
        &mut self,
        state: &BrokerState,
        _changes: &StateChangeSet,
    ) -> Result<(), RepositoryError> {
        if self.fail_commit {
            return Err(RepositoryError::InvalidConfiguration(
                "injected commit failure",
            ));
        }
        self.checkpoint = state.checkpoint();
        Ok(())
    }
}

fn checkpoint_with_namespace() -> Result<BrokerCheckpoint, Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    machine.apply(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("project-a")?,
        max_namespaces: 64,
    }))?;
    Ok(machine.state().checkpoint())
}

#[test]
fn idempotent_noop_skips_persistence_even_when_repository_would_fail() -> Result<(), Box<dyn Error>>
{
    let repository = FakeRepository {
        checkpoint: checkpoint_with_namespace()?,
        fail_load: false,
        fail_commit: true,
    };
    let mut adapter = StandaloneConsensusAdapter::new(repository)?;
    let before_revision = adapter.revision();

    adapter.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("project-a")?,
        max_namespaces: 64,
    }))?;

    assert_eq!(adapter.revision(), before_revision);
    assert!(!adapter.is_poisoned());
    Ok(())
}

#[test]
fn durability_failure_poison_stops_all_later_mutations() -> Result<(), Box<dyn Error>> {
    let repository = FakeRepository {
        checkpoint: BrokerStateMachine::default().state().checkpoint(),
        fail_load: false,
        fail_commit: true,
    };
    let mut adapter = StandaloneConsensusAdapter::new(repository)?;
    let first = adapter.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("project-a")?,
        max_namespaces: 64,
    }));
    let Err(first_error) = first else {
        return Err("injected commit failure must reject the proposal".into());
    };
    assert_eq!(first_error.code(), BrokerErrorCode::PersistenceError);
    assert!(adapter.is_poisoned());
    assert_eq!(adapter.revision(), Revision::new(1));

    let second = adapter.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("project-b")?,
        max_namespaces: 64,
    }));
    let Err(second_error) = second else {
        return Err("poisoned adapter must fail-stop".into());
    };
    assert_eq!(second_error.code(), BrokerErrorCode::PersistenceError);
    assert_eq!(
        second_error.message(),
        "Standalone Broker is fail-stopped after a durability failure."
    );
    assert_eq!(adapter.revision(), Revision::new(1));
    Ok(())
}

#[test]
fn recovery_failure_maps_to_stable_persistence_error() -> Result<(), Box<dyn Error>> {
    let repository = FakeRepository {
        checkpoint: BrokerStateMachine::default().state().checkpoint(),
        fail_load: true,
        fail_commit: false,
    };
    let Err(error) = StandaloneConsensusAdapter::new(repository) else {
        return Err("recovery failure must reject adapter construction".into());
    };
    assert_eq!(error.code(), BrokerErrorCode::PersistenceError);
    Ok(())
}

#[test]
fn fsynced_standalone_mutations_survive_adapter_restart() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let snapshot_path = directory.path().join("broker-state.json");
    let policy = JournalCompactionPolicy::new(10_000, 64 * 1024 * 1024)?;
    let repository = JournaledBrokerStateRepository::new(snapshot_path.clone(), None, policy);
    let mut adapter = StandaloneConsensusAdapter::new(repository)?;
    adapter.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: NamespaceId::new("project-a")?,
        max_namespaces: 64,
    }))?;
    adapter.propose(BrokerCommand::PublishTask(PublishTaskCommand {
        namespace_id: NamespaceId::new("project-a")?,
        task_id: TaskId::new("task-1")?,
        objective: TaskObjective::new("survive standalone restart")?,
        created_at_ms: TimestampMs::new(1_000),
        max_namespace_tasks: 4_096,
    }))?;
    assert_eq!(adapter.revision(), Revision::new(2));
    drop(adapter);

    let repository = JournaledBrokerStateRepository::new(snapshot_path, None, policy);
    let restarted = StandaloneConsensusAdapter::new(repository)?;
    assert_eq!(restarted.revision(), Revision::new(2));
    assert!(
        restarted
            .state_machine()
            .state()
            .namespace(&NamespaceId::new("project-a")?)
            .is_some()
    );
    assert!(
        restarted
            .state_machine()
            .state()
            .task(&TaskId::new("task-1")?)
            .is_some()
    );
    Ok(())
}

#[test]
fn standalone_group_directory_is_side_effect_free_and_current() -> Result<(), Box<dyn Error>> {
    let repository = FakeRepository {
        checkpoint: BrokerStateMachine::default().state().checkpoint(),
        fail_load: false,
        fail_commit: false,
    };
    let mut adapter = StandaloneConsensusAdapter::new(repository)?;
    let namespace_id = NamespaceId::new("project-a")?;
    let group_id = ConsumerGroupId::new("backend-company")?;

    adapter.propose(BrokerCommand::EnsureNamespace(EnsureNamespaceCommand {
        namespace_id: namespace_id.clone(),
        max_namespaces: 64,
    }))?;
    adapter.propose(BrokerCommand::EnsureConsumerGroup(
        EnsureConsumerGroupCommand {
            namespace_id: namespace_id.clone(),
            group_id: group_id.clone(),
            max_namespace_groups: 64,
        },
    ))?;
    adapter.propose(BrokerCommand::JoinConsumerGroup(JoinConsumerGroupCommand {
        group_id: group_id.clone(),
        member_id: ConsumerId::new("consumer-a")?,
        capabilities: Capabilities::new(["code", "rust"])?,
        now_ms: TimestampMs::new(1_000),
        max_group_members: 64,
    }))?;

    let before_revision = adapter.revision();
    let directory = adapter.group_directory()?;
    assert_eq!(adapter.revision(), before_revision);
    assert_eq!(directory.revision(), before_revision);
    let Some(group) = directory.group(&group_id) else {
        return Err("group directory must contain the committed Company".into());
    };
    assert_eq!(group.namespace_id(), &namespace_id);
    assert_eq!(group.generation().get(), 1);
    assert_eq!(group.consumer_count(), 1);
    Ok(())
}

#[test]
fn journaled_authority_fences_survive_restart_and_keep_their_floor() -> Result<(), Box<dyn Error>> {
    use agent_broker_domain::commands::{
        RecordAuthorityFenceCommand, ReleaseAuthorityFenceCommand,
    };
    use agent_broker_domain::results::BrokerMutationResult;
    use agent_broker_domain::{
        AuthorityFence, AuthorityFenceKey, AuthorityId, AuthorityOwnerId, AuthorityRevision,
    };

    let fence_key = AuthorityFenceKey::new("leader:shard-a")?;
    let record =
        |revision: u64, authority: &str, owner: &str| -> Result<BrokerCommand, Box<dyn Error>> {
            Ok(BrokerCommand::RecordAuthorityFence(
                RecordAuthorityFenceCommand {
                    key: fence_key.clone(),
                    revision: AuthorityRevision::new(revision)?,
                    authority_id: AuthorityId::new(authority)?,
                    owner_instance_id: AuthorityOwnerId::new(owner)?,
                    max_authority_fences: 4_096,
                },
            ))
        };

    let directory = tempdir()?;
    let snapshot_path = directory.path().join("authority-state.json");
    let policy = JournalCompactionPolicy::new(10_000, 64 * 1024 * 1024)?;
    let repository = JournaledBrokerStateRepository::new(snapshot_path.clone(), None, policy);
    let mut adapter = StandaloneConsensusAdapter::new(repository)?;
    adapter.propose(record(5, "authority-a", "process-a")?)?;
    let revision_after_record = adapter.revision();
    adapter.propose(BrokerCommand::ReleaseAuthorityFence(
        ReleaseAuthorityFenceCommand {
            key: fence_key.clone(),
            expected_revision: AuthorityRevision::new(5)?,
            owner_instance_id: AuthorityOwnerId::new("process-a")?,
        },
    ))?;
    let revision_after_release = adapter.revision();
    assert_eq!(
        revision_after_release.get(),
        revision_after_record.get() + 1
    );
    drop(adapter);

    // Restart from the same journal: the tombstone floor survives the process.
    let repository = JournaledBrokerStateRepository::new(snapshot_path.clone(), None, policy);
    let mut restarted = StandaloneConsensusAdapter::new(repository)?;
    assert_eq!(restarted.revision(), revision_after_release);
    assert_eq!(
        restarted
            .state_machine()
            .state()
            .authority_fence(&fence_key),
        Some(&AuthorityFence::Released {
            revision: AuthorityRevision::new(5)?,
        })
    );

    // ABA protection survives the restart: revision <= floor stays fenced.
    let stale = restarted.propose(record(5, "authority-a", "process-a")?);
    let Err(error) = stale else {
        return Err("released floor must fence an equal revision after restart".into());
    };
    assert_eq!(error.code(), BrokerErrorCode::StaleFence);

    // Succession above the floor is still accepted and persisted.
    let succession = restarted.propose(record(6, "authority-b", "process-b")?)?;
    let BrokerMutationResult::AuthorityRecorded(recorded) = succession else {
        return Err("expected AuthorityRecorded result".into());
    };
    assert_eq!(recorded.recorded_revision, AuthorityRevision::new(6)?);
    drop(restarted);

    let repository = JournaledBrokerStateRepository::new(snapshot_path.clone(), None, policy);
    let final_adapter = StandaloneConsensusAdapter::new(repository)?;
    assert_eq!(
        final_adapter
            .state_machine()
            .state()
            .authority_fence(&fence_key),
        Some(&AuthorityFence::Record {
            revision: AuthorityRevision::new(6)?,
            authority_id: AuthorityId::new("authority-b")?,
            owner_instance_id: AuthorityOwnerId::new("process-b")?,
        })
    );
    Ok(())
}
