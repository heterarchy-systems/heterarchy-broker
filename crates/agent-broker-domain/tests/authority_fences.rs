use std::error::Error;

use agent_broker_domain::commands::{
    BrokerCommand, RecordAuthorityFenceCommand, ReleaseAuthorityFenceCommand,
};
use agent_broker_domain::results::{BrokerMutationResult, StateChangeSet};
use agent_broker_domain::{
    AuthorityFence, AuthorityFenceError, AuthorityFenceKey, AuthorityId, AuthorityOwnerId,
    AuthorityRevision, BrokerStateMachine,
};

const DEFAULT_MAX: usize = 4_096;

fn key(value: &str) -> Result<AuthorityFenceKey, Box<dyn Error>> {
    Ok(AuthorityFenceKey::new(value)?)
}

fn revision(value: u64) -> Result<AuthorityRevision, Box<dyn Error>> {
    Ok(AuthorityRevision::new(value)?)
}

fn authority(value: &str) -> Result<AuthorityId, Box<dyn Error>> {
    Ok(AuthorityId::new(value)?)
}

fn owner(value: &str) -> Result<AuthorityOwnerId, Box<dyn Error>> {
    Ok(AuthorityOwnerId::new(value)?)
}

fn record_command(
    key: &AuthorityFenceKey,
    revision: AuthorityRevision,
    authority_id: &AuthorityId,
    owner_instance_id: &AuthorityOwnerId,
) -> RecordAuthorityFenceCommand {
    RecordAuthorityFenceCommand {
        key: key.clone(),
        revision,
        authority_id: authority_id.clone(),
        owner_instance_id: owner_instance_id.clone(),
        max_authority_fences: DEFAULT_MAX,
    }
}

fn release_command(
    key: &AuthorityFenceKey,
    expected_revision: AuthorityRevision,
    owner_instance_id: &AuthorityOwnerId,
) -> ReleaseAuthorityFenceCommand {
    ReleaseAuthorityFenceCommand {
        key: key.clone(),
        expected_revision,
        owner_instance_id: owner_instance_id.clone(),
    }
}

fn assert_recorded(result: BrokerMutationResult) -> Result<AuthorityRevision, Box<dyn Error>> {
    let BrokerMutationResult::AuthorityRecorded(result) = result else {
        return Err("expected AuthorityRecorded result".into());
    };
    Ok(result.recorded_revision)
}

fn assert_released(result: BrokerMutationResult) -> Result<AuthorityRevision, Box<dyn Error>> {
    let BrokerMutationResult::AuthorityReleased(result) = result else {
        return Err("expected AuthorityReleased result".into());
    };
    Ok(result.floor_revision)
}

fn assert_authority_error(
    result: Result<
        agent_broker_domain::results::AppliedMutation,
        agent_broker_domain::StateMachineError,
    >,
    expected: &AuthorityFenceError,
) -> Result<(), Box<dyn Error>> {
    let Err(agent_broker_domain::StateMachineError::AuthorityFence(error)) = result else {
        return Err(format!("expected authority fence rejection, got {result:?}").into());
    };
    assert_eq!(&error, expected);
    Ok(())
}

#[test]
fn absent_key_records_any_revision_including_gaps() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let fence_key = key("leader:shard-7")?;

    let first = machine.apply(BrokerCommand::RecordAuthorityFence(record_command(
        &fence_key,
        revision(1)?,
        &authority("authority-a")?,
        &owner("process-a")?,
    )))?;
    assert_eq!(assert_recorded(first.result)?, revision(1)?);
    assert_eq!(machine.state().revision().get(), 1);

    // A gap is valid: the caller owns the exact monotonic record value.
    let gapped = machine.apply(BrokerCommand::RecordAuthorityFence(record_command(
        &key("leader:shard-9")?,
        revision(5_000_042)?,
        &authority("authority-b")?,
        &owner("process-b")?,
    )))?;
    assert_eq!(assert_recorded(gapped.result)?, revision(5_000_042)?);

    let stored = machine
        .state()
        .authority_fence(&fence_key)
        .ok_or("recorded fence missing")?;
    assert_eq!(
        stored,
        &AuthorityFence::Record {
            revision: revision(1)?,
            authority_id: authority("authority-a")?,
            owner_instance_id: owner("process-a")?,
        }
    );
    Ok(())
}

#[test]
fn zero_revision_is_rejected_at_the_boundary() {
    assert!(AuthorityRevision::new(0).is_err());
}

#[test]
fn exact_replay_is_idempotent_without_state_change() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let fence_key = key("leader:shard-1")?;
    let command = record_command(
        &fence_key,
        revision(7)?,
        &authority("authority-a")?,
        &owner("process-a")?,
    );
    let first = machine.apply(BrokerCommand::RecordAuthorityFence(command.clone()))?;
    let revision_after_first = machine.state().revision();
    let changes_after_first: StateChangeSet = first.changes;
    assert_eq!(changes_after_first.authority_fences.len(), 1);

    let replay = machine.apply(BrokerCommand::RecordAuthorityFence(command))?;
    assert_eq!(assert_recorded(replay.result)?, revision(7)?);
    assert!(replay.changes.is_empty());
    assert_eq!(machine.state().revision(), revision_after_first);

    let wrong_owner = machine.apply(BrokerCommand::RecordAuthorityFence(record_command(
        &fence_key,
        revision(7)?,
        &authority("authority-a")?,
        &owner("process-b")?,
    )));
    assert_authority_error(
        wrong_owner,
        &AuthorityFenceError::ReplayConflict {
            key: fence_key,
            recorded: revision(7)?,
            requested: revision(7)?,
        },
    )?;
    assert_eq!(machine.state().revision(), revision_after_first);
    Ok(())
}

#[test]
fn same_authority_different_revision_is_conflict() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let fence_key = key("leader:shard-2")?;
    machine.apply(BrokerCommand::RecordAuthorityFence(record_command(
        &fence_key,
        revision(10)?,
        &authority("authority-a")?,
        &owner("process-a")?,
    )))?;
    let revision_before = machine.state().revision();

    let conflict = machine.apply(BrokerCommand::RecordAuthorityFence(record_command(
        &fence_key,
        revision(11)?,
        &authority("authority-a")?,
        &owner("process-a")?,
    )));
    assert_authority_error(
        conflict,
        &AuthorityFenceError::ReplayConflict {
            key: fence_key.clone(),
            recorded: revision(10)?,
            requested: revision(11)?,
        },
    )?;
    assert_eq!(machine.state().revision(), revision_before);
    Ok(())
}

#[test]
fn higher_revision_from_new_authority_succeeds_and_lower_or_equal_is_stale()
-> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let fence_key = key("leader:shard-3")?;
    machine.apply(BrokerCommand::RecordAuthorityFence(record_command(
        &fence_key,
        revision(10)?,
        &authority("authority-a")?,
        &owner("process-a")?,
    )))?;

    // Equal revision from a different authority is already superseded.
    let equal = machine.apply(BrokerCommand::RecordAuthorityFence(record_command(
        &fence_key,
        revision(10)?,
        &authority("authority-b")?,
        &owner("process-b")?,
    )));
    assert_authority_error(
        equal,
        &AuthorityFenceError::Superseded {
            key: fence_key.clone(),
            recorded: revision(10)?,
            requested: revision(10)?,
        },
    )?;

    let lower = machine.apply(BrokerCommand::RecordAuthorityFence(record_command(
        &fence_key,
        revision(9)?,
        &authority("authority-b")?,
        &owner("process-b")?,
    )));
    assert_authority_error(
        lower,
        &AuthorityFenceError::Superseded {
            key: fence_key.clone(),
            recorded: revision(10)?,
            requested: revision(9)?,
        },
    )?;

    let succession = machine.apply(BrokerCommand::RecordAuthorityFence(record_command(
        &fence_key,
        revision(11)?,
        &authority("authority-b")?,
        &owner("process-b")?,
    )))?;
    assert_eq!(assert_recorded(succession.result)?, revision(11)?);
    assert_eq!(
        machine.state().authority_fence(&fence_key),
        Some(&AuthorityFence::Record {
            revision: revision(11)?,
            authority_id: authority("authority-b")?,
            owner_instance_id: owner("process-b")?,
        })
    );
    Ok(())
}

#[test]
fn release_tombstones_the_record_and_floor_blocks_aba_reuse() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let fence_key = key("leader:shard-4")?;
    machine.apply(BrokerCommand::RecordAuthorityFence(record_command(
        &fence_key,
        revision(5)?,
        &authority("authority-a")?,
        &owner("process-a")?,
    )))?;

    let wrong_owner = machine.apply(BrokerCommand::ReleaseAuthorityFence(release_command(
        &fence_key,
        revision(5)?,
        &owner("process-b")?,
    )));
    assert_authority_error(
        wrong_owner,
        &AuthorityFenceError::NotOwner {
            key: fence_key.clone(),
        },
    )?;

    let wrong_revision = machine.apply(BrokerCommand::ReleaseAuthorityFence(release_command(
        &fence_key,
        revision(4)?,
        &owner("process-a")?,
    )));
    assert_authority_error(
        wrong_revision,
        &AuthorityFenceError::Superseded {
            key: fence_key.clone(),
            recorded: revision(5)?,
            requested: revision(4)?,
        },
    )?;

    let release = machine.apply(BrokerCommand::ReleaseAuthorityFence(release_command(
        &fence_key,
        revision(5)?,
        &owner("process-a")?,
    )))?;
    assert_eq!(assert_released(release.result)?, revision(5)?);
    assert_eq!(
        machine.state().authority_fence(&fence_key),
        Some(&AuthorityFence::Released {
            revision: revision(5)?,
        })
    );

    // ABA protection: the exact released revision cannot be re-recorded by anyone.
    let aba_same = machine.apply(BrokerCommand::RecordAuthorityFence(record_command(
        &fence_key,
        revision(5)?,
        &authority("authority-a")?,
        &owner("process-a")?,
    )));
    assert_authority_error(
        aba_same,
        &AuthorityFenceError::Superseded {
            key: fence_key.clone(),
            recorded: revision(5)?,
            requested: revision(5)?,
        },
    )?;

    // Succession above the floor still works and re-creates a live record.
    let revival = machine.apply(BrokerCommand::RecordAuthorityFence(record_command(
        &fence_key,
        revision(6)?,
        &authority("authority-b")?,
        &owner("process-b")?,
    )))?;
    assert_eq!(assert_recorded(revival.result)?, revision(6)?);

    // Releasing a tombstone at the exact floor is an idempotent replay.
    let tombstone_release = machine.apply(BrokerCommand::ReleaseAuthorityFence(release_command(
        &key("leader:shard-5")?,
        revision(2)?,
        &owner("process-z")?,
    )));
    assert_authority_error(
        tombstone_release,
        &AuthorityFenceError::NotFound(key("leader:shard-5")?),
    )?;
    Ok(())
}

#[test]
fn tombstone_release_replay_is_idempotent() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let fence_key = key("leader:shard-6")?;
    machine.apply(BrokerCommand::RecordAuthorityFence(record_command(
        &fence_key,
        revision(3)?,
        &authority("authority-a")?,
        &owner("process-a")?,
    )))?;
    machine.apply(BrokerCommand::ReleaseAuthorityFence(release_command(
        &fence_key,
        revision(3)?,
        &owner("process-a")?,
    )))?;
    let revision_before = machine.state().revision();

    let replay = machine.apply(BrokerCommand::ReleaseAuthorityFence(release_command(
        &fence_key,
        revision(3)?,
        &owner("process-a")?,
    )))?;
    assert_eq!(assert_released(replay.result)?, revision(3)?);
    assert!(replay.changes.is_empty());
    assert_eq!(machine.state().revision(), revision_before);

    let higher = machine.apply(BrokerCommand::ReleaseAuthorityFence(release_command(
        &fence_key,
        revision(4)?,
        &owner("process-a")?,
    )));
    assert_authority_error(
        higher,
        &AuthorityFenceError::Superseded {
            key: fence_key,
            recorded: revision(3)?,
            requested: revision(4)?,
        },
    )?;
    Ok(())
}

#[test]
fn authority_capacity_counts_records_and_tombstones() -> Result<(), Box<dyn Error>> {
    let mut machine = BrokerStateMachine::default();
    let capacity = 2;
    for index in 0..capacity {
        machine.apply(BrokerCommand::RecordAuthorityFence(
            RecordAuthorityFenceCommand {
                key: key(&format!("capacity:shard-{index}"))?,
                revision: revision(1)?,
                authority_id: authority("authority-a")?,
                owner_instance_id: owner("process-a")?,
                max_authority_fences: capacity,
            },
        ))?;
    }
    // Tombstones retain their capacity slot.
    machine.apply(BrokerCommand::ReleaseAuthorityFence(
        ReleaseAuthorityFenceCommand {
            key: key("capacity:shard-0")?,
            expected_revision: revision(1)?,
            owner_instance_id: owner("process-a")?,
        },
    ))?;
    let rejected = machine.apply(BrokerCommand::RecordAuthorityFence(
        RecordAuthorityFenceCommand {
            key: key("capacity:shard-new")?,
            revision: revision(9)?,
            authority_id: authority("authority-b")?,
            owner_instance_id: owner("process-b")?,
            max_authority_fences: capacity,
        },
    ));
    assert_authority_error(
        rejected,
        &AuthorityFenceError::CapacityExceeded { max: capacity },
    )?;
    // Succession of an existing key never needs a new slot.
    let succession = machine.apply(BrokerCommand::RecordAuthorityFence(
        RecordAuthorityFenceCommand {
            key: key("capacity:shard-1")?,
            revision: revision(2)?,
            authority_id: authority("authority-b")?,
            owner_instance_id: owner("process-b")?,
            max_authority_fences: capacity,
        },
    ))?;
    assert_eq!(assert_recorded(succession.result)?, revision(2)?);
    Ok(())
}
