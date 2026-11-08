use crate::{
    AuthorityFenceKey, AuthorityId, AuthorityOwnerId, AuthorityRevision, Capabilities,
    ConsumerGroupId, ConsumerId, Generation, LeaseEpoch, LeaseId, NamespaceId, TaskId,
    TaskObjective, TaskResult, Term, TimestampMs,
};

/// Idempotently ensure one project/work namespace exists.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsureNamespaceCommand {
    /// Namespace to ensure.
    pub namespace_id: NamespaceId,
    /// Maximum retained namespaces for new-namespace admission.
    pub max_namespaces: usize,
}

/// Idempotently publish one task into a namespace.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PublishTaskCommand {
    /// Owning namespace.
    pub namespace_id: NamespaceId,
    /// Stable task identity.
    pub task_id: TaskId,
    /// Validated task objective.
    pub objective: TaskObjective,
    /// Explicit deterministic publication timestamp.
    pub created_at_ms: TimestampMs,
    /// Maximum retained tasks in this namespace for new-task admission.
    pub max_namespace_tasks: usize,
}

/// Idempotently ensure one Consumer Group exists in a namespace.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsureConsumerGroupCommand {
    /// Owning namespace.
    pub namespace_id: NamespaceId,
    /// Stable Consumer Group identity.
    pub group_id: ConsumerGroupId,
    /// Maximum Consumer Groups in this namespace for new-group admission.
    pub max_namespace_groups: usize,
}

/// Join or idempotently refresh one Consumer Group member.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JoinConsumerGroupCommand {
    /// Consumer Group to join.
    pub group_id: ConsumerGroupId,
    /// Stable member identity.
    pub member_id: ConsumerId,
    /// Normalized provider-neutral capabilities.
    pub capabilities: Capabilities,
    /// Explicit join/refresh timestamp.
    pub now_ms: TimestampMs,
    /// Maximum members for new-member admission.
    pub max_group_members: usize,
}

/// Record one heartbeat against the current Consumer Group generation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HeartbeatCommand {
    /// Consumer Group identity.
    pub group_id: ConsumerGroupId,
    /// Consumer identity.
    pub member_id: ConsumerId,
    /// Generation observed by the member.
    pub expected_generation: Generation,
    /// Explicit heartbeat timestamp.
    pub now_ms: TimestampMs,
}

/// Leave one Consumer Group member from the current generation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LeaveConsumerGroupCommand {
    /// Consumer Group identity.
    pub group_id: ConsumerGroupId,
    /// Consumer identity.
    pub member_id: ConsumerId,
    /// Generation observed by the member.
    pub expected_generation: Generation,
}

/// Reap stale Consumer Group members globally in heartbeat order.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ReapStaleMembersCommand {
    /// Members with heartbeat at or before this timestamp are eligible.
    pub stale_before_ms: TimestampMs,
    /// Maximum number of members to remove in one bounded command.
    pub max_members: usize,
}

/// Claim the oldest ready Task in the member's Consumer Group namespace.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimTaskCommand {
    pub group_id: ConsumerGroupId,
    pub member_id: ConsumerId,
    pub expected_term: Term,
    pub expected_generation: Generation,
    pub lease_id: LeaseId,
    pub now_ms: TimestampMs,
    pub lease_duration_ms: u64,
}

/// Renew one matching active Task lease.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RenewTaskLeaseCommand {
    pub task_id: TaskId,
    pub group_id: ConsumerGroupId,
    pub member_id: ConsumerId,
    pub expected_term: Term,
    pub expected_generation: Generation,
    pub expected_lease_epoch: LeaseEpoch,
    pub lease_id: LeaseId,
    pub now_ms: TimestampMs,
    pub lease_duration_ms: u64,
}

/// Complete one matching active Task lease.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompleteTaskCommand {
    pub task_id: TaskId,
    pub group_id: ConsumerGroupId,
    pub member_id: ConsumerId,
    pub expected_term: Term,
    pub expected_generation: Generation,
    pub expected_lease_epoch: LeaseEpoch,
    pub lease_id: LeaseId,
    pub result: TaskResult,
    pub completed_at_ms: TimestampMs,
}

/// Prune retained completed Tasks at or before a deterministic cutoff.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct PruneCompletedTasksCommand {
    pub completed_before_ms: TimestampMs,
    pub max_tasks: usize,
}

/// Advance the Broker term after consensus/leader control has validated the transition.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AdvanceTermCommand {
    pub expected_term: Term,
    pub new_term: Term,
}

/// Record or succeed one generic authority fence binding.
///
/// This is a provider-neutral fencing record only: it carries no election policy and no
/// command-session ownership semantics. `revision` is the caller's exact monotonic record value
/// (arbitrary values >= 1, including gaps), not a compare-and-increment counter.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RecordAuthorityFenceCommand {
    /// Bounded authority fence key.
    pub key: AuthorityFenceKey,
    /// Caller's exact monotonic record value.
    pub revision: AuthorityRevision,
    /// Authority identity claiming the key.
    pub authority_id: AuthorityId,
    /// Process/incarnation identity recorded as the owner.
    pub owner_instance_id: AuthorityOwnerId,
    /// Server-owned bound on retained records, including tombstones.
    pub max_authority_fences: usize,
}

/// Release one generic authority fence, tombstoning the key at its recorded revision.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReleaseAuthorityFenceCommand {
    /// Bounded authority fence key.
    pub key: AuthorityFenceKey,
    /// Exact revision the releaser believes is currently recorded.
    pub expected_revision: AuthorityRevision,
    /// Process/incarnation identity that must currently own the record.
    pub owner_instance_id: AuthorityOwnerId,
}

/// Deterministic Broker commands currently implemented by the Rust migration.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BrokerCommand {
    /// Ensure namespace.
    EnsureNamespace(EnsureNamespaceCommand),
    /// Publish task.
    PublishTask(PublishTaskCommand),
    /// Ensure Consumer Group.
    EnsureConsumerGroup(EnsureConsumerGroupCommand),
    /// Join Consumer Group member.
    JoinConsumerGroup(JoinConsumerGroupCommand),
    /// Record member heartbeat.
    Heartbeat(HeartbeatCommand),
    /// Leave Consumer Group member.
    LeaveConsumerGroup(LeaveConsumerGroupCommand),
    /// Reap globally stale Consumer Group members.
    ReapStaleMembers(ReapStaleMembersCommand),
    /// Claim the oldest ready Task.
    ClaimTask(ClaimTaskCommand),
    /// Renew an active Task lease.
    RenewTaskLease(RenewTaskLeaseCommand),
    /// Complete an active Task lease.
    CompleteTask(CompleteTaskCommand),
    /// Prune completed Task retention.
    PruneCompletedTasks(PruneCompletedTasksCommand),
    /// Advance Broker term.
    AdvanceTerm(AdvanceTermCommand),
    /// Record or succeed one generic authority fence.
    RecordAuthorityFence(RecordAuthorityFenceCommand),
    /// Release one generic authority fence into an ABA-protecting tombstone.
    ReleaseAuthorityFence(ReleaseAuthorityFenceCommand),
}
