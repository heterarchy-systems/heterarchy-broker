use std::collections::BTreeMap;

use agent_broker_application::{
    BrokerErrorDisposition, CommandIdentity, CommandSequence, CommandSessionId,
    CommandSessionOwnerRecord, CommandSessionOwnerStatus, SessionOwnerEpoch,
    SessionOwnerInstanceId,
};
use agent_broker_domain::{AuthorityFenceKey, AuthorityId, AuthorityOwnerId, AuthorityRevision};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::{
    BrokerRequest, BrokerResponse, Operation, PROTOCOL_VERSION, ProtocolCodecError, RequestId,
    ResponseDecodeError, SuccessPayload, decode_request_with_limit,
    decode_response_for_operation_with_limit, encode_request_with_limit,
    encode_response_with_limit,
};

pub const PROTOCOL_VERSION_V3: u32 = 3;
pub const ACQUIRE_COMMAND_SESSION_OWNER_OPERATION: &str = "acquire_command_session_owner";
pub const OWNER_STATUS_OPERATION: &str = "owner.status";
pub const AUTHORITY_RECORD_OPERATION: &str = "authority.record";
pub const AUTHORITY_RELEASE_OPERATION: &str = "authority.release";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerAcquisitionRequestV3 {
    request_id: RequestId,
    session_id: CommandSessionId,
    expected_owner_epoch: SessionOwnerEpoch,
    owner_instance_id: SessionOwnerInstanceId,
}

impl OwnerAcquisitionRequestV3 {
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        session_id: CommandSessionId,
        expected_owner_epoch: SessionOwnerEpoch,
        owner_instance_id: SessionOwnerInstanceId,
    ) -> Self {
        Self {
            request_id,
            session_id,
            expected_owner_epoch,
            owner_instance_id,
        }
    }

    #[must_use]
    pub const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    #[must_use]
    pub const fn session_id(&self) -> &CommandSessionId {
        &self.session_id
    }

    #[must_use]
    pub const fn expected_owner_epoch(&self) -> SessionOwnerEpoch {
        self.expected_owner_epoch
    }

    #[must_use]
    pub const fn owner_instance_id(&self) -> &SessionOwnerInstanceId {
        &self.owner_instance_id
    }
}

/// Read-only `owner.status` request.
///
/// `caller_instance_id` is informational: it is echoed into the envelope's required
/// `owner_instance_id` field and is never used for authorization or ownership.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerStatusRequestV3 {
    request: RequestId,
    session: CommandSessionId,
    caller_instance: SessionOwnerInstanceId,
}

impl OwnerStatusRequestV3 {
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        session_id: CommandSessionId,
        caller_instance_id: SessionOwnerInstanceId,
    ) -> Self {
        Self {
            request: request_id,
            session: session_id,
            caller_instance: caller_instance_id,
        }
    }

    #[must_use]
    pub const fn request_id(&self) -> &RequestId {
        &self.request
    }

    #[must_use]
    pub const fn session_id(&self) -> &CommandSessionId {
        &self.session
    }

    #[must_use]
    pub const fn caller_instance_id(&self) -> &SessionOwnerInstanceId {
        &self.caller_instance
    }
}

/// Generic authority fence record request (`authority.record`).
///
/// `revision` is the caller's exact monotonic record value, not a compare-and-increment counter.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthorityRecordRequestV3 {
    request_id: RequestId,
    key: AuthorityFenceKey,
    revision: AuthorityRevision,
    authority_id: AuthorityId,
    owner_instance_id: AuthorityOwnerId,
}

impl AuthorityRecordRequestV3 {
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        key: AuthorityFenceKey,
        revision: AuthorityRevision,
        authority_id: AuthorityId,
        owner_instance_id: AuthorityOwnerId,
    ) -> Self {
        Self {
            request_id,
            key,
            revision,
            authority_id,
            owner_instance_id,
        }
    }

    #[must_use]
    pub const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    #[must_use]
    pub const fn key(&self) -> &AuthorityFenceKey {
        &self.key
    }

    #[must_use]
    pub const fn revision(&self) -> AuthorityRevision {
        self.revision
    }

    #[must_use]
    pub const fn authority_id(&self) -> &AuthorityId {
        &self.authority_id
    }

    #[must_use]
    pub const fn owner_instance_id(&self) -> &AuthorityOwnerId {
        &self.owner_instance_id
    }
}

/// Generic authority fence release request (`authority.release`).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AuthorityReleaseRequestV3 {
    request_id: RequestId,
    key: AuthorityFenceKey,
    expected_revision: AuthorityRevision,
    owner_instance_id: AuthorityOwnerId,
}

impl AuthorityReleaseRequestV3 {
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        key: AuthorityFenceKey,
        expected_revision: AuthorityRevision,
        owner_instance_id: AuthorityOwnerId,
    ) -> Self {
        Self {
            request_id,
            key,
            expected_revision,
            owner_instance_id,
        }
    }

    #[must_use]
    pub const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    #[must_use]
    pub const fn key(&self) -> &AuthorityFenceKey {
        &self.key
    }

    #[must_use]
    pub const fn expected_revision(&self) -> AuthorityRevision {
        self.expected_revision
    }

    #[must_use]
    pub const fn owner_instance_id(&self) -> &AuthorityOwnerId {
        &self.owner_instance_id
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnerIdentifiedBrokerRequestV3 {
    identity: CommandIdentity,
    request: BrokerRequest,
}

impl OwnerIdentifiedBrokerRequestV3 {
    /// Construct an owner-aware mutation-only request.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolCodecError`] when owner instance metadata is absent or `request` is health.
    pub fn new(
        identity: CommandIdentity,
        request: BrokerRequest,
    ) -> Result<Self, ProtocolCodecError> {
        if identity.owner_instance_id().is_none() {
            return Err(invalid_request(
                "protocol-v3 owner-aware mutation requires command_session_owner_instance_id",
            ));
        }
        if request.operation() == Operation::Health {
            return Err(invalid_request(
                "protocol-v3 owner-aware requests are mutation-only; health remains protocol-v1/read-only",
            ));
        }
        Ok(Self { identity, request })
    }

    #[must_use]
    pub const fn identity(&self) -> &CommandIdentity {
        &self.identity
    }

    #[must_use]
    pub const fn request(&self) -> &BrokerRequest {
        &self.request
    }

    #[must_use]
    pub fn request_id(&self) -> &RequestId {
        self.request.request_id()
    }

    #[must_use]
    pub const fn operation(&self) -> Operation {
        self.request.operation()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BrokerRequestV3 {
    AcquireOwner(OwnerAcquisitionRequestV3),
    OwnerStatus(OwnerStatusRequestV3),
    Mutation(OwnerIdentifiedBrokerRequestV3),
    AuthorityRecord(AuthorityRecordRequestV3),
    AuthorityRelease(AuthorityReleaseRequestV3),
}

impl BrokerRequestV3 {
    #[must_use]
    pub fn request_id(&self) -> &RequestId {
        match self {
            Self::AcquireOwner(request) => request.request_id(),
            Self::OwnerStatus(request) => request.request_id(),
            Self::Mutation(request) => request.request_id(),
            Self::AuthorityRecord(request) => request.request_id(),
            Self::AuthorityRelease(request) => request.request_id(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestEnvelopeV3 {
    version: u32,
    request_id: String,
    operation: String,
    #[serde(default)]
    command_session_id: Option<String>,
    #[serde(default)]
    expected_owner_epoch: Option<u64>,
    #[serde(default)]
    owner_epoch: Option<u64>,
    owner_instance_id: String,
    #[serde(default)]
    command_sequence: Option<u64>,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    revision: Option<u64>,
    #[serde(default)]
    expected_revision: Option<u64>,
    #[serde(default)]
    authority_id: Option<String>,
    payload: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseEnvelopeV3 {
    version: u32,
    request_id: String,
    ok: bool,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<ResponseErrorV3>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseErrorV3 {
    code: String,
    message: String,
    disposition: String,
}

/// Decode one strict protocol-v3 owner acquisition or owner-aware mutation.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] for malformed JSON, frame overflow, unsupported versions,
/// invalid owner/session identity, invalid field combinations, or invalid Broker mutation payloads.
pub fn decode_request_v3_with_limit(
    frame: &[u8],
    max_frame_bytes: usize,
) -> Result<BrokerRequestV3, ProtocolCodecError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: RequestEnvelopeV3 = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Request frame must be valid protocol-v3 JSON: {error}"
        ))
    })?;
    if envelope.version != PROTOCOL_VERSION_V3 {
        return Err(invalid_request(format!(
            "Unsupported protocol-v3 request version {}.",
            envelope.version
        )));
    }
    if envelope.operation == ACQUIRE_COMMAND_SESSION_OWNER_OPERATION {
        return decode_owner_acquisition(envelope);
    }
    if envelope.operation == OWNER_STATUS_OPERATION {
        return decode_owner_status(envelope);
    }
    if envelope.operation == AUTHORITY_RECORD_OPERATION {
        return decode_authority_record(envelope);
    }
    if envelope.operation == AUTHORITY_RELEASE_OPERATION {
        return decode_authority_release(envelope);
    }
    decode_owner_mutation(envelope, max_frame_bytes)
}

fn require_absent<T>(field: Option<&T>, message: &str) -> Result<(), ProtocolCodecError> {
    if field.is_some() {
        return Err(invalid_request(message));
    }
    Ok(())
}

fn decode_owner_status(envelope: RequestEnvelopeV3) -> Result<BrokerRequestV3, ProtocolCodecError> {
    require_absent(
        envelope.expected_owner_epoch.as_ref(),
        "protocol-v3 owner.status must not contain expected_owner_epoch",
    )?;
    require_absent(
        envelope.owner_epoch.as_ref(),
        "protocol-v3 owner.status must not contain owner_epoch",
    )?;
    require_absent(
        envelope.command_sequence.as_ref(),
        "protocol-v3 owner.status must not contain command_sequence",
    )?;
    require_absent(
        envelope.key.as_ref(),
        "protocol-v3 owner.status must not contain authority fence fields",
    )?;
    require_absent(
        envelope.revision.as_ref(),
        "protocol-v3 owner.status must not contain authority fence fields",
    )?;
    require_absent(
        envelope.expected_revision.as_ref(),
        "protocol-v3 owner.status must not contain authority fence fields",
    )?;
    require_absent(
        envelope.authority_id.as_ref(),
        "protocol-v3 owner.status must not contain authority fence fields",
    )?;
    if !envelope.payload.as_object().is_some_and(Map::is_empty) {
        return Err(invalid_request(
            "protocol-v3 owner.status payload must be an empty object",
        ));
    }
    let request_id =
        RequestId::new(envelope.request_id).map_err(|error| invalid_request(error.to_string()))?;
    let session_id =
        CommandSessionId::new(envelope.command_session_id.ok_or_else(|| {
            invalid_request("protocol-v3 owner.status requires command_session_id")
        })?)
        .map_err(|error| invalid_request(error.to_string()))?;
    let caller_instance_id = SessionOwnerInstanceId::new(envelope.owner_instance_id)
        .map_err(|error| invalid_request(error.to_string()))?;
    Ok(BrokerRequestV3::OwnerStatus(OwnerStatusRequestV3::new(
        request_id,
        session_id,
        caller_instance_id,
    )))
}

fn decode_authority_record(
    envelope: RequestEnvelopeV3,
) -> Result<BrokerRequestV3, ProtocolCodecError> {
    require_absent(
        envelope.command_session_id.as_ref(),
        "protocol-v3 authority.record must not contain command_session_id",
    )?;
    require_absent(
        envelope.expected_owner_epoch.as_ref(),
        "protocol-v3 authority.record must not contain expected_owner_epoch",
    )?;
    require_absent(
        envelope.owner_epoch.as_ref(),
        "protocol-v3 authority.record must not contain owner_epoch",
    )?;
    require_absent(
        envelope.command_sequence.as_ref(),
        "protocol-v3 authority.record must not contain command_sequence",
    )?;
    require_absent(
        envelope.expected_revision.as_ref(),
        "protocol-v3 authority.record must not contain expected_revision",
    )?;
    if !envelope.payload.as_object().is_some_and(Map::is_empty) {
        return Err(invalid_request(
            "protocol-v3 authority.record payload must be an empty object",
        ));
    }
    let request_id =
        RequestId::new(envelope.request_id).map_err(|error| invalid_request(error.to_string()))?;
    let key = AuthorityFenceKey::new(
        envelope
            .key
            .ok_or_else(|| invalid_request("protocol-v3 authority.record requires key"))?,
    )
    .map_err(|error| invalid_request(error.to_string()))?;
    let revision = AuthorityRevision::new(
        envelope
            .revision
            .ok_or_else(|| invalid_request("protocol-v3 authority.record requires revision"))?,
    )
    .map_err(|error| invalid_request(error.to_string()))?;
    let authority_id =
        AuthorityId::new(envelope.authority_id.ok_or_else(|| {
            invalid_request("protocol-v3 authority.record requires authority_id")
        })?)
        .map_err(|error| invalid_request(error.to_string()))?;
    let owner_instance_id = AuthorityOwnerId::new(envelope.owner_instance_id)
        .map_err(|error| invalid_request(error.to_string()))?;
    Ok(BrokerRequestV3::AuthorityRecord(
        AuthorityRecordRequestV3::new(request_id, key, revision, authority_id, owner_instance_id),
    ))
}

fn decode_authority_release(
    envelope: RequestEnvelopeV3,
) -> Result<BrokerRequestV3, ProtocolCodecError> {
    require_absent(
        envelope.command_session_id.as_ref(),
        "protocol-v3 authority.release must not contain command_session_id",
    )?;
    require_absent(
        envelope.expected_owner_epoch.as_ref(),
        "protocol-v3 authority.release must not contain expected_owner_epoch",
    )?;
    require_absent(
        envelope.owner_epoch.as_ref(),
        "protocol-v3 authority.release must not contain owner_epoch",
    )?;
    require_absent(
        envelope.command_sequence.as_ref(),
        "protocol-v3 authority.release must not contain command_sequence",
    )?;
    require_absent(
        envelope.revision.as_ref(),
        "protocol-v3 authority.release must not contain revision",
    )?;
    require_absent(
        envelope.authority_id.as_ref(),
        "protocol-v3 authority.release must not contain authority_id",
    )?;
    if !envelope.payload.as_object().is_some_and(Map::is_empty) {
        return Err(invalid_request(
            "protocol-v3 authority.release payload must be an empty object",
        ));
    }
    let request_id =
        RequestId::new(envelope.request_id).map_err(|error| invalid_request(error.to_string()))?;
    let key = AuthorityFenceKey::new(
        envelope
            .key
            .ok_or_else(|| invalid_request("protocol-v3 authority.release requires key"))?,
    )
    .map_err(|error| invalid_request(error.to_string()))?;
    let expected_revision =
        AuthorityRevision::new(envelope.expected_revision.ok_or_else(|| {
            invalid_request("protocol-v3 authority.release requires expected_revision")
        })?)
        .map_err(|error| invalid_request(error.to_string()))?;
    let owner_instance_id = AuthorityOwnerId::new(envelope.owner_instance_id)
        .map_err(|error| invalid_request(error.to_string()))?;
    Ok(BrokerRequestV3::AuthorityRelease(
        AuthorityReleaseRequestV3::new(request_id, key, expected_revision, owner_instance_id),
    ))
}

fn decode_owner_acquisition(
    envelope: RequestEnvelopeV3,
) -> Result<BrokerRequestV3, ProtocolCodecError> {
    require_absent(
        envelope.owner_epoch.as_ref(),
        "protocol-v3 owner acquisition must not contain owner_epoch or command_sequence",
    )?;
    require_absent(
        envelope.command_sequence.as_ref(),
        "protocol-v3 owner acquisition must not contain owner_epoch or command_sequence",
    )?;
    require_absent(
        envelope.key.as_ref(),
        "protocol-v3 owner acquisition must not contain authority fence fields",
    )?;
    require_absent(
        envelope.revision.as_ref(),
        "protocol-v3 owner acquisition must not contain authority fence fields",
    )?;
    require_absent(
        envelope.expected_revision.as_ref(),
        "protocol-v3 owner acquisition must not contain authority fence fields",
    )?;
    require_absent(
        envelope.authority_id.as_ref(),
        "protocol-v3 owner acquisition must not contain authority fence fields",
    )?;
    let expected_owner_epoch = envelope.expected_owner_epoch.ok_or_else(|| {
        invalid_request("protocol-v3 owner acquisition requires expected_owner_epoch")
    })?;
    if !envelope.payload.as_object().is_some_and(Map::is_empty) {
        return Err(invalid_request(
            "protocol-v3 owner acquisition payload must be an empty object",
        ));
    }
    let request_id =
        RequestId::new(envelope.request_id).map_err(|error| invalid_request(error.to_string()))?;
    let session_id = CommandSessionId::new(envelope.command_session_id.ok_or_else(|| {
        invalid_request("protocol-v3 owner acquisition requires command_session_id")
    })?)
    .map_err(|error| invalid_request(error.to_string()))?;
    let expected_owner_epoch = SessionOwnerEpoch::new(expected_owner_epoch)
        .map_err(|error| invalid_request(error.to_string()))?;
    let owner_instance_id = SessionOwnerInstanceId::new(envelope.owner_instance_id)
        .map_err(|error| invalid_request(error.to_string()))?;
    Ok(BrokerRequestV3::AcquireOwner(
        OwnerAcquisitionRequestV3::new(
            request_id,
            session_id,
            expected_owner_epoch,
            owner_instance_id,
        ),
    ))
}

fn decode_owner_mutation(
    envelope: RequestEnvelopeV3,
    max_frame_bytes: usize,
) -> Result<BrokerRequestV3, ProtocolCodecError> {
    if envelope.expected_owner_epoch.is_some() {
        return Err(invalid_request(
            "protocol-v3 owner-aware mutation must not contain expected_owner_epoch",
        ));
    }
    require_absent(
        envelope.key.as_ref(),
        "protocol-v3 owner-aware mutation must not contain authority fence fields",
    )?;
    require_absent(
        envelope.revision.as_ref(),
        "protocol-v3 owner-aware mutation must not contain authority fence fields",
    )?;
    require_absent(
        envelope.expected_revision.as_ref(),
        "protocol-v3 owner-aware mutation must not contain authority fence fields",
    )?;
    require_absent(
        envelope.authority_id.as_ref(),
        "protocol-v3 owner-aware mutation must not contain authority fence fields",
    )?;
    let owner_epoch = envelope
        .owner_epoch
        .ok_or_else(|| invalid_request("protocol-v3 owner-aware mutation requires owner_epoch"))?;
    let command_sequence = envelope.command_sequence.ok_or_else(|| {
        invalid_request("protocol-v3 owner-aware mutation requires command_sequence")
    })?;
    let session_id = CommandSessionId::new(envelope.command_session_id.ok_or_else(|| {
        invalid_request("protocol-v3 owner-aware mutation requires command_session_id")
    })?)
    .map_err(|error| invalid_request(error.to_string()))?;
    let owner_epoch =
        SessionOwnerEpoch::new(owner_epoch).map_err(|error| invalid_request(error.to_string()))?;
    let owner_instance_id = SessionOwnerInstanceId::new(envelope.owner_instance_id)
        .map_err(|error| invalid_request(error.to_string()))?;
    let sequence = CommandSequence::new(command_sequence)
        .map_err(|error| invalid_request(error.to_string()))?;
    let identity =
        CommandIdentity::new_with_owner(session_id, owner_epoch, owner_instance_id, sequence);
    let v1 = object([
        ("operation", Value::String(envelope.operation)),
        ("payload", envelope.payload),
        ("request_id", Value::String(envelope.request_id)),
        ("version", Value::from(PROTOCOL_VERSION)),
    ]);
    let v1_frame = serialize_bounded(&v1, max_frame_bytes)?;
    let request = decode_request_with_limit(&v1_frame, max_frame_bytes)?;
    OwnerIdentifiedBrokerRequestV3::new(identity, request).map(BrokerRequestV3::Mutation)
}

/// Encode one strict protocol-v3 owner acquisition request.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] when serialization fails or the encoded frame exceeds the bound.
pub fn encode_owner_acquisition_request_with_limit(
    request: &OwnerAcquisitionRequestV3,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ProtocolCodecError> {
    let root = object([
        (
            "command_session_id",
            Value::String(request.session_id().as_str().to_owned()),
        ),
        (
            "expected_owner_epoch",
            Value::from(request.expected_owner_epoch().get()),
        ),
        (
            "operation",
            Value::String(ACQUIRE_COMMAND_SESSION_OWNER_OPERATION.to_owned()),
        ),
        (
            "owner_instance_id",
            Value::String(request.owner_instance_id().as_str().to_owned()),
        ),
        ("payload", Value::Object(Map::new())),
        (
            "request_id",
            Value::String(request.request_id().as_str().to_owned()),
        ),
        ("version", Value::from(PROTOCOL_VERSION_V3)),
    ]);
    serialize_bounded(&root, max_frame_bytes)
}

/// Encode one strict protocol-v3 read-only `owner.status` request.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] when serialization fails or the encoded frame exceeds the bound.
pub fn encode_owner_status_request_with_limit(
    request: &OwnerStatusRequestV3,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ProtocolCodecError> {
    let root = object([
        (
            "command_session_id",
            Value::String(request.session_id().as_str().to_owned()),
        ),
        (
            "operation",
            Value::String(OWNER_STATUS_OPERATION.to_owned()),
        ),
        (
            "owner_instance_id",
            Value::String(request.caller_instance_id().as_str().to_owned()),
        ),
        ("payload", Value::Object(Map::new())),
        (
            "request_id",
            Value::String(request.request_id().as_str().to_owned()),
        ),
        ("version", Value::from(PROTOCOL_VERSION_V3)),
    ]);
    serialize_bounded(&root, max_frame_bytes)
}

/// Encode one strict protocol-v3 owner-aware mutation request.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] when the owner identity is incomplete, serialization fails, or
/// the encoded frame exceeds the bound.
pub fn encode_owner_mutation_request_with_limit(
    request: &OwnerIdentifiedBrokerRequestV3,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ProtocolCodecError> {
    let v1_frame = encode_request_with_limit(request.request(), max_frame_bytes)?;
    let mut v1: Value = serde_json::from_slice(&v1_frame)
        .map_err(|error| ProtocolCodecError::Serialization(error.to_string()))?;
    let payload = v1
        .get_mut("payload")
        .map(Value::take)
        .ok_or_else(|| invalid_request("internal protocol-v1 request encoding omitted payload"))?;
    let owner_instance_id = request.identity().owner_instance_id().ok_or_else(|| {
        invalid_request("protocol-v3 owner-aware mutation omitted owner instance identity")
    })?;
    let root = object([
        (
            "command_sequence",
            Value::from(request.identity().sequence().get()),
        ),
        (
            "command_session_id",
            Value::String(request.identity().session_id().as_str().to_owned()),
        ),
        (
            "operation",
            Value::String(request.operation().as_str().to_owned()),
        ),
        (
            "owner_epoch",
            Value::from(request.identity().owner_epoch().get()),
        ),
        (
            "owner_instance_id",
            Value::String(owner_instance_id.as_str().to_owned()),
        ),
        ("payload", payload),
        (
            "request_id",
            Value::String(request.request_id().as_str().to_owned()),
        ),
        ("version", Value::from(PROTOCOL_VERSION_V3)),
    ]);
    serialize_bounded(&root, max_frame_bytes)
}

/// Encode one strict protocol-v3 `authority.record` request.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] when serialization fails or the encoded frame exceeds the bound.
pub fn encode_authority_record_request_with_limit(
    request: &AuthorityRecordRequestV3,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ProtocolCodecError> {
    let root = object([
        (
            "authority_id",
            Value::String(request.authority_id().as_str().to_owned()),
        ),
        ("key", Value::String(request.key().as_str().to_owned())),
        (
            "operation",
            Value::String(AUTHORITY_RECORD_OPERATION.to_owned()),
        ),
        (
            "owner_instance_id",
            Value::String(request.owner_instance_id().as_str().to_owned()),
        ),
        ("payload", Value::Object(Map::new())),
        (
            "request_id",
            Value::String(request.request_id().as_str().to_owned()),
        ),
        ("revision", Value::from(request.revision().get())),
        ("version", Value::from(PROTOCOL_VERSION_V3)),
    ]);
    serialize_bounded(&root, max_frame_bytes)
}

/// Encode one strict protocol-v3 `authority.release` request.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] when serialization fails or the encoded frame exceeds the bound.
pub fn encode_authority_release_request_with_limit(
    request: &AuthorityReleaseRequestV3,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ProtocolCodecError> {
    let root = object([
        (
            "expected_revision",
            Value::from(request.expected_revision().get()),
        ),
        ("key", Value::String(request.key().as_str().to_owned())),
        (
            "operation",
            Value::String(AUTHORITY_RELEASE_OPERATION.to_owned()),
        ),
        (
            "owner_instance_id",
            Value::String(request.owner_instance_id().as_str().to_owned()),
        ),
        ("payload", Value::Object(Map::new())),
        (
            "request_id",
            Value::String(request.request_id().as_str().to_owned()),
        ),
        ("version", Value::from(PROTOCOL_VERSION_V3)),
    ]);
    serialize_bounded(&root, max_frame_bytes)
}

fn encode_owner_status_success(
    response: &BrokerResponse,
    max_frame_bytes: usize,
) -> Option<Result<Vec<u8>, ProtocolCodecError>> {
    let BrokerResponse::Success {
        request_id,
        result:
            SuccessPayload::SessionOwnerStatus {
                owner_epoch,
                owner_instance_id,
                applied_index,
            },
    } = response
    else {
        return None;
    };
    Some(serialize_bounded(
        &object([
            ("ok", Value::Bool(true)),
            ("request_id", Value::String(request_id.as_str().to_owned())),
            (
                "result",
                object([
                    ("applied_index", Value::from(*applied_index)),
                    (
                        "owner_epoch",
                        owner_epoch.map_or(Value::Null, |epoch| Value::from(epoch.get())),
                    ),
                    (
                        "owner_instance_id",
                        owner_instance_id
                            .as_ref()
                            .map_or(Value::Null, |id| Value::String(id.as_str().to_owned())),
                    ),
                    ("session_found", Value::Bool(owner_epoch.is_some())),
                ]),
            ),
            ("version", Value::from(PROTOCOL_VERSION_V3)),
        ]),
        max_frame_bytes,
    ))
}

/// Encode one Broker response using the protocol-v3 envelope.
///
/// # Errors
///
/// Returns [`ProtocolCodecError`] when the response cannot be encoded or exceeds the frame bound.
pub fn encode_response_v3_with_limit(
    response: &BrokerResponse,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, ProtocolCodecError> {
    if let BrokerResponse::Success {
        request_id,
        result: SuccessPayload::SessionOwnerAcquired { owner_epoch },
    } = response
    {
        return serialize_bounded(
            &object([
                ("ok", Value::Bool(true)),
                ("request_id", Value::String(request_id.as_str().to_owned())),
                (
                    "result",
                    object([("owner_epoch", Value::from(owner_epoch.get()))]),
                ),
                ("version", Value::from(PROTOCOL_VERSION_V3)),
            ]),
            max_frame_bytes,
        );
    }
    if let Some(frame) = encode_owner_status_success(response, max_frame_bytes) {
        return frame;
    }
    if let BrokerResponse::Success {
        request_id,
        result: SuccessPayload::AuthorityRecorded {
            recorded_revision, ..
        },
    } = response
    {
        return serialize_bounded(
            &object([
                ("ok", Value::Bool(true)),
                ("request_id", Value::String(request_id.as_str().to_owned())),
                (
                    "result",
                    object([("recorded_revision", Value::from(recorded_revision.get()))]),
                ),
                ("version", Value::from(PROTOCOL_VERSION_V3)),
            ]),
            max_frame_bytes,
        );
    }
    if let BrokerResponse::Success {
        request_id,
        result: SuccessPayload::AuthorityReleased { floor_revision, .. },
    } = response
    {
        return serialize_bounded(
            &object([
                ("ok", Value::Bool(true)),
                ("request_id", Value::String(request_id.as_str().to_owned())),
                (
                    "result",
                    object([("floor_revision", Value::from(floor_revision.get()))]),
                ),
                ("version", Value::from(PROTOCOL_VERSION_V3)),
            ]),
            max_frame_bytes,
        );
    }
    let v1_frame = encode_response_with_limit(response, max_frame_bytes)?;
    let mut root: Value = serde_json::from_slice(&v1_frame)
        .map_err(|error| ProtocolCodecError::Serialization(error.to_string()))?;
    let object = root.as_object_mut().ok_or_else(|| {
        invalid_request("internal protocol-v1 response encoding was not an object")
    })?;
    if let BrokerResponse::Error { error, .. } = response {
        let error_object = object
            .get_mut("error")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| invalid_request("internal protocol-v1 error encoding omitted error"))?;
        error_object.insert(
            "disposition".to_owned(),
            Value::String(error.disposition.as_str().to_owned()),
        );
    }
    object.insert("version".to_owned(), Value::from(PROTOCOL_VERSION_V3));
    serialize_bounded(&root, max_frame_bytes)
}

/// Decode one correlated protocol-v3 owner-aware mutation response.
///
/// # Errors
///
/// Returns [`ResponseDecodeError::Protocol`] for malformed/mismatched frames and
/// [`ResponseDecodeError::Broker`] for stable Broker application errors.
pub fn decode_owner_mutation_response_with_limit(
    frame: &[u8],
    expected_request_id: &RequestId,
    operation: Operation,
    max_frame_bytes: usize,
) -> Result<SuccessPayload, ResponseDecodeError> {
    let disposition = response_error_disposition(frame, max_frame_bytes)?;
    let root = v3_response_to_v1(frame, max_frame_bytes)?;
    let v1_frame = serialize_bounded(&root, max_frame_bytes)?;
    match decode_response_for_operation_with_limit(
        &v1_frame,
        expected_request_id,
        operation,
        max_frame_bytes,
    ) {
        Err(ResponseDecodeError::Broker(error)) => Err(ResponseDecodeError::Broker(
            error.with_disposition(disposition.ok_or_else(|| {
                invalid_request("protocol-v3 error response omitted disposition")
            })?),
        )),
        other => other,
    }
}

/// Decode one correlated protocol-v3 owner acquisition response.
///
/// # Errors
///
/// Returns [`ResponseDecodeError::Protocol`] for malformed/mismatched frames and
/// [`ResponseDecodeError::Broker`] for stable Broker acquisition errors.
pub fn decode_owner_acquisition_response_with_limit(
    frame: &[u8],
    expected_request_id: &RequestId,
    max_frame_bytes: usize,
) -> Result<SessionOwnerEpoch, ResponseDecodeError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: ResponseEnvelopeV3 = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Response frame must be valid protocol-v3 JSON: {error}"
        ))
    })?;
    if envelope.version != PROTOCOL_VERSION_V3 {
        return Err(invalid_request(format!(
            "Unsupported response protocol-v3 version {}.",
            envelope.version
        ))
        .into());
    }
    let request_id = RequestId::new(envelope.request_id.clone())
        .map_err(|error| invalid_request(error.to_string()))?;
    if &request_id != expected_request_id {
        return Err(invalid_request("Response request_id does not match request.").into());
    }
    match (envelope.ok, envelope.result, envelope.error) {
        (true, Some(result), None) => {
            let object = result.as_object().ok_or_else(|| {
                invalid_request("protocol-v3 owner acquisition result must be an object")
            })?;
            if object.len() != 1 {
                return Err(invalid_request(
                    "protocol-v3 owner acquisition result contains unknown fields",
                )
                .into());
            }
            let owner_epoch = object
                .get("owner_epoch")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    invalid_request("protocol-v3 owner acquisition result requires owner_epoch")
                })?;
            SessionOwnerEpoch::new(owner_epoch)
                .map_err(|error| invalid_request(error.to_string()).into())
        }
        (false, None, Some(error)) => {
            let disposition = parse_error_disposition(&error.disposition)?;
            let root = object([
                (
                    "error",
                    object([
                        ("code", Value::String(error.code)),
                        ("message", Value::String(error.message)),
                    ]),
                ),
                ("ok", Value::Bool(false)),
                ("request_id", Value::String(request_id.as_str().to_owned())),
                ("version", Value::from(PROTOCOL_VERSION)),
            ]);
            let v1_frame = serialize_bounded(&root, max_frame_bytes)?;
            match decode_response_for_operation_with_limit(
                &v1_frame,
                expected_request_id,
                Operation::EnsureNamespace,
                max_frame_bytes,
            ) {
                Err(ResponseDecodeError::Broker(error)) => Err(ResponseDecodeError::Broker(
                    error.with_disposition(disposition),
                )),
                Err(error) => Err(error),
                Ok(_) => Err(invalid_request(
                    "protocol-v3 owner acquisition error decoded as success",
                )
                .into()),
            }
        }
        (true, _, _) => Err(invalid_request(
            "protocol-v3 success response must contain result and no error",
        )
        .into()),
        (false, _, _) => Err(invalid_request(
            "protocol-v3 error response must contain error and no result",
        )
        .into()),
    }
}

/// Decode one correlated protocol-v3 `owner.status` response.
///
/// # Errors
///
/// Returns [`ResponseDecodeError::Protocol`] for malformed/mismatched frames and
/// [`ResponseDecodeError::Broker`] for stable Broker read errors.
pub fn decode_owner_status_response_with_limit(
    frame: &[u8],
    expected_request_id: &RequestId,
    max_frame_bytes: usize,
) -> Result<CommandSessionOwnerStatus, ResponseDecodeError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: ResponseEnvelopeV3 = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Response frame must be valid protocol-v3 JSON: {error}"
        ))
    })?;
    if envelope.version != PROTOCOL_VERSION_V3 {
        return Err(invalid_request(format!(
            "Unsupported response protocol-v3 version {}.",
            envelope.version
        ))
        .into());
    }
    let request_id = RequestId::new(envelope.request_id.clone())
        .map_err(|error| invalid_request(error.to_string()))?;
    if &request_id != expected_request_id {
        return Err(invalid_request("Response request_id does not match request.").into());
    }
    match (envelope.ok, envelope.result, envelope.error) {
        (true, Some(result), None) => decode_owner_status_result(&result),
        (false, None, Some(error)) => Err(decode_owner_status_error(
            error,
            &request_id,
            expected_request_id,
            max_frame_bytes,
        )),
        (true, _, _) => Err(invalid_request(
            "protocol-v3 success response must contain result and no error",
        )
        .into()),
        (false, _, _) => Err(invalid_request(
            "protocol-v3 error response must contain error and no result",
        )
        .into()),
    }
}

fn decode_owner_status_error(
    error: ResponseErrorV3,
    request_id: &RequestId,
    expected_request_id: &RequestId,
    max_frame_bytes: usize,
) -> ResponseDecodeError {
    let disposition = match parse_error_disposition(&error.disposition) {
        Ok(disposition) => disposition,
        Err(error) => return error.into(),
    };
    let root = object([
        (
            "error",
            object([
                ("code", Value::String(error.code)),
                ("message", Value::String(error.message)),
            ]),
        ),
        ("ok", Value::Bool(false)),
        ("request_id", Value::String(request_id.as_str().to_owned())),
        ("version", Value::from(PROTOCOL_VERSION)),
    ]);
    let v1_frame = match serialize_bounded(&root, max_frame_bytes) {
        Ok(frame) => frame,
        Err(error) => return error.into(),
    };
    match decode_response_for_operation_with_limit(
        &v1_frame,
        expected_request_id,
        Operation::EnsureNamespace,
        max_frame_bytes,
    ) {
        Err(ResponseDecodeError::Broker(error)) => {
            ResponseDecodeError::Broker(error.with_disposition(disposition))
        }
        Err(error) => error,
        Ok(_) => invalid_request("protocol-v3 owner.status error decoded as success").into(),
    }
}

fn decode_owner_status_result(
    result: &Value,
) -> Result<CommandSessionOwnerStatus, ResponseDecodeError> {
    let object = result
        .as_object()
        .ok_or_else(|| invalid_request("protocol-v3 owner.status result must be an object"))?;
    if object.len() != 4 {
        return Err(invalid_request(
            "protocol-v3 owner.status result must contain exactly four fields",
        )
        .into());
    }
    let field = |name: &str| {
        object.get(name).ok_or_else(|| {
            invalid_request(format!("protocol-v3 owner.status result requires {name}"))
        })
    };
    let session_found = field("session_found")?
        .as_bool()
        .ok_or_else(|| invalid_request("protocol-v3 owner.status session_found must be bool"))?;
    let applied_index = field("applied_index")?
        .as_u64()
        .ok_or_else(|| invalid_request("protocol-v3 owner.status applied_index must be u64"))?;
    let epoch = field("owner_epoch")?;
    let instance = field("owner_instance_id")?;
    if !session_found {
        if !epoch.is_null() || !instance.is_null() {
            return Err(invalid_request(
                "protocol-v3 owner.status absent session must not carry owner fields",
            )
            .into());
        }
        return Ok(CommandSessionOwnerStatus {
            owner: None,
            applied_index,
        });
    }
    let owner_epoch = SessionOwnerEpoch::new(
        epoch
            .as_u64()
            .ok_or_else(|| invalid_request("protocol-v3 owner.status owner_epoch must be u64"))?,
    )
    .map_err(|error| invalid_request(error.to_string()))?;
    let owner_instance_id = match instance {
        Value::Null => None,
        Value::String(value) => Some(
            SessionOwnerInstanceId::new(value.clone())
                .map_err(|error| invalid_request(error.to_string()))?,
        ),
        _ => {
            return Err(invalid_request(
                "protocol-v3 owner.status owner_instance_id must be string or null",
            )
            .into());
        }
    };
    Ok(CommandSessionOwnerStatus {
        owner: Some(CommandSessionOwnerRecord {
            owner_epoch,
            owner_instance_id,
        }),
        applied_index,
    })
}

/// Decode one correlated protocol-v3 `authority.record` response.
///
/// # Errors
///
/// Returns [`ResponseDecodeError::Protocol`] for malformed/mismatched frames and
/// [`ResponseDecodeError::Broker`] for stable Broker authority errors.
pub fn decode_authority_record_response_with_limit(
    frame: &[u8],
    expected_request_id: &RequestId,
    max_frame_bytes: usize,
) -> Result<AuthorityRevision, ResponseDecodeError> {
    decode_authority_response_revision(
        frame,
        expected_request_id,
        "recorded_revision",
        max_frame_bytes,
    )
}

/// Decode one correlated protocol-v3 `authority.release` response.
///
/// # Errors
///
/// Returns [`ResponseDecodeError::Protocol`] for malformed/mismatched frames and
/// [`ResponseDecodeError::Broker`] for stable Broker authority errors.
pub fn decode_authority_release_response_with_limit(
    frame: &[u8],
    expected_request_id: &RequestId,
    max_frame_bytes: usize,
) -> Result<AuthorityRevision, ResponseDecodeError> {
    decode_authority_response_revision(
        frame,
        expected_request_id,
        "floor_revision",
        max_frame_bytes,
    )
}

fn decode_authority_response_revision(
    frame: &[u8],
    expected_request_id: &RequestId,
    result_field: &'static str,
    max_frame_bytes: usize,
) -> Result<AuthorityRevision, ResponseDecodeError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: ResponseEnvelopeV3 = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Response frame must be valid protocol-v3 JSON: {error}"
        ))
    })?;
    if envelope.version != PROTOCOL_VERSION_V3 {
        return Err(invalid_request(format!(
            "Unsupported response protocol-v3 version {}.",
            envelope.version
        ))
        .into());
    }
    let request_id = RequestId::new(envelope.request_id.clone())
        .map_err(|error| invalid_request(error.to_string()))?;
    if &request_id != expected_request_id {
        return Err(invalid_request("Response request_id does not match request.").into());
    }
    match (envelope.ok, envelope.result, envelope.error) {
        (true, Some(result), None) => {
            let object = result.as_object().ok_or_else(|| {
                invalid_request("protocol-v3 authority response result must be an object")
            })?;
            if object.len() != 1 {
                return Err(invalid_request(
                    "protocol-v3 authority response result contains unknown fields",
                )
                .into());
            }
            let revision = object
                .get(result_field)
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    invalid_request(format!(
                        "protocol-v3 authority response result requires {result_field}"
                    ))
                })?;
            AuthorityRevision::new(revision)
                .map_err(|error| invalid_request(error.to_string()).into())
        }
        (false, None, Some(error)) => {
            let disposition = parse_error_disposition(&error.disposition)?;
            let root = object([
                (
                    "error",
                    object([
                        ("code", Value::String(error.code)),
                        ("message", Value::String(error.message)),
                    ]),
                ),
                ("ok", Value::Bool(false)),
                ("request_id", Value::String(request_id.as_str().to_owned())),
                ("version", Value::from(PROTOCOL_VERSION)),
            ]);
            let v1_frame = serialize_bounded(&root, max_frame_bytes)?;
            match decode_response_for_operation_with_limit(
                &v1_frame,
                expected_request_id,
                Operation::EnsureNamespace,
                max_frame_bytes,
            ) {
                Err(ResponseDecodeError::Broker(error)) => Err(ResponseDecodeError::Broker(
                    error.with_disposition(disposition),
                )),
                Err(error) => Err(error),
                Ok(_) => {
                    Err(invalid_request("protocol-v3 authority error decoded as success").into())
                }
            }
        }
        (true, _, _) => Err(invalid_request(
            "protocol-v3 success response must contain result and no error",
        )
        .into()),
        (false, _, _) => Err(invalid_request(
            "protocol-v3 error response must contain error and no result",
        )
        .into()),
    }
}

fn response_error_disposition(
    frame: &[u8],
    max_frame_bytes: usize,
) -> Result<Option<BrokerErrorDisposition>, ProtocolCodecError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: ResponseEnvelopeV3 = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Response frame must be valid protocol-v3 JSON: {error}"
        ))
    })?;
    envelope
        .error
        .as_ref()
        .map(|error| parse_error_disposition(&error.disposition))
        .transpose()
}

fn parse_error_disposition(value: &str) -> Result<BrokerErrorDisposition, ProtocolCodecError> {
    match value {
        "COMMITTED" => Ok(BrokerErrorDisposition::Committed),
        "REJECTED" => Ok(BrokerErrorDisposition::Rejected),
        "UNKNOWN" => Ok(BrokerErrorDisposition::Unknown),
        other => Err(invalid_request(format!(
            "Unsupported protocol-v3 error disposition {other}."
        ))),
    }
}

fn v3_response_to_v1(frame: &[u8], max_frame_bytes: usize) -> Result<Value, ProtocolCodecError> {
    ensure_frame_limit(frame.len(), max_frame_bytes)?;
    let envelope: ResponseEnvelopeV3 = serde_json::from_slice(frame).map_err(|error| {
        invalid_request(format!(
            "Response frame must be valid protocol-v3 JSON: {error}"
        ))
    })?;
    if envelope.version != PROTOCOL_VERSION_V3 {
        return Err(invalid_request(format!(
            "Unsupported response protocol-v3 version {}.",
            envelope.version
        )));
    }
    match (envelope.ok, envelope.result, envelope.error) {
        (true, Some(result), None) => Ok(object([
            ("ok", Value::Bool(true)),
            ("request_id", Value::String(envelope.request_id)),
            ("result", result),
            ("version", Value::from(PROTOCOL_VERSION)),
        ])),
        (false, None, Some(error)) => Ok(object([
            (
                "error",
                object([
                    ("code", Value::String(error.code)),
                    ("message", Value::String(error.message)),
                ]),
            ),
            ("ok", Value::Bool(false)),
            ("request_id", Value::String(envelope.request_id)),
            ("version", Value::from(PROTOCOL_VERSION)),
        ])),
        (true, _, _) => Err(invalid_request(
            "protocol-v3 success response must contain result and no error",
        )),
        (false, _, _) => Err(invalid_request(
            "protocol-v3 error response must contain error and no result",
        )),
    }
}

fn ensure_frame_limit(actual: usize, max: usize) -> Result<(), ProtocolCodecError> {
    if actual > max {
        return Err(ProtocolCodecError::FrameTooLarge { actual, max });
    }
    Ok(())
}

fn serialize_bounded(value: &Value, max: usize) -> Result<Vec<u8>, ProtocolCodecError> {
    let mut encoded = serde_json::to_vec(value)
        .map_err(|error| ProtocolCodecError::Serialization(error.to_string()))?;
    encoded.push(b'\n');
    ensure_frame_limit(encoded.len(), max)?;
    Ok(encoded)
}

fn object<const N: usize>(entries: [(&'static str, Value); N]) -> Value {
    let ordered: BTreeMap<String, Value> = entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect();
    Value::Object(ordered.into_iter().collect())
}

fn invalid_request(message: impl Into<String>) -> ProtocolCodecError {
    ProtocolCodecError::InvalidRequest(message.into())
}

#[cfg(test)]
mod tests {
    use agent_broker_application::{
        BrokerError, BrokerErrorCode, BrokerErrorDisposition, CommandIdentity, CommandSequence,
        CommandSessionId, SessionOwnerEpoch, SessionOwnerInstanceId,
    };
    use agent_broker_domain::NamespaceId;

    use super::*;
    use crate::{BrokerRequest, EnsureNamespaceRequest};

    #[test]
    fn owner_acquisition_round_trips_strict_v3() -> Result<(), Box<dyn std::error::Error>> {
        let request = OwnerAcquisitionRequestV3::new(
            RequestId::new("v3-acquire-1")?,
            CommandSessionId::new("session-a")?,
            SessionOwnerEpoch::INITIAL,
            SessionOwnerInstanceId::new("process-a")?,
        );
        let frame = encode_owner_acquisition_request_with_limit(&request, 16 * 1024)?;
        let decoded = decode_request_v3_with_limit(&frame, 16 * 1024)?;
        assert_eq!(decoded, BrokerRequestV3::AcquireOwner(request));
        Ok(())
    }

    #[test]
    fn owner_status_request_round_trips_and_rejects_authority_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = OwnerStatusRequestV3::new(
            RequestId::new("v3-status-1")?,
            CommandSessionId::new("session-a")?,
            SessionOwnerInstanceId::new("process-a")?,
        );
        let frame = encode_owner_status_request_with_limit(&request, 16 * 1024)?;
        assert_eq!(
            decode_request_v3_with_limit(&frame, 16 * 1024)?,
            BrokerRequestV3::OwnerStatus(request)
        );
        for extra in [
            ("expected_owner_epoch", Value::from(1)),
            ("owner_epoch", Value::from(1)),
            ("command_sequence", Value::from(1)),
        ] {
            let mut root: Value = serde_json::from_slice(&frame)?;
            root.as_object_mut()
                .ok_or("frame must be an object")?
                .insert(extra.0.to_owned(), extra.1);
            assert!(
                decode_request_v3_with_limit(&serde_json::to_vec(&root)?, 16 * 1024).is_err(),
                "owner.status must reject {}",
                extra.0
            );
        }
        let mut no_session: Value = serde_json::from_slice(&frame)?;
        no_session
            .as_object_mut()
            .ok_or("frame must be an object")?
            .remove("command_session_id");
        assert!(
            decode_request_v3_with_limit(&serde_json::to_vec(&no_session)?, 16 * 1024).is_err()
        );
        let mut payload: Value = serde_json::from_slice(&frame)?;
        payload["payload"] = serde_json::json!({"x": 1});
        assert!(decode_request_v3_with_limit(&serde_json::to_vec(&payload)?, 16 * 1024).is_err());
        Ok(())
    }

    #[test]
    fn owner_status_response_round_trips_found_absent_and_rejects_malformed()
    -> Result<(), Box<dyn std::error::Error>> {
        let request_id = RequestId::new("v3-status-2")?;
        for (epoch, instance, found) in [
            (
                Some(SessionOwnerEpoch::new(2)?),
                Some(SessionOwnerInstanceId::new("process-b")?),
                true,
            ),
            (Some(SessionOwnerEpoch::new(1)?), None, true),
            (None, None, false),
        ] {
            let response = BrokerResponse::Success {
                request_id: request_id.clone(),
                result: SuccessPayload::SessionOwnerStatus {
                    owner_epoch: epoch,
                    owner_instance_id: instance.clone(),
                    applied_index: 41,
                },
            };
            let frame = encode_response_v3_with_limit(&response, 16 * 1024)?;
            let decoded = decode_owner_status_response_with_limit(&frame, &request_id, 16 * 1024)?;
            assert_eq!(decoded.applied_index, 41);
            assert_eq!(decoded.owner.is_some(), found);
            assert_eq!(decoded.owner.as_ref().map(|owner| owner.owner_epoch), epoch);
            assert_eq!(
                decoded.owner.and_then(|owner| owner.owner_instance_id),
                instance
            );
        }
        let wrong_id = RequestId::new("other")?;
        let frame = encode_response_v3_with_limit(
            &BrokerResponse::Success {
                request_id: request_id.clone(),
                result: SuccessPayload::SessionOwnerStatus {
                    owner_epoch: None,
                    owner_instance_id: None,
                    applied_index: 1,
                },
            },
            16 * 1024,
        )?;
        assert!(decode_owner_status_response_with_limit(&frame, &wrong_id, 16 * 1024).is_err());
        let mut absent_with_owner: Value = serde_json::from_slice(&frame)?;
        absent_with_owner["result"]["owner_epoch"] = Value::from(3);
        assert!(
            decode_owner_status_response_with_limit(
                &serde_json::to_vec(&absent_with_owner)?,
                &request_id,
                16 * 1024
            )
            .is_err()
        );
        let mut extra_field: Value = serde_json::from_slice(&frame)?;
        extra_field["result"]["extra"] = Value::from(1);
        assert!(
            decode_owner_status_response_with_limit(
                &serde_json::to_vec(&extra_field)?,
                &request_id,
                16 * 1024
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn owner_mutation_round_trips_strict_v3() -> Result<(), Box<dyn std::error::Error>> {
        let identity = CommandIdentity::new_with_owner(
            CommandSessionId::new("session-a")?,
            SessionOwnerEpoch::new(2)?,
            SessionOwnerInstanceId::new("process-a")?,
            CommandSequence::new(1)?,
        );
        let request = OwnerIdentifiedBrokerRequestV3::new(
            identity,
            BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
                request_id: RequestId::new("v3-mutation-1")?,
                namespace_id: NamespaceId::new("v3-owned")?,
            }),
        )?;
        let frame = encode_owner_mutation_request_with_limit(&request, 16 * 1024)?;
        let decoded = decode_request_v3_with_limit(&frame, 16 * 1024)?;
        assert_eq!(decoded, BrokerRequestV3::Mutation(request));
        Ok(())
    }

    #[test]
    fn acquisition_rejects_mutation_only_fields() {
        let frame = br#"{"version":3,"request_id":"v3-bad","operation":"acquire_command_session_owner","command_session_id":"session-a","expected_owner_epoch":1,"owner_epoch":2,"owner_instance_id":"process-a","payload":{}}
"#;
        assert!(decode_request_v3_with_limit(frame, 16 * 1024).is_err());
    }

    #[test]
    fn v3_error_disposition_round_trips_without_widening_v1_v2()
    -> Result<(), Box<dyn std::error::Error>> {
        let request_id = RequestId::new("v3-disposition")?;
        for disposition in [
            BrokerErrorDisposition::Committed,
            BrokerErrorDisposition::Rejected,
            BrokerErrorDisposition::Unknown,
        ] {
            let response = BrokerResponse::error(
                request_id.clone(),
                BrokerError::new(BrokerErrorCode::Conflict, "disposition-test")
                    .with_disposition(disposition),
            );
            let frame = encode_response_v3_with_limit(&response, 16 * 1024)?;
            let value: serde_json::Value = serde_json::from_slice(&frame)?;
            assert_eq!(
                value["error"]["disposition"].as_str(),
                Some(disposition.as_str())
            );
            let decoded = decode_owner_mutation_response_with_limit(
                &frame,
                &request_id,
                Operation::EnsureNamespace,
                16 * 1024,
            );
            let error = match decoded {
                Err(ResponseDecodeError::Broker(error)) => error,
                other => return Err(format!("expected v3 Broker error, got {other:?}").into()),
            };
            assert_eq!(error.code(), BrokerErrorCode::Conflict);
            assert_eq!(error.disposition(), disposition);
        }
        Ok(())
    }

    #[test]
    fn v3_error_disposition_is_required_and_strict() -> Result<(), Box<dyn std::error::Error>> {
        let request_id = RequestId::new("v3-disposition-bad")?;
        let missing = br#"{"version":3,"request_id":"v3-disposition-bad","ok":false,"error":{"code":"CONFLICT","message":"missing"}}
"#;
        assert!(matches!(
            decode_owner_mutation_response_with_limit(
                missing,
                &request_id,
                Operation::EnsureNamespace,
                16 * 1024
            ),
            Err(ResponseDecodeError::Protocol(_))
        ));

        let unknown = br#"{"version":3,"request_id":"v3-disposition-bad","ok":false,"error":{"code":"CONFLICT","message":"unknown","disposition":"MAYBE"}}
"#;
        assert!(matches!(
            decode_owner_mutation_response_with_limit(
                unknown,
                &request_id,
                Operation::EnsureNamespace,
                16 * 1024
            ),
            Err(ResponseDecodeError::Protocol(_))
        ));
        Ok(())
    }

    #[test]
    fn v3_error_encoding_preserves_v1_message_bound() -> Result<(), Box<dyn std::error::Error>> {
        let response = BrokerResponse::error(
            RequestId::new("v3-error-bound")?,
            BrokerError::new(
                BrokerErrorCode::InternalError,
                "x".repeat(crate::MAX_ERROR_MESSAGE_BYTES + 1),
            )
            .with_disposition(BrokerErrorDisposition::Committed),
        );
        assert!(encode_response_v3_with_limit(&response, 256 * 1024).is_err());
        Ok(())
    }

    #[test]
    fn frozen_v1_v2_do_not_accept_v3_owner_surface() {
        let v1_acquisition = br#"{"version":1,"request_id":"v1-reject-v3","operation":"acquire_command_session_owner","payload":{}}
"#;
        assert!(crate::decode_wire_request_with_limit(v1_acquisition, 16 * 1024).is_err());

        let v2_owner_field = br#"{"version":2,"request_id":"v2-reject-v3","operation":"ensure_namespace","command_session_id":"session-a","command_sequence":1,"owner_instance_id":"process-a","payload":{"namespace_id":"v2-must-reject-owner-field"}}
"#;
        assert!(crate::decode_wire_request_with_limit(v2_owner_field, 16 * 1024).is_err());
    }

    fn authority_record_request() -> Result<AuthorityRecordRequestV3, Box<dyn std::error::Error>> {
        Ok(AuthorityRecordRequestV3::new(
            RequestId::new("v3-authority-record-1")?,
            agent_broker_domain::AuthorityFenceKey::new("leader:shard-1")?,
            agent_broker_domain::AuthorityRevision::new(5)?,
            agent_broker_domain::AuthorityId::new("authority-a")?,
            agent_broker_domain::AuthorityOwnerId::new("process-a")?,
        ))
    }

    #[test]
    fn authority_record_round_trips_strict_v3() -> Result<(), Box<dyn std::error::Error>> {
        let request = authority_record_request()?;
        let frame = encode_authority_record_request_with_limit(&request, 16 * 1024)?;
        let decoded = decode_request_v3_with_limit(&frame, 16 * 1024)?;
        assert_eq!(decoded, BrokerRequestV3::AuthorityRecord(request));
        Ok(())
    }

    #[test]
    fn authority_release_round_trips_strict_v3() -> Result<(), Box<dyn std::error::Error>> {
        let request = AuthorityReleaseRequestV3::new(
            RequestId::new("v3-authority-release-1")?,
            agent_broker_domain::AuthorityFenceKey::new("leader:shard-1")?,
            agent_broker_domain::AuthorityRevision::new(5)?,
            agent_broker_domain::AuthorityOwnerId::new("process-a")?,
        );
        let frame = encode_authority_release_request_with_limit(&request, 16 * 1024)?;
        let decoded = decode_request_v3_with_limit(&frame, 16 * 1024)?;
        assert_eq!(decoded, BrokerRequestV3::AuthorityRelease(request));
        Ok(())
    }

    #[test]
    fn authority_record_rejects_foreign_and_missing_fields() {
        let foreign_session = br#"{"version":3,"request_id":"v3-auth-bad","operation":"authority.record","command_session_id":"session-a","key":"leader:shard-1","revision":5,"authority_id":"authority-a","owner_instance_id":"process-a","payload":{}}
"#;
        assert!(decode_request_v3_with_limit(foreign_session, 16 * 1024).is_err());

        let foreign_expected_revision = br#"{"version":3,"request_id":"v3-auth-bad","operation":"authority.record","key":"leader:shard-1","revision":5,"expected_revision":6,"authority_id":"authority-a","owner_instance_id":"process-a","payload":{}}
"#;
        assert!(decode_request_v3_with_limit(foreign_expected_revision, 16 * 1024).is_err());

        let zero_revision = br#"{"version":3,"request_id":"v3-auth-bad","operation":"authority.record","key":"leader:shard-1","revision":0,"authority_id":"authority-a","owner_instance_id":"process-a","payload":{}}
"#;
        assert!(decode_request_v3_with_limit(zero_revision, 16 * 1024).is_err());

        let missing_authority = br#"{"version":3,"request_id":"v3-auth-bad","operation":"authority.record","key":"leader:shard-1","revision":5,"owner_instance_id":"process-a","payload":{}}
"#;
        assert!(decode_request_v3_with_limit(missing_authority, 16 * 1024).is_err());

        let release_with_revision = br#"{"version":3,"request_id":"v3-auth-bad","operation":"authority.release","key":"leader:shard-1","revision":5,"expected_revision":5,"owner_instance_id":"process-a","payload":{}}
"#;
        assert!(decode_request_v3_with_limit(release_with_revision, 16 * 1024).is_err());

        let acquire_with_key = br#"{"version":3,"request_id":"v3-auth-bad","operation":"acquire_command_session_owner","command_session_id":"session-a","expected_owner_epoch":1,"key":"leader:shard-1","owner_instance_id":"process-a","payload":{}}
"#;
        assert!(decode_request_v3_with_limit(acquire_with_key, 16 * 1024).is_err());
    }

    #[test]
    fn authority_responses_round_trip_strict_v3() -> Result<(), Box<dyn std::error::Error>> {
        use agent_broker_domain::results::{
            AuthorityRecordedResult, AuthorityReleasedResult, MutationMetadata,
        };
        use agent_broker_domain::{AuthorityFenceKey, AuthorityRevision, Revision, Term};

        let request_id = RequestId::new("v3-authority-response")?;
        let metadata = MutationMetadata {
            term: Term::INITIAL,
            revision: Revision::new(1),
        };
        let recorded = BrokerResponse::success(
            request_id.clone(),
            crate::DispatchResult::AuthorityRecorded(AuthorityRecordedResult {
                metadata,
                key: AuthorityFenceKey::new("leader:shard-1")?,
                recorded_revision: AuthorityRevision::new(5)?,
            }),
        );
        let frame = encode_response_v3_with_limit(&recorded, 16 * 1024)?;
        let decoded_revision =
            decode_authority_record_response_with_limit(&frame, &request_id, 16 * 1024)?;
        assert_eq!(decoded_revision, AuthorityRevision::new(5)?);
        let raw: serde_json::Value = serde_json::from_slice(&frame)?;
        assert_eq!(raw["result"]["recorded_revision"].as_u64(), Some(5));
        assert_eq!(raw["version"].as_u64(), Some(3));

        let released = BrokerResponse::success(
            request_id.clone(),
            crate::DispatchResult::AuthorityReleased(AuthorityReleasedResult {
                metadata,
                key: AuthorityFenceKey::new("leader:shard-1")?,
                floor_revision: AuthorityRevision::new(5)?,
            }),
        );
        let release_frame = encode_response_v3_with_limit(&released, 16 * 1024)?;
        let decoded_floor =
            decode_authority_release_response_with_limit(&release_frame, &request_id, 16 * 1024)?;
        assert_eq!(decoded_floor, AuthorityRevision::new(5)?);
        Ok(())
    }

    #[test]
    fn authority_error_responses_decode_with_stable_codes_and_dispositions()
    -> Result<(), Box<dyn std::error::Error>> {
        let request_id = RequestId::new("v3-authority-error")?;
        let response = BrokerResponse::error(
            request_id.clone(),
            agent_broker_application::BrokerError::new(
                agent_broker_application::BrokerErrorCode::StaleFence,
                "authority fence is superseded",
            )
            .with_disposition(agent_broker_application::BrokerErrorDisposition::Rejected),
        );
        let frame = encode_response_v3_with_limit(&response, 16 * 1024)?;
        let error =
            match decode_authority_record_response_with_limit(&frame, &request_id, 16 * 1024) {
                Err(ResponseDecodeError::Broker(error)) => error,
                other => {
                    return Err(format!("expected Broker authority error, got {other:?}").into());
                }
            };
        assert_eq!(
            error.code(),
            agent_broker_application::BrokerErrorCode::StaleFence
        );
        assert_eq!(
            error.disposition(),
            agent_broker_application::BrokerErrorDisposition::Rejected
        );

        let mismatched = decode_authority_record_response_with_limit(
            &frame,
            &RequestId::new("v3-other-request")?,
            16 * 1024,
        );
        assert!(matches!(mismatched, Err(ResponseDecodeError::Protocol(_))));
        Ok(())
    }

    #[test]
    fn authority_success_payloads_are_rejected_by_the_v1_encoder()
    -> Result<(), Box<dyn std::error::Error>> {
        use agent_broker_domain::results::{AuthorityRecordedResult, MutationMetadata};
        use agent_broker_domain::{AuthorityFenceKey, AuthorityRevision, Revision, Term};

        let recorded = BrokerResponse::success(
            RequestId::new("v1-reject-authority")?,
            crate::DispatchResult::AuthorityRecorded(AuthorityRecordedResult {
                metadata: MutationMetadata {
                    term: Term::INITIAL,
                    revision: Revision::new(1),
                },
                key: AuthorityFenceKey::new("leader:shard-1")?,
                recorded_revision: AuthorityRevision::new(5)?,
            }),
        );
        assert!(crate::encode_response_with_limit(&recorded, 16 * 1024).is_err());
        Ok(())
    }
}
