use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use agent_broker_application::{BrokerApplicationService, BrokerErrorCode};
use agent_broker_client::{BrokerClient, BrokerClientConfig, ClientError};
use agent_broker_consensus::StandaloneConsensusAdapter;
use agent_broker_domain::{
    AuthorityFenceKey, AuthorityId, AuthorityOwnerId, AuthorityRevision, BrokerCapacityPolicy,
};
use agent_broker_protocol::BrokerRequestDispatcher;
use agent_broker_runtime::{BrokerServerConfig, RuntimeError, StateOwnerHandle, TcpBrokerServer};
use agent_broker_storage::{JournalCompactionPolicy, JournaledBrokerStateRepository};
use tempfile::tempdir;

struct TestServer {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<Result<(), RuntimeError>>,
}

impl TestServer {
    fn stop(self) -> Result<(), Box<dyn Error>> {
        self.stop.store(true, Ordering::Release);
        match self.handle.join() {
            Ok(result) => result.map_err(Into::into),
            Err(_) => Err("Broker server thread panicked".into()),
        }
    }
}

fn spawn_test_server(state_path: std::path::PathBuf) -> Result<TestServer, Box<dyn Error>> {
    let repository = JournaledBrokerStateRepository::new(
        state_path,
        None,
        JournalCompactionPolicy::new(10_000, 64 * 1024 * 1024)?,
    );
    let consensus = StandaloneConsensusAdapter::new(repository)?;
    let service = BrokerApplicationService::new(consensus, BrokerCapacityPolicy::default());
    let dispatcher = BrokerRequestDispatcher::new(service);
    let state_owner = StateOwnerHandle::spawn(dispatcher, 16)?;
    let server = TcpBrokerServer::bind(
        BrokerServerConfig {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            max_frame_bytes: 128 * 1024,
            max_connections: 16,
            connection_io_timeout: Duration::from_secs(10),
        },
        state_owner,
    )?;
    let address = server.local_addr()?;
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let handle = thread::Builder::new()
        .name("agent-broker-authority-e2e-server".to_owned())
        .spawn(move || server.serve_until(thread_stop.as_ref()))?;
    Ok(TestServer {
        address,
        stop,
        handle,
    })
}

fn broker_code(error: ClientError) -> Result<BrokerErrorCode, Box<dyn Error>> {
    match error {
        ClientError::Broker(broker_error) => Ok(broker_error.code()),
        other => Err(format!("expected a Broker error, got {other:?}").into()),
    }
}

fn client(address: SocketAddr) -> Result<BrokerClient, Box<dyn Error>> {
    Ok(BrokerClient::new(BrokerClientConfig {
        address,
        timeout: Duration::from_secs(5),
        max_response_frame_bytes: 128 * 1024,
    })?)
}

#[test]
fn authority_record_replay_release_and_aba_protection_over_real_tcp() -> Result<(), Box<dyn Error>>
{
    let directory = tempdir()?;
    let server = spawn_test_server(directory.path().join("authority-e2e-state.json"))?;
    let mut client = client(server.address)?;

    let key = AuthorityFenceKey::new("leadership:revision-e2e")?;
    let authority_id = AuthorityId::new("runtime-a")?;
    let owner = AuthorityOwnerId::new("process-a")?;
    let revision = AuthorityRevision::new(41)?;

    // Record with an arbitrary (gapped) exact revision value.
    let recorded =
        client.authority_record(key.clone(), revision, authority_id.clone(), owner.clone())?;
    assert_eq!(recorded, revision);

    // Exact replay is idempotent and returns the same recorded revision.
    let replayed =
        client.authority_record(key.clone(), revision, authority_id.clone(), owner.clone())?;
    assert_eq!(replayed, revision);

    // Same authority at a different revision violates the replay binding.
    let conflict = client.authority_record(
        key.clone(),
        AuthorityRevision::new(42)?,
        authority_id.clone(),
        owner.clone(),
    );
    let Err(error) = conflict else {
        return Err("same-authority revision change must fail over TCP".into());
    };
    assert_eq!(broker_code(error)?, BrokerErrorCode::Conflict);

    // A different authority at a lower revision is already superseded.
    let stale = client.authority_record(
        key.clone(),
        AuthorityRevision::new(40)?,
        AuthorityId::new("runtime-b")?,
        AuthorityOwnerId::new("process-b")?,
    );
    let Err(error) = stale else {
        return Err("lower revision from another authority must fail over TCP".into());
    };
    assert_eq!(broker_code(error)?, BrokerErrorCode::StaleFence);

    // Succession with a strictly higher revision replaces the record.
    let successor = AuthorityId::new("runtime-b")?;
    let successor_owner = AuthorityOwnerId::new("process-b")?;
    let succeeded = client.authority_record(
        key.clone(),
        AuthorityRevision::new(42)?,
        successor.clone(),
        successor_owner.clone(),
    )?;
    assert_eq!(succeeded, AuthorityRevision::new(42)?);

    // Release tombstones the key at the recorded revision.
    let floor = client.authority_release(key.clone(), succeeded, successor_owner)?;
    assert_eq!(floor, AuthorityRevision::new(42)?);

    // ABA protection: the exact released revision can never be re-recorded.
    let aba = client.authority_record(
        key.clone(),
        AuthorityRevision::new(42)?,
        successor.clone(),
        AuthorityOwnerId::new("process-c")?,
    );
    let Err(error) = aba else {
        return Err("released floor must fence equal revision over TCP".into());
    };
    assert_eq!(broker_code(error)?, BrokerErrorCode::StaleFence);

    // Succession above the floor re-creates a live record.
    let revival = client.authority_record(
        key,
        AuthorityRevision::new(43)?,
        AuthorityId::new("runtime-c")?,
        AuthorityOwnerId::new("process-c")?,
    )?;
    assert_eq!(revival, AuthorityRevision::new(43)?);

    client.close();
    server.stop()?;
    Ok(())
}

#[test]
fn releasing_an_unknown_authority_key_is_rejected_over_real_tcp() -> Result<(), Box<dyn Error>> {
    let directory = tempdir()?;
    let server = spawn_test_server(directory.path().join("authority-e2e-missing.json"))?;
    let mut client = client(server.address)?;

    let missing = client.authority_release(
        AuthorityFenceKey::new("leadership:never-recorded")?,
        AuthorityRevision::new(1)?,
        AuthorityOwnerId::new("process-a")?,
    );
    let Err(error) = missing else {
        return Err("release of an unknown key must fail over TCP".into());
    };
    assert_eq!(broker_code(error)?, BrokerErrorCode::NotFound);

    client.close();
    server.stop()?;
    Ok(())
}
