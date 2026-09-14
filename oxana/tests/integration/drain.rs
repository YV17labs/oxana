use crate::shared::*;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use testresult::TestResult;
use tokio::sync::Notify;

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(25);
const DEAD_PROCESS_THRESHOLD: Duration = Duration::from_millis(500);
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Serialize)]
struct QueueDynamic(i32);

#[derive(Serialize)]
struct QueueStatic;

impl oxana::Queue for QueueDynamic {
    fn key(&self) -> String {
        format!(
            "dynamic#{}",
            oxana::value_to_queue_key(serde_json::to_value(self).unwrap_or_default())
        )
    }

    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_dynamic("dynamic")
    }
}

impl oxana::Queue for QueueStatic {
    fn to_config() -> oxana::QueueConfig {
        oxana::QueueConfig::as_static("static")
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DrainFailJob;

impl oxana::Job for DrainFailJob {}

struct DrainFailWorker;

impl oxana::FromContext<()> for DrainFailWorker {
    fn from_context(_ctx: &()) -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl oxana::Worker<DrainFailJob> for DrainFailWorker {
    type Error = WorkerError;

    async fn run_batch(
        &self,
        _jobs: Vec<oxana::BatchItem<DrainFailJob>>,
    ) -> Result<(), WorkerError> {
        Err(WorkerError::Generic("drain failed".to_string()))
    }
}

#[tokio::test]
pub async fn test_drain() -> TestResult {
    let redis_pool = setup();
    let ctx = ();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let runtime = storage
        .runtime(ctx)
        .queue::<QueueDynamic>()
        .queue::<QueueStatic>()
        .worker::<WorkerNoop, WorkerNoopJob>()
        .exit_when_processed(2);

    storage.enqueue(QueueDynamic(1), WorkerNoopJob {}).await?;
    storage.enqueue(QueueDynamic(2), WorkerNoopJob {}).await?;
    storage.enqueue(QueueStatic, WorkerNoopJob {}).await?;
    storage.enqueue(QueueStatic, WorkerNoopJob {}).await?;

    assert_eq!(storage.jobs_count().await?, 4);
    assert_eq!(storage.enqueued_count(QueueDynamic(1)).await?, 1);
    assert_eq!(storage.enqueued_count(QueueDynamic(2)).await?, 1);
    assert_eq!(storage.enqueued_count(QueueDynamic(3)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueStatic).await?, 2);

    let stats = runtime.drain(QueueDynamic(1)).await?;

    assert_eq!(storage.jobs_count().await?, 3);
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.succeeded, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(1)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(2)).await?, 1);
    assert_eq!(storage.enqueued_count(QueueDynamic(3)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueStatic).await?, 2);

    let stats = runtime.drain(QueueDynamic(2)).await?;

    assert_eq!(storage.jobs_count().await?, 2);
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.succeeded, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(1)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(2)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(3)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueStatic).await?, 2);

    let stats = runtime.drain(QueueStatic).await?;

    assert_eq!(storage.jobs_count().await?, 0);
    assert_eq!(stats.processed, 2);
    assert_eq!(stats.succeeded, 2);
    assert_eq!(stats.failed, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(1)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(2)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueDynamic(3)).await?, 0);
    assert_eq!(storage.enqueued_count(QueueStatic).await?, 0);

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct DrainSlowJob;

impl oxana::Job for DrainSlowJob {}

#[derive(Clone, Default)]
struct DrainSlowState {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

struct DrainSlowWorker(DrainSlowState);

impl oxana::FromContext<DrainSlowState> for DrainSlowWorker {
    fn from_context(ctx: &DrainSlowState) -> Self {
        Self(ctx.clone())
    }
}

#[async_trait::async_trait]
impl oxana::Worker<DrainSlowJob> for DrainSlowWorker {
    type Error = WorkerError;

    async fn run_batch(
        &self,
        _jobs: Vec<oxana::BatchItem<DrainSlowJob>>,
    ) -> Result<(), WorkerError> {
        self.0.started.notify_one();
        self.0.release.notified().await;
        Ok(())
    }
}

async fn sweep_drain_namespace(storage: &oxana::Storage, pool: deadpool_redis::Pool) -> TestResult {
    let peer = oxana::Storage::builder()
        .namespace(storage.namespace())
        .build_from_pool(pool)?;
    // No queues are registered, so the peer only runs background maintenance.
    let sweeper = peer
        .runtime(())
        .heartbeat_interval(HEARTBEAT_INTERVAL)
        .dead_process_threshold(DEAD_PROCESS_THRESHOLD)
        .resurrect_scan_interval(HEARTBEAT_INTERVAL)
        .shutdown_on(async {
            tokio::time::sleep(DEAD_PROCESS_THRESHOLD * 3).await;
            Ok(())
        });

    tokio::time::timeout(TEST_TIMEOUT, sweeper.run()).await??;
    Ok(())
}

#[tokio::test]
pub async fn test_drain_heartbeats_while_jobs_run() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let state = DrainSlowState::default();
    let runtime = storage
        .runtime(state.clone())
        .queue::<QueueStatic>()
        .worker::<DrainSlowWorker, DrainSlowJob>()
        .heartbeat_interval(HEARTBEAT_INTERVAL)
        .dead_process_threshold(DEAD_PROCESS_THRESHOLD);

    storage.enqueue(QueueStatic, DrainSlowJob).await?;
    assert!(storage.processes().await?.is_empty());

    let drain = tokio::spawn(async move { runtime.drain(QueueStatic).await });
    tokio::time::timeout(TEST_TIMEOUT, state.started.notified()).await?;
    assert_eq!(storage.processes().await?.len(), 1);

    // The job stays blocked across several liveness windows. A single initial
    // ping cannot prevent the peer from resurrecting it during this time.
    sweep_drain_namespace(&storage, redis_pool).await?;
    assert_eq!(storage.processes().await?.len(), 1);
    assert_eq!(storage.enqueued_count(QueueStatic).await?, 0);

    state.release.notify_one();
    let stats = tokio::time::timeout(TEST_TIMEOUT, drain).await???;
    assert_eq!(stats.processed, 1);
    assert_eq!(stats.succeeded, 1);
    assert!(storage.processes().await?.is_empty());
    Ok(())
}

#[tokio::test]
pub async fn test_cancelled_drain_can_be_resurrected() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let state = DrainSlowState::default();
    let runtime = storage
        .runtime(state.clone())
        .queue::<QueueStatic>()
        .worker::<DrainSlowWorker, DrainSlowJob>()
        .heartbeat_interval(HEARTBEAT_INTERVAL)
        .dead_process_threshold(DEAD_PROCESS_THRESHOLD);

    storage.enqueue(QueueStatic, DrainSlowJob).await?;
    let drain = tokio::spawn(async move { runtime.drain(QueueStatic).await });
    tokio::time::timeout(TEST_TIMEOUT, state.started.notified()).await?;
    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());

    sweep_drain_namespace(&storage, redis_pool).await?;
    assert!(storage.processes().await?.is_empty());
    assert_eq!(
        storage.enqueued_count(QueueStatic).await?,
        1,
        "the cancelled drain's job should be available for recovery"
    );
    Ok(())
}

#[tokio::test]
pub async fn test_drain_preserves_running_runtime_process() -> TestResult {
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(setup())?;
    let state = DrainSlowState::default();
    let runtime = storage
        .runtime(state.clone())
        .queue::<QueueStatic>()
        .worker::<DrainSlowWorker, DrainSlowJob>()
        // Keep the next heartbeat from hiding an incorrect removal.
        .heartbeat_interval(Duration::from_secs(5))
        .dead_process_threshold(Duration::from_secs(10))
        .resurrect_scan_interval(Duration::from_secs(30))
        .exit_when_processed(1);

    storage.enqueue(QueueStatic, DrainSlowJob).await?;
    let runner = tokio::spawn(async move { runtime.run().await });
    tokio::time::timeout(TEST_TIMEOUT, state.started.notified()).await?;
    let process_id = storage.processes().await?[0].id();

    let stats = storage.runtime(()).drain(QueueDynamic(0)).await?;
    assert_eq!(stats.processed, 0);
    let processes = storage.processes().await?;
    assert_eq!(processes.len(), 1);
    assert_eq!(processes[0].id(), process_id);

    state.release.notify_one();
    tokio::time::timeout(TEST_TIMEOUT, runner).await???;
    assert!(storage.processes().await?.is_empty());
    Ok(())
}

#[tokio::test]
pub async fn test_concurrent_drains_have_independent_processes() -> TestResult {
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(setup())?;
    let state = DrainSlowState::default();
    let runtime = Arc::new(
        storage
            .runtime(state.clone())
            .queue::<QueueStatic>()
            .queue::<QueueDynamic>()
            .worker::<DrainSlowWorker, DrainSlowJob>()
            .heartbeat_interval(Duration::from_secs(5))
            .dead_process_threshold(Duration::from_secs(10)),
    );

    storage.enqueue(QueueStatic, DrainSlowJob).await?;
    let drain = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move { runtime.drain(QueueStatic).await }
    });
    tokio::time::timeout(TEST_TIMEOUT, state.started.notified()).await?;
    let process_id = storage.processes().await?[0].id();

    let stats = runtime.drain(QueueDynamic(0)).await?;
    assert_eq!(stats.processed, 0);
    let processes = storage.processes().await?;
    assert_eq!(processes.len(), 1);
    assert_eq!(processes[0].id(), process_id);

    state.release.notify_one();
    let stats = tokio::time::timeout(TEST_TIMEOUT, drain).await???;
    assert_eq!(stats.succeeded, 1);
    assert!(storage.processes().await?.is_empty());
    Ok(())
}

#[tokio::test]
pub async fn test_drain_uses_custom_error_formatter() -> TestResult {
    let redis_pool = setup();
    let storage = oxana::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool)?;
    let runtime = storage
        .runtime(())
        .queue::<QueueStatic>()
        .worker::<DrainFailWorker, DrainFailJob>()
        .error_formatter(|error| format!("diagnostic:\n{error:?}"));

    storage.enqueue(QueueStatic, DrainFailJob).await?;

    let stats = runtime.drain(QueueStatic).await?;
    let dead = storage
        .list_dead(&oxana::QueueListOpts {
            count: 1,
            offset: 0,
        })
        .await?;

    assert_eq!(stats.failed, 1);
    assert_eq!(
        dead[0].meta.error.as_deref(),
        Some("diagnostic:\nGeneric(\"drain failed\")")
    );

    Ok(())
}
