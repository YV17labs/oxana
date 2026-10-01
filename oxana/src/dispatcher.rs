use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::JobId;
use crate::error::OxanaError;
use crate::queue::{QueueConfig, QueueThrottle};
use crate::runtime::Runtime;
use crate::semaphores_map::{QueueControl, QueuePermit};
use crate::storage_internal::StorageInternal;
use crate::throttler::Throttler;
use crate::worker_event::WorkerJob;

#[derive(Debug)]
enum PopError {
    BeforeClaim(OxanaError),
    // A claim is known to exist, or LMOVE may have committed before failing.
    Claim(OxanaError),
}

pub async fn run<DT>(
    config: Arc<Runtime<DT>>,
    queue_config: QueueConfig,
    queue_key: String,
    job_tx: mpsc::Sender<WorkerJob>,
    queue_control: Arc<QueueControl>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    loop {
        let Some(permit) = acquire_while_running(&config.cancel_token, &queue_control).await else {
            tracing::debug!("Stopping dispatcher for queue {}", queue_key);
            break;
        };

        tokio::select! {
            result = pop_queue_message(&config.storage.internal, &queue_config, &queue_key, config.settings.dequeue_timeout, config.settings.throttled_queue_fallback_wait) => {
                let result = match result {
                    Ok(job_id) => Ok(job_id),
                    Err(PopError::BeforeClaim(error)) => Err(error),
                    // Continuing would abandon a claim while this process's
                    // heartbeat prevents its resurrection. Drain and shut down.
                    Err(PopError::Claim(error)) => return Err(error),
                };
                match config.storage.internal.track_redis_result(result, config.settings.redis_failure_tolerance)? {
                    Some(Some(job_id)) => {
                        let job = WorkerJob { job_id, permit };
                        tokio::select! {
                            _ = config.cancel_token.cancelled() => break,
                            result = job_tx.send(job) => {
                                if result.is_err() {
                                    if config.cancel_token.is_cancelled() {
                                        break;
                                    }
                                    return Err(crate::OxanaError::GenericError(
                                        "Job receiver closed unexpectedly".to_string(),
                                    ));
                                }
                            }
                        }
                    }
                    Some(None) => {
                        drop(permit);
                    }
                    None => {
                        drop(permit);
                        sleep(config.settings.dispatcher_idle_sleep).await;
                    }
                }
            }
            _ = config.cancel_token.cancelled() => {
                tracing::debug!("Stopping dispatcher for queue {}", queue_key);
                drop(permit);
                break;
            }
        }
    }

    Ok(())
}

async fn acquire_while_running(
    cancel_token: &CancellationToken,
    queue_control: &Arc<QueueControl>,
) -> Option<QueuePermit> {
    tokio::select! {
        biased;
        _ = cancel_token.cancelled() => None,
        permit = queue_control.acquire() => Some(permit),
    }
}

async fn pop_queue_message(
    storage: &StorageInternal,
    queue_config: &QueueConfig,
    queue_key: &str,
    dequeue_timeout: std::time::Duration,
    throttled_queue_fallback_wait: std::time::Duration,
) -> Result<Option<JobId>, PopError> {
    match &queue_config.throttle {
        Some(throttle) => {
            pop_queue_message_w_throttle(
                storage,
                queue_key,
                throttle,
                throttled_queue_fallback_wait,
            )
            .await
        }
        None => pop_queue_message_wo_throttle(storage, queue_key, dequeue_timeout).await,
    }
}

async fn pop_queue_message_wo_throttle(
    storage: &StorageInternal,
    queue_key: &str,
    timeout: Duration,
) -> Result<Option<JobId>, PopError> {
    let job_id = claim_job(storage, queue_key).await?;
    if job_id.is_none() {
        sleep(timeout).await;
    }
    Ok(job_id)
}

async fn pop_queue_message_w_throttle(
    storage: &StorageInternal,
    queue_key: &str,
    throttle: &QueueThrottle,
    fallback_wait: Duration,
) -> Result<Option<JobId>, PopError> {
    let pool = storage.pool().await.map_err(PopError::BeforeClaim)?;
    let throttler = Throttler::new(pool, queue_key, throttle.limit, throttle.window_ms);

    let state = throttler.state().await.map_err(PopError::BeforeClaim)?;

    if state.is_allowed
        && let Some(job_id) = claim_job(storage, queue_key).await?
    {
        let cost = storage
            .get_claimed_job(&job_id)
            .await
            .map_err(PopError::Claim)?
            .and_then(|envelope| envelope.meta.throttle_cost);
        throttler.consume(cost).await.map_err(PopError::Claim)?;
        return Ok(Some(job_id));
    }

    let wait = state
        .throttled_for
        .and_then(|millis| u64::try_from(millis).ok())
        .map_or(fallback_wait, Duration::from_millis);
    sleep(wait).await;
    Ok(None)
}

async fn claim_job(storage: &StorageInternal, queue_key: &str) -> Result<Option<JobId>, PopError> {
    let mut redis = storage.connection().await.map_err(PopError::BeforeClaim)?;
    storage
        .dequeue_w_conn(&mut redis, queue_key)
        .await
        .map_err(PopError::Claim)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use testresult::TestResult;
    use tokio::sync::mpsc;

    use super::{acquire_while_running, run};
    use crate::config::{Config, RuntimeSettings};
    use crate::runtime::Runtime;
    use crate::semaphores_map::QueueControlsMap;
    use crate::test_helper::random_string;
    use crate::worker_event::WorkerJob;
    use crate::{QueueConfig, QueueRuntimeConfig, Storage, StorageBuilderTimeouts};

    #[tokio::test]
    async fn cancelled_dispatcher_does_not_acquire_available_capacity() {
        let cancel_token = tokio_util::sync::CancellationToken::new();
        cancel_token.cancel();
        let queue_controls = QueueControlsMap::new();
        let queue_control = queue_controls
            .get_or_create("queue".to_string(), QueueRuntimeConfig::new(1))
            .await;

        let permit = acquire_while_running(&cancel_token, &queue_control).await;

        assert!(permit.is_none());
    }

    #[tokio::test]
    async fn dispatcher_retries_connection_failure_before_claiming() -> TestResult {
        #[derive(serde::Serialize)]
        struct TestJob;
        impl crate::Job for TestJob {}

        dotenvy::from_filename(".env.test").ok();
        let queue = random_string();
        let storage = Storage::builder()
            .namespace(random_string())
            .max_pool_size(1)
            .timeouts(StorageBuilderTimeouts {
                wait: Some(Duration::from_millis(20)),
                ..Default::default()
            })
            .build_from_redis_url(std::env::var("REDIS_URL")?)?;
        let envelope = crate::JobEnvelope::new(queue.clone(), TestJob)?;
        storage.internal.enqueue(envelope.clone()).await?;
        let mut settings = RuntimeSettings::new();
        settings.dispatcher_idle_sleep = Duration::from_millis(10);
        let runtime = Arc::new(Runtime::new(storage.clone(), Config::<()>::new(), settings));
        let controls = QueueControlsMap::new();
        let control = controls
            .get_or_create(queue.clone(), QueueRuntimeConfig::new(1))
            .await;
        let (job_tx, mut job_rx) = mpsc::channel(1);
        let held_connection = storage.internal.connection().await?;
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(run(
            Arc::clone(&runtime),
            QueueConfig::as_static(&queue),
            queue,
            job_tx,
            control,
        ));

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            tasks.try_join_next().is_none(),
            "a pool timeout must be tolerated before claiming"
        );
        assert!(job_rx.try_recv().is_err());
        drop(held_connection);
        let job = tokio::time::timeout(Duration::from_secs(2), job_rx.recv())
            .await?
            .unwrap();
        assert_eq!(job.job_id, envelope.id);
        runtime.cancel_token.cancel();
        drop(job);
        tokio::time::timeout(Duration::from_secs(2), tasks.join_next())
            .await?
            .unwrap()??;
        assert_eq!(controls.busy_count().await, 0);
        Ok(())
    }

    #[tokio::test]
    async fn idle_dispatcher_does_not_hold_pool_connection() -> TestResult {
        dotenvy::from_filename(".env.test").ok();
        let redis_url = std::env::var("REDIS_URL")?;
        let queue = random_string();
        let storage = Storage::builder()
            .namespace(random_string())
            .max_pool_size(1)
            .timeouts(StorageBuilderTimeouts::new(Duration::from_millis(50)))
            .build_from_redis_url(redis_url)?;
        let runtime = Arc::new(Runtime::new(
            storage.clone(),
            Config::<()>::new(),
            RuntimeSettings::new(),
        ));
        let (job_tx, _job_rx) = mpsc::channel::<WorkerJob>(1);
        let queue_controls = QueueControlsMap::new();
        let queue_control = queue_controls
            .get_or_create(queue.clone(), QueueRuntimeConfig::new(1))
            .await;

        let handle = tokio::spawn(run(
            Arc::clone(&runtime),
            QueueConfig::as_static(&queue),
            queue.clone(),
            job_tx,
            queue_control,
        ));

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(storage.internal.enqueued_count(&queue).await?, 0);

        runtime.cancel_token.cancel();
        tokio::time::timeout(Duration::from_secs(2), handle).await???;

        Ok(())
    }
}
