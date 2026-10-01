mod redis_proxy;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use deadpool_redis::redis::AsyncCommands;
use oxana::{QueueConfig, QueueThrottle, Storage};
use testresult::TestResult;
use tokio::sync::{Mutex, Notify};

use crate::shared::{random_string, setup};
use redis_proxy::RedisProxy;

const WAIT: Duration = Duration::from_secs(5);

#[derive(serde::Serialize, serde::Deserialize)]
struct RecoveryJob {
    block: bool,
}

impl oxana::Job for RecoveryJob {}

struct RecoveryQueue(String);

impl oxana::Queue for RecoveryQueue {
    fn key(&self) -> String {
        self.0.clone()
    }

    fn to_config() -> QueueConfig {
        QueueConfig::as_dynamic("recovery")
    }
}

#[derive(Default)]
struct State {
    executed: Mutex<Vec<String>>,
    started: Notify,
    release: Notify,
}

struct RecoveryWorker(Arc<State>);

impl oxana::FromContext<Arc<State>> for RecoveryWorker {
    fn from_context(state: &Arc<State>) -> Self {
        Self(Arc::clone(state))
    }
}

#[async_trait::async_trait]
impl oxana::Worker<RecoveryJob> for RecoveryWorker {
    type Error = std::io::Error;

    async fn run_batch(&self, jobs: Vec<oxana::BatchItem<RecoveryJob>>) -> Result<(), Self::Error> {
        for job in jobs {
            self.0.executed.lock().await.push(job.ctx.meta.id);
            if job.job.block {
                self.0.started.notify_one();
                self.0.release.notified().await;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Fault {
    ClaimReply,
    Payload,
    ThrottledPayload,
    ThrottleConsume,
}

async fn recovers_after_claim_error(fault: Fault, active_handler: bool) -> TestResult {
    let pool = setup();
    let namespace = random_string();
    let queue = random_string();
    let storage = Storage::builder()
        .namespace(&namespace)
        .build_from_pool(pool.clone())?;
    let active_id = if active_handler {
        Some(
            storage
                .enqueue(RecoveryQueue(queue.clone()), RecoveryJob { block: true })
                .await?,
        )
    } else {
        None
    };
    let job_id = storage
        .enqueue(RecoveryQueue(queue.clone()), RecoveryJob { block: false })
        .await?;
    let prefix = match fault {
        Fault::ClaimReply => vec!["LMOVE".into(), format!("{namespace}:queue:{queue}")],
        Fault::Payload | Fault::ThrottledPayload => {
            vec!["HGET".into(), format!("{namespace}:jobs"), job_id.clone()]
        }
        Fault::ThrottleConsume => vec!["ZADD".into(), format!("oxana:throttler:{queue}")],
    };
    let proxy = RedisProxy::start(prefix, matches!(fault, Fault::ClaimReply)).await?;
    let runtime_storage = Storage::builder()
        .namespace(&namespace)
        .build_from_pool(proxy.pool.clone())?;
    let mut queue_config = QueueConfig::as_static(&queue).concurrency(2);
    if matches!(fault, Fault::ThrottledPayload | Fault::ThrottleConsume) {
        queue_config = queue_config.throttle(QueueThrottle {
            limit: 100,
            window_ms: 1000,
        });
    }
    let state = Arc::new(State::default());
    // Leave redis_failure_tolerance at its default: a single claim failure must
    // initiate shutdown even while other Redis operations continue succeeding.
    let runtime = runtime_storage
        .runtime(Arc::clone(&state))
        .queue_with(queue_config.clone())
        .worker::<RecoveryWorker, RecoveryJob>()
        .heartbeat_interval(Duration::from_millis(20))
        .dequeue_timeout(Duration::from_millis(10))
        .shutdown_timeout(WAIT);
    let mut runners = tokio::task::JoinSet::new();
    runners.spawn(runtime.run());

    if active_handler {
        tokio::time::timeout(WAIT, state.started.notified()).await?;
    }
    let mut redis = pool.get().await?;
    tokio::time::timeout(WAIT, async {
        while !proxy.injected.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;

    let processes_key = format!("{namespace}:processes");
    if active_handler {
        let processes: Vec<String> = redis.zrange(&processes_key, 0, -1).await?;
        assert_eq!(processes.len(), 1);
        let heartbeat: f64 = redis.zscore(&processes_key, &processes[0]).await?;
        tokio::time::timeout(WAIT, async {
            loop {
                let later: f64 = redis.zscore(&processes_key, &processes[0]).await?;
                if later > heartbeat {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            TestResult::Ok(())
        })
        .await??;
        assert!(
            runners.try_join_next().is_none(),
            "shutdown must wait for the active handler"
        );
        assert_eq!(
            *state.executed.lock().await,
            vec![active_id.clone().unwrap()]
        );
        state.release.notify_one();
    }

    let result = tokio::time::timeout(WAIT, runners.join_next())
        .await?
        .unwrap()?;
    let Err(oxana::OxanaError::DeadpoolRedisError(error)) = result else {
        panic!("expected the initiating Redis error, got {result:?}");
    };
    if matches!(fault, Fault::ClaimReply) {
        assert!(error.is_io_error(), "{error}");
    } else {
        assert!(
            error.to_string().contains("injected claim failure"),
            "{error}"
        );
    }
    let processes: Vec<String> = redis.zrange(&processes_key, 0, -1).await?;
    assert!(
        processes.is_empty(),
        "registration must be removed after draining"
    );
    let processing: Vec<String> = redis.keys(format!("{namespace}:processing:*")).await?;
    assert_eq!(processing.len(), 1);
    let claimed: Vec<String> = redis.lrange(&processing[0], 0, -1).await?;
    assert_eq!(claimed, vec![job_id.clone()]);
    assert!(!state.executed.lock().await.contains(&job_id));
    let envelope = storage.get_job(&job_id).await?.unwrap();
    assert_eq!(envelope.meta.retries, 0);

    // A distinct process identity uses the ordinary resurrection path.
    let replacement = storage
        .runtime(Arc::clone(&state))
        .queue_with(queue_config)
        .worker::<RecoveryWorker, RecoveryJob>()
        .dequeue_timeout(Duration::from_millis(10))
        .resurrect_scan_interval(Duration::from_millis(20))
        .exit_when_processed(1);
    runners.spawn(replacement.run());
    let stats = tokio::time::timeout(WAIT, runners.join_next())
        .await?
        .unwrap()??;
    assert_eq!(stats.processed, 1);
    let mut expected: Vec<String> = active_id.into_iter().collect();
    expected.push(job_id.clone());
    assert_eq!(*state.executed.lock().await, expected);
    assert!(storage.get_job(&job_id).await?.is_none());
    let remaining: usize = redis.llen(&processing[0]).await?;
    assert_eq!(remaining, 0);
    Ok(())
}

#[tokio::test]
async fn committed_claim_with_lost_reply_is_recovered() -> TestResult {
    recovers_after_claim_error(Fault::ClaimReply, false).await
}

#[tokio::test]
async fn payload_error_drains_active_handler_then_recovers_claim() -> TestResult {
    recovers_after_claim_error(Fault::Payload, true).await
}

#[tokio::test]
async fn throttled_payload_error_is_recovered() -> TestResult {
    recovers_after_claim_error(Fault::ThrottledPayload, false).await
}

#[tokio::test]
async fn throttle_consume_error_is_recovered() -> TestResult {
    recovers_after_claim_error(Fault::ThrottleConsume, false).await
}

#[tokio::test]
async fn throttle_state_error_before_claim_is_retried() -> TestResult {
    let pool = setup();
    let queue = random_string();
    let storage = Storage::builder()
        .namespace(random_string())
        .build_from_pool(pool)?;
    let job_id = storage
        .enqueue(RecoveryQueue(queue.clone()), RecoveryJob { block: false })
        .await?;
    let proxy = RedisProxy::start(
        vec!["ZCARD".into(), format!("oxana:throttler:{queue}")],
        false,
    )
    .await?;
    let runtime_storage = Storage::builder()
        .namespace(storage.namespace())
        .build_from_pool(proxy.pool.clone())?;
    let state = Arc::new(State::default());
    let runtime = runtime_storage
        .runtime(Arc::clone(&state))
        .queue_with(QueueConfig::as_static(queue).throttle(QueueThrottle {
            limit: 100,
            window_ms: 1000,
        }))
        .worker::<RecoveryWorker, RecoveryJob>()
        .dispatcher_idle_sleep(Duration::from_millis(10))
        .exit_when_processed(1);
    let stats = tokio::time::timeout(WAIT, runtime.run()).await??;
    assert!(proxy.injected.load(Ordering::SeqCst));
    assert_eq!(stats.processed, 1);
    assert_eq!(*state.executed.lock().await, vec![job_id.clone()]);
    assert!(storage.get_job(&job_id).await?.is_none());
    Ok(())
}
