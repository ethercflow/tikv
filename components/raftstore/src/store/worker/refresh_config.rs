// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use crate::store::fsm::apply::{ApplyFsm, ControlFsm};
use crate::store::fsm::store::StoreFsm;
use crate::store::fsm::PeerFsm;
use batch_system::{
    BatchRouter, Fsm, FsmTypes, HandlerBuilder, Poller, PoolState, Priority, SCALE_FINISHED_CHANNEL,
};
use file_system::{set_io_type, IOType};
use std::fmt::{self, Display, Formatter};
use std::sync::{atomic::Ordering, Arc};
use std::thread;
use tikv_util::worker::Runnable;
use tikv_util::{debug, error, info, safe_panic, thd_name};

pub struct PoolController<N: Fsm, C: Fsm, H: HandlerBuilder<N, C>> {
    pub router: BatchRouter<N, C>,
    pub state: PoolState<N, C, H>,
}

impl<N, C, H> PoolController<N, C, H>
where
    N: Fsm,
    C: Fsm,
    H: HandlerBuilder<N, C>,
{
    #[allow(dead_code)]
    pub fn new(router: BatchRouter<N, C>, state: PoolState<N, C, H>) -> Self {
        PoolController { router, state }
    }
}

impl<N, C, H> PoolController<N, C, H>
where
    N: Fsm + std::marker::Send + 'static,
    C: Fsm + std::marker::Send + 'static,
    H: HandlerBuilder<N, C>,
{
    pub fn resize_to(&mut self, size: usize) -> (bool, usize) {
        let current_pool_size = self.state.pool_size.load(Ordering::Relaxed);
        if current_pool_size > size {
            return (true, current_pool_size - size);
        }
        (false, size - current_pool_size)
    }

    pub fn decrease_by(&mut self, size: usize) {
        let orignal_pool_size = self.state.pool_size.load(Ordering::Relaxed);
        let s = self
            .state
            .expected_pool_size
            .fetch_sub(size, Ordering::Relaxed);
        for _ in 0..size {
            if let Err(e) = self.state.fsm_sender.send(FsmTypes::Empty) {
                error!(
                    "failed to decrese thread pool";
                    "decrease to" => size,
                    "err" => %e,
                );
                return;
            }
        }
        info!(
            "decrease thread pool";
            "from" => orignal_pool_size,
            "decrease to" => s - 1,
            "pool" => ?self.state.name_prefix
        );
    }

    pub fn increase_by(&mut self, size: usize) {
        let name_prefix = self.state.name_prefix.clone();
        let mut workers = self.state.workers.lock().unwrap();
        self.state.id_base += size;
        for i in 0..size {
            let handler = self.state.handler_builder.build(Priority::Normal);
            let pool_size = Arc::clone(&self.state.pool_size);
            let mut poller = Poller {
                router: self.router.clone(),
                fsm_receiver: self.state.fsm_receiver.clone(),
                handler,
                max_batch_size: self.state.max_batch_size,
                reschedule_duration: self.state.reschedule_duration,
                pool_size: Some(pool_size),
                expected_pool_size: Some(Arc::clone(&self.state.expected_pool_size)),
                pool_scale_finished: Some(SCALE_FINISHED_CHANNEL.0.clone()),
                joinable_workers: Some(Arc::clone(&self.state.joinable_workers)),
            };
            let props = tikv_util::thread_group::current_properties();
            let t = thread::Builder::new()
                .name(thd_name!(format!(
                    "{}-{}",
                    name_prefix,
                    i + self.state.id_base,
                )))
                .spawn(move || {
                    tikv_util::thread_group::set_properties(props);
                    set_io_type(IOType::ForegroundWrite);
                    poller.poll();
                })
                .unwrap();
            workers.push(t);
        }
        let _ = self
            .state
            .expected_pool_size
            .fetch_add(size, Ordering::Relaxed);
        let s = self.state.pool_size.fetch_add(size, Ordering::Relaxed);
        info!("increase thread pool"; "from" => s, "increase to" => s + size, "pool" => ?self.state.name_prefix);
        SCALE_FINISHED_CHANNEL.0.send(()).unwrap();
    }

    pub fn cleanup_poller_threads(&mut self) {
        let mut joinable_workers = self.state.joinable_workers.lock().unwrap();
        let mut workers = self.state.workers.lock().unwrap();
        let mut last_error = None;
        for tid in joinable_workers.drain(..) {
            if let Some(i) = workers.iter().position(|v| v.thread().id() == tid) {
                let h = workers.swap_remove(i);
                debug!("cleanup poller waiting for {}", h.thread().name().unwrap());
                if let Err(e) = h.join() {
                    error!("failed to join worker thread: {:?}", e);
                    last_error = Some(e);
                }
            }
        }
        if let Some(e) = last_error {
            safe_panic!("failed to join worker thread: {:?}", e);
        }
    }
}

#[derive(Debug)]
pub enum Task {
    #[allow(dead_code)]
    ScalePool(String, usize),
    #[allow(dead_code)]
    CleanupPollerThreads(String),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match &self {
            Task::ScalePool(type_, size) => {
                write!(f, "Scale pool ")?;
                match type_.as_str() {
                    "apply" => write!(f, "ajusts apply: {} ", size),
                    "raft" => write!(f, "ajusts raft: {} ", size),
                    _ => unreachable!(),
                }
            }
            Task::CleanupPollerThreads(type_) => {
                write!(f, "Cleanup {}'s poller threads", type_)
            }
        }
    }
}

pub struct Runner<EK, ER, AH, RH>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    AH: HandlerBuilder<ApplyFsm<EK>, ControlFsm>,
    RH: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK, ER>>,
{
    apply_pool: PoolController<ApplyFsm<EK>, ControlFsm, AH>,
    raft_pool: PoolController<PeerFsm<EK, ER>, StoreFsm<EK, ER>, RH>,
}

impl<EK, ER, AH, RH> Runner<EK, ER, AH, RH>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    AH: HandlerBuilder<ApplyFsm<EK>, ControlFsm>,
    RH: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK, ER>>,
{
    #[allow(dead_code)]
    pub fn new(
        apply_router: BatchRouter<ApplyFsm<EK>, ControlFsm>,
        raft_router: BatchRouter<PeerFsm<EK, ER>, StoreFsm<EK, ER>>,
        apply_pool_state: PoolState<ApplyFsm<EK>, ControlFsm, AH>,
        raft_pool_state: PoolState<PeerFsm<EK, ER>, StoreFsm<EK, ER>, RH>,
    ) -> Self {
        let apply_pool = PoolController::new(apply_router, apply_pool_state);
        let raft_pool = PoolController::new(raft_router, raft_pool_state);

        Runner {
            apply_pool,
            raft_pool,
        }
    }

    fn resize_raft_pool(&mut self, size: usize) {
        match self.raft_pool.resize_to(size) {
            (_, 0) => {
                SCALE_FINISHED_CHANNEL.0.send(()).unwrap();
                return;
            }
            (true, s) => self.raft_pool.decrease_by(s),
            (false, s) => self.raft_pool.increase_by(s),
        }
        info!("resize_raft_pool");
    }

    fn resize_apply_pool(&mut self, size: usize) {
        match self.apply_pool.resize_to(size) {
            (_, 0) => {
                SCALE_FINISHED_CHANNEL.0.send(()).unwrap();
                return;
            }
            (true, s) => self.apply_pool.decrease_by(s),
            (false, s) => self.apply_pool.increase_by(s),
        }
        info!("resize_apply_pool");
    }

    fn cleanup_poller_threads(&mut self, type_: &str) {
        println!("*** type_: {}", type_);
        match type_ {
            "apply" => self.apply_pool.cleanup_poller_threads(),
            "raft" => self.raft_pool.cleanup_poller_threads(),
            _ => unreachable!(),
        };
        SCALE_FINISHED_CHANNEL.0.send(()).unwrap();
        info!("cleanup_poller_threads");
    }
}

impl<EK, ER, AH, RH> Runnable for Runner<EK, ER, AH, RH>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    AH: HandlerBuilder<ApplyFsm<EK>, ControlFsm> + std::marker::Send,
    RH: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK, ER>> + std::marker::Send,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::ScalePool(type_, size) => {
                match type_.as_str() {
                    "apply" => self.resize_apply_pool(size),
                    "raft" => self.resize_raft_pool(size),
                    _ => unreachable!(),
                }
                info!("run ScalePool");
            }
            Task::CleanupPollerThreads(type_) => {
                self.cleanup_poller_threads(type_.as_str());
                info!("run CleanupPollerThreads");
            }
        }
    }
}
