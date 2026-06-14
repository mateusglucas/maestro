# solicita novos jobs ao server, spawn de workers, distribui para workers e monitora status deles (salvar PID, creation time e job enviado)
# envia heartbeat pro server (que é substituível por qualquer outra requisição que seja feita)
# se worker morrer, spawn novo worker e redistribui o job dele pro novo worker
# workers são daemon do agente. se agente morrer, workers morrem
# agente pode redimensionar numero de workers soft (espera terminar trabalho pra encerrar) ou hard (mata processo na hora). como fornecer endpoint pra esse controle? fastapi ou CLI?
# worker sinaliza job completo pro agent. agent envia resultado pro server junto com artefatos (comprimidos em tarball)
# usa queue de dados compartilhada entre todos workers. quando precisa reduzir jobs, adiciona None na queue. Um worker que recebe None automaticamente se encerra, diminuindo numero de workers (isso pra redução soft). Redução hard tem que matar aleatoriamente algum worker.
# tem queue agent->workers e queue workers->agent, em que cada worker sinaliza pro agente quando pega um job específico, ou finaliza um job, ou se encerra.
# sem estados permanentes, sql..., tudo efêmero. Agent morre, workers morrem.
# ID do agente deve ter parcela que indique maquina (hostname?) e parcela UUID like, pro server poder identificar maquina e se o agent é novo ou não.

import tomllib
from multiprocessing import Process, SimpleQueue
import uuid
import asyncio
import threading
import enum
import time
import httpx
import socket
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
import tempfile
from pathlib import Path
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, JSON, DateTime, ForeignKey, Enum, Boolean, Integer
from sqlalchemy import select, update, func
from datetime import datetime
from typing import Any
import shutil
import tarfile
from contextlib import nullcontext

import pyzstd

from common import JobResults, JobStatus


from sqlalchemy.sql.expression import true

class Event(enum.StrEnum):
    WORKER_IDLE = enum.auto()
    JOB_COMPLETED = enum.auto()
    JOB_FAILED = enum.auto()

class Base(DeclarativeBase):
    pass

class Job(Base):
    __tablename__ = "jobs"

    id:                 Mapped[str]                     = mapped_column(String, primary_key=True)
    parameters:         Mapped[dict[str, Any]]          = mapped_column(JSON)
    received_at:        Mapped[datetime]                = mapped_column(DateTime)
    assigned_worker_id: Mapped[str | None]              = mapped_column(ForeignKey("workers.id"))
    assigned_at:        Mapped[datetime | None]         = mapped_column(DateTime)
    status:             Mapped[JobStatus]               = mapped_column(Enum(JobStatus, native_enum=False))

class Worker(Base):
    __tablename__ = "workers"

    id:         Mapped[str]  = mapped_column(String, primary_key=True)
    pid:        Mapped[int]  = mapped_column(Integer)
    is_alive:   Mapped[bool] = mapped_column(Boolean)

class Agent:
    def __init__(self, work_fn):
        self._init_config()

        self.work_fn = work_fn

        self.id = None
        self.last_heartbeat = None
        self.pending_terminations = 0
        self.events_queue = SimpleQueue()

        self.thread = threading.Thread(target=self._main_thread, daemon=True)
        self.thread_exception = None

        self.workers: dict[str, Process] = {}
        self.job_queues: dict[str, SimpleQueue] = {}

    def start(self):
        self.thread.start()

    def join(self):
        self.thread.join()

        if self.thread_exception is not None:
            raise self.thread_exception

    def _init_config(self):
        with open("agent_config.toml", "rb") as f:
            self.config = tomllib.load(f)

    def _main_thread(self):
        try:
            asyncio.run(self._main_task())
        except BaseException as e:
            self.thread_exception = e
            raise

    async def _main_task(self):
        server_url = self.config['server']['url']
        async with httpx.AsyncClient(base_url = server_url) as client:
            with tempfile.TemporaryDirectory() as runtime_dir:
                runtime_dir = Path(runtime_dir)
                db_path = runtime_dir / "maestro.db"

                db_engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
                db_sessionmaker = async_sessionmaker(bind=db_engine, expire_on_commit=False)
                async with db_engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)

                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self.heartbeat(client))
                    tg.create_task(self.workers_monitor(db_sessionmaker, runtime_dir))
                    tg.create_task(self.event_loop(client, db_sessionmaker, runtime_dir))

    async def heartbeat(self, client: httpx.AsyncClient):
        # Envia heartbeat pro server. Pula envio caso alguma outra mensagem já tenha sido enviada pro server,
        # já que qualquer mensagem também serve como heartbeat para o server.
        interval = self.config['heartbeat']['interval']
        retry_interval = 5

        while True:
            time_to_sleep = min(retry_interval, interval) # worst scenario (retry)

            if self.last_heartbeat is not None and (delay:=self.last_heartbeat + interval - time.time()) > 0:
                time_to_sleep = delay
            elif self.id is not None:
                heartbeat_time = time.time()
                try:
                    response = await client.post(f"/heartbeat/{self.id}")
                except httpx.RequestError:
                    pass
                else:
                    if response.is_success:
                        self.last_heartbeat = heartbeat_time
                        time_to_sleep = interval - (time.time()-heartbeat_time)

            await asyncio.sleep(time_to_sleep)

    async def workers_monitor(self, db_sessionmaker: async_sessionmaker[AsyncSession], runtime_dir: Path):
        # Identifica workers mortos, redisponibiliza os jobs interrompidos e cria novos workers
        poll_interval = 5
        
        while True:
            cnt_alive_workers = 0
            async with db_sessionmaker.begin() as session:
                stmt = (
                    select(Worker)
                    .where(Worker.is_alive == True)
                )

                alive_workers = (await session.execute(stmt)).scalars().all()
                
                for worker in alive_workers:
                    if self.workers[worker.id].is_alive():
                        cnt_alive_workers += 1
                    else:
                        worker.is_alive = False
                        self.job_queues[worker.id] = None # deixar a queue ser fechada por ref count
                                                          # para evitar erro caso alguma corrotina ainda
                                                          # tente acessar a queue após o processo estar morto

                        stmt = (
                            update(Job)
                            .where(
                                Job.assigned_worker_id == worker.id,
                                Job.status == JobStatus.ASSIGNED
                            )
                            .values(
                                status = JobStatus.PENDING,
                                assigned_at = None,
                                assigned_worker_id = None
                            )
                            .returning(Job)
                        )

                        orphan_job = (await session.execute(stmt)).scalar_one_or_none()

            workers_to_spawn = self.config['jobs']['n_workers'] - cnt_alive_workers

            if workers_to_spawn > 0:
                async with db_sessionmaker.begin() as session:
                    for _ in range(workers_to_spawn):
                        worker_id = str(uuid.uuid4())

                        job_queue = SimpleQueue()
                        process = Process(
                            target = type(self)._worker_main,
                            args = (worker_id, self.work_fn, job_queue, self.events_queue, runtime_dir),
                            daemon = True,
                        )
                        process.start()

                        self.workers[worker_id] = process
                        self.job_queues[worker_id] = job_queue

                        worker = Worker()
                        worker.id = worker_id
                        worker.pid = process.pid
                        worker.is_alive = True

                        session.add(worker)

            workers_to_terminate = -workers_to_spawn
            if workers_to_terminate > 0:
                self.pending_terminations = workers_to_terminate

            await asyncio.sleep(poll_interval)

    @staticmethod
    def _worker_main(worker_id, work_fn, jobs_queue: SimpleQueue, events_queue: SimpleQueue, runtime_dir: Path):
        while True:
            events_queue.put({
                'type': Event.WORKER_IDLE,
                'worker_id': worker_id,
            })

            job = jobs_queue.get()

            if job['job_id'] is None:
                return

            try:
                artifact_path: Path =  runtime_dir / 'artifacts' / job['job_id']
                if artifact_path.exists():
                    shutil.rmtree(artifact_path)

                artifact_path.mkdir(parents = True, exist_ok = False)

                result = work_fn(artifact_path, **job['parameters'])

                events_queue.put({
                    'type': Event.JOB_COMPLETED,
                    'worker_id': worker_id,
                    'job_id': job['job_id'],
                    'result': result,
                })

            except Exception as e:
                events_queue.put({
                    'type': Event.JOB_FAILED,
                    'worker_id': worker_id,
                    'job_id': job['job_id'],
                    'error': str(e),
                })

    async def event_loop(self, client: httpx.AsyncClient, db_sessionmaker: async_sessionmaker[AsyncSession], runtime_dir: Path):

        retry_interval = 5

        while True:
            now = time.time()
            try:
                response = await client.post('/register_agent', json={'hostname': socket.gethostname()})
            except httpx.RequestError:
                pass
            else:
                if response.is_success:
                    self.last_heartbeat = now
                    self.id = response.json()['agent_id']
                    break
            await asyncio.sleep(retry_interval)

        async with asyncio.TaskGroup() as tg:
            while True:
                # verificar criação/destruição de workers
                event = await asyncio.to_thread(self.events_queue.get)

                if event['type'] == Event.WORKER_IDLE:
                    tg.create_task(self._request_new_job(event['worker_id'], client, db_sessionmaker))

                elif event['type'] == Event.JOB_COMPLETED:
                    tg.create_task(self._send_completed_result(event['worker_id'], 
                                                                    event['job_id'], 
                                                                    event['result'], 
                                                                    client, 
                                                                    db_sessionmaker, 
                                                                    runtime_dir))

                elif event['type'] == Event.JOB_FAILED:
                    tg.create_task(self._send_error_result(event['worker_id'], 
                                                                event['job_id'], 
                                                                event['error'], 
                                                                client, 
                                                                db_sessionmaker))

                else:
                    raise RuntimeError(f'Unknown event {event['type']}.')

    async def _send_completed_result(self, worker_id, job_id: str, result, client: httpx.AsyncClient, db_sessionmaker, runtime_dir: Path):
        async with db_sessionmaker.begin() as session:
            stmt = (
                update(Job)
                .where(
                    Job.id == job_id,
                    Job.status == JobStatus.ASSIGNED,
                    Job.assigned_worker_id == worker_id
                )
                .values(
                    status = JobStatus.COMPLETED
                )
                .returning(Job)
            )

            (await session.execute(stmt)).scalar_one()

        retry_interval = 5

        result = JobResults(status=JobStatus.COMPLETED, results=result)

        data = {
            'results': result.model_dump_json()
        }

        has_artifact = False
        artifacts_dir = runtime_dir / 'artifacts' / job_id
        archive_path = runtime_dir / 'tar' / f'{job_id}.tar.zst'

        if any(artifacts_dir.iterdir()):
            has_artifact = True

            archive_path.parent.mkdir(parents = True, exist_ok = True)

            await asyncio.to_thread(
                self.create_archive,
                artifacts_dir,
                archive_path,
            )

        with (open(archive_path, "rb") if has_artifact else nullcontext()) as file:
            while True:
                if file is not None:
                    file.seek(0)
                    files = {"artifact": file}
                else:
                    files = None

                now = time.time()
                try:
                    response = await client.post(f'/submit_result/{self.id}/{job_id}', data = data, files=files)
                except httpx.RequestError:
                    pass
                else:
                    if response.is_success:
                        self.last_heartbeat = now
                        break

                await asyncio.sleep(retry_interval)

        archive_path.unlink(missing_ok = True)
        await asyncio.to_thread(shutil.rmtree, artifacts_dir)

    @staticmethod
    def create_archive(source_dir: Path, archive_path: Path) -> None:
        with pyzstd.open(archive_path, "wb") as zst:
            with tarfile.open(fileobj=zst, mode="w|") as tar:
                tar.add(source_dir, arcname=".")

    async def _send_error_result(self, worker_id, job_id, error, client: httpx.AsyncClient, db_sessionmaker):
        async with db_sessionmaker.begin() as session:
            stmt = (
                update(Job)
                .where(
                    Job.id == job_id,
                    Job.status == JobStatus.ASSIGNED,
                    Job.assigned_worker_id == worker_id
                )
                .values(
                    status = JobStatus.FAILED
                )
                .returning(Job)
            )

            (await session.execute(stmt)).scalar_one()

        retry_interval = 5

        result = JobResults(status=JobStatus.FAILED, error=error)

        data = {
            'results': result.model_dump_json()
        }

        while True:
            now = time.time()
            try:
                response = await client.post(f'/submit_result/{self.id}/{job_id}', data = data)
            except httpx.RequestError:
                pass
            else:
                if response.is_success:
                    self.last_heartbeat = now
                    return

            await asyncio.sleep(retry_interval)


    async def _request_new_job(self, worker_id, client: httpx.AsyncClient, db_sessionmaker: async_sessionmaker[AsyncSession]):
        retry_interval = 5

        job_queue = self.job_queues[worker_id]

        while self.workers[worker_id].is_alive():
            if self.pending_terminations > 0:
                self.pending_terminations -= 1
                job_queue.put({'job_id': None})
                await asyncio.to_thread(self.workers[worker_id].join)
                return
            else:
                async with db_sessionmaker.begin() as session:
                    candidate = (
                        select(Job.id)
                        .where(Job.status == JobStatus.PENDING)
                        .order_by(Job.received_at)
                        .limit(1)
                        .scalar_subquery()
                    )

                    stmt = (
                        update(Job)
                        .where(Job.id == candidate)
                        .values(
                            status = JobStatus.ASSIGNED,
                            assigned_worker_id = worker_id,
                            assigned_at = datetime.now()
                        )
                        .returning(Job)
                    )

                    job = (await session.execute(stmt)).scalar_one_or_none()
                    if job is not None:
                        job_queue.put({'job_id': job.id, 'parameters': job.parameters})
                        return

                now = time.time()
                try:
                    response = await client.post(f'/request_job/{self.id}')
                except httpx.RequestError:
                    pass
                else:
                    if response.is_success:
                        self.last_heartbeat = now
                        job_json = response.json()
                        if job_json is not None: # None indica nenhum trabalho disponível
                            async with db_sessionmaker.begin() as session:    
                                job = Job()

                                now = datetime.now()

                                job.id = job_json['job_id']
                                job.received_at = now
                                job.assigned_at = now
                                job.status = JobStatus.ASSIGNED
                                job.assigned_worker_id = worker_id
                                job.parameters = job_json['parameters']
                                
                                session.add(job)
                                job_queue.put({'job_id': job_json['job_id'], 'parameters': job_json['parameters']})
                                return
                
            await asyncio.sleep(retry_interval)
        else: # worker not alive
            if self.pending_terminations > 0:
                self.pending_terminations -= 1

# TODO: backtest: talvez adicionar file FAIL se o job falhar, porque eu preciso enviar results pro server e tem q sinalizar de alguma forma q deu falha.