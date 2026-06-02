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
from abc import ABC, abstractmethod
import asyncio
import threading
import enum
import time
from fastapi import status
import httpx
import socket
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
import tempfile
from pathlib import Path
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy import create_engine, event, String, JSON, DateTime, ForeignKey, Enum, Boolean
from sqlalchemy import select, update, func
from datetime import datetime
from typing import Any

from sqlalchemy.sql.expression import true

class JobStatus(enum.StrEnum):
    PENDING = enum.auto()
    ASSIGNED  = enum.auto()
    SUCCESS = enum.auto()
    FAIL    = enum.auto()

class Event(enum.StrEnum):
    WORKER_IDLE = enum.auto()
    JOB_DONE = enum.auto()
    JOB_FAILED = enum.auto()

class Base(DeclarativeBase):
    pass

class Job(Base):
    __tablename__ = "jobs"

    id:                 Mapped[str]                     = mapped_column(String, primary_key=True)
    payload:            Mapped[dict[str, Any]]          = mapped_column(JSON)
    received_at:        Mapped[datetime]                = mapped_column(DateTime)
    assigned_worker_id: Mapped[str | None]              = mapped_column(ForeignKey("workers.id"))
    assigned_at:        Mapped[datetime | None]         = mapped_column(DateTime)
    status:             Mapped[JobStatus]               = mapped_column(Enum(JobStatus, native_enum=False))
    result:             Mapped[dict[str, Any] | None]   = mapped_column(JSON)


class Worker(Base):
    __tablename__ = "workers"

    id:         Mapped[str]  = mapped_column(String, primary_key=True)
    is_alive:   Mapped[bool] = mapped_column(Boolean)

class Agent(ABC):
    def __init__(self, work_fn):
        self._init_config()

        self.work_fn = work_fn

        self.id = None
        self.last_heartbeat = None
        self.pending_terminations = 0
        self.events_queue = SimpleQueue()

        self.thread = threading.Thread(target=self._main_thread, daemon=True)

        self.workers: dict[str, Process] = {}
        self.job_queues: dict[str, SimpleQueue] = {}

    def start(self):
        self.thread.start()

    def _init_config(self):
        with open("agent_config.toml", "rb") as f:
            self.config = tomllib.load(f)

    def _main_thread(self):
        asyncio.run(self._main_task())

    async def _main_task(self):
        server_url = self.config['server']['url']
        async with httpx.AsyncClient(base_url = server_url) as client:
            with tempfile.TemporaryDirectory() as runtime_dir:
                db_path = Path(runtime_dir) / "maestro.db"

                db_engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
                db_sessionmaker = async_sessionmaker(bind=db_engine, expire_on_commit=False)
                async with db_engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)

                await asyncio.gather(
                    self.heartbeat(client),
                    self.workers_monitor(db_sessionmaker),
                    self.event_loop(client, db_sessionmaker),
                )

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
                response = await client.post(f"/heartbeat/{self.id}")
                if response.is_success:
                    self.last_heartbeat = heartbeat_time
                    time_to_sleep = interval - (time.time()-heartbeat_time)

            await asyncio.sleep(time_to_sleep)

    async def workers_monitor(self, db_sessionmaker: async_sessionmaker[AsyncSession]):
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
                            args = (worker_id, self.work_fn, job_queue, self.events_queue),
                        )
                        process.start()

                        self.workers[worker_id] = process
                        self.job_queues[worker_id] = job_queue

                        worker = Worker()
                        worker.id = worker_id
                        worker.is_alive = True

                        session.add(worker)

            workers_to_terminate = -workers_to_spawn
            if workers_to_terminate > 0:
                self.pending_terminations = workers_to_terminate

            await asyncio.sleep(poll_interval)

    @staticmethod
    def _worker_main(worker_id, work_fn, jobs_queue: SimpleQueue, events_queue: SimpleQueue):
        while True:
            events_queue.put({
                'type': Event.WORKER_IDLE,
                'worker_id': worker_id,
            })

            job = jobs_queue.get()

            if job['job_id'] is None:
                return

            try:
                result = work_fn(job['payload'])

                events_queue.put({
                    'type': Event.JOB_DONE,
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

    async def event_loop(self, client: httpx.AsyncClient, db_sessionmaker: async_sessionmaker[AsyncSession]):
        # TODO: Cenários:
        #  - worker envia IDLE event e morre enquanto espera queue: a corroutine dele pra obter
        #    novo trabalho vai seguir rodando e vai obter um trabalho. No fim, sempre vamos ter
        #    um trabalho sobrando na queue.
        #  - worker pega trabalho da queue e morre antes de enviar evento de JOB_STARTED: o job
        #    vai ficar em estado QUEUED para sempre e não vai ser pego por outros jobs.
        retry_interval = 5

        while True:
            now = time.time()
            response = await client.post('/register_agent', json={'hostname': socket.gethostname()})
            if response.is_success:
                self.last_heartbeat = now
                self.id = response.json()['agent_id']
                break
            await asyncio.sleep(retry_interval)

        while True:
            # verificar criação/destruição de workers
            event = await asyncio.to_thread(self.events_queue.get())

            if event['type'] == Event.WORKER_IDLE:
                asyncio.create_task(self._request_new_job(event['worker_id'], client, db_sessionmaker))

            elif event['type'] == Event.JOB_DONE:
                # TODO
                pass

            elif event['type'] == Event.JOB_FAILED:
                # TODO
                pass

    async def _request_new_job(self, worker_id, client: httpx.AsyncClient, db_sessionmaker: async_sessionmaker[AsyncSession]):
        retry_interval = 5

        job_queue = self.job_queues[worker_id]

        while self.workers[worker_id].is_alive():
            if self.pending_terminations > 0:
                self.pending_terminations -= 1
                job_queue.put({'job_id': None})
                self.workers[worker_id].join() # TODO: pode ficar preso aqui ou é paranoia
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
                        job_queue.put({'job_id': job.id, 'payload': job.payload})
                        return

                now = time.time()
                response = await client.post(f'/request_job/{self.id}')
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
                            job.payload = job_json['payload']
                            
                            session.add(job)
                            job_queue.put({'job_id': job_json['job_id'], 'payload': job_json['payload']})
                            return
                
            await asyncio.sleep(retry_interval)
        else: # worker not alive
            if self.pending_terminations > 0:
                self.pending_terminations -= 1

    @abstractmethod
    def _submit_result(self, job_id, payload):
        pass


# TODO: backtest: talvez adicionar file FAIL se o job falhar, porque eu preciso enviar results pro server e tem q sinalizar de alguma forma q deu falha.