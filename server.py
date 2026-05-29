from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
from sqlalchemy import create_engine, event, String, JSON, DateTime, ForeignKey
from sqlalchemy import select, update, insert, delete
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
import tomllib
from enum import StrEnum, auto
from datetime import datetime, timedelta
from typing import Any
import uuid
from abc import ABC
from abc import abstractmethod
from contextlib import asynccontextmanager

class JobStatus(StrEnum):
    PENDING = auto()
    RUNNING = auto()
    DONE = auto()
    CANCELLED = auto()
    FAILED = auto()

class Base(DeclarativeBase):
    pass

class Job(Base):
    __tablename__ = "jobs"

    id:                 Mapped[str]             = mapped_column(String, primary_key=True)
    payload:            Mapped[dict[str, Any]]  = mapped_column(JSON)
    assigned_agent_id:  Mapped[str | None]      = mapped_column(ForeignKey("agents.id"))
    created_at:         Mapped[datetime]        = mapped_column(DateTime)
    started_at:         Mapped[datetime | None] = mapped_column(DateTime)
    finished_at:        Mapped[datetime | None] = mapped_column(DateTime)
    status:             Mapped[str]             = mapped_column(String)

class Agent(Base):
    __tablename__ = "agents"

    id:         Mapped[str]         = mapped_column(String, primary_key=True)
    hostname:   Mapped[str]         = mapped_column(String)
    first_seen: Mapped[datetime]    = mapped_column(DateTime)
    last_seen:  Mapped[datetime]    = mapped_column(DateTime)

class Api(ABC):
    def __init__(self):
        self._init_config()

    def _init_config(self):
        with open("server_config.toml", "rb") as f:
            self.config = tomllib.load(f)

    async def init_database(self):
        database_url = self.config["database"]["url"]

        self.db_engine = create_async_engine(database_url)

        self.db_sessionmaker = async_sessionmaker(bind=self.db_engine, expire_on_commit=False)

        async with self.db_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def register_agent(self, hostname):
        async with self.db_sessionmaker.begin() as session:
            now = datetime.now()
            agent_id = str(uuid.uuid4())
            
            agent = Agent()

            agent.id = agent_id
            agent.hostname = hostname
            agent.first_seen = now
            agent.last_seen = now

            session.add(agent)

            return {'agent_id': agent_id}

    async def heartbeat(self, agent_id):
        async with self.db_sessionmaker.begin() as session:  
            stmt = (
                update(Agent)
                .where(Agent.id == agent_id)
                .values(last_seen=datetime.now())
                .returning(Agent)
            )     

            agent = (await session.execute(stmt)).scalar_one_or_none()

            if agent is None:
                raise HTTPException(status_code=404, detail="Unknown agent")

    async def submit_result(self, agent_id, job_id, payload):
        async with self.db_sessionmaker.begin() as session:
            stmt = (
                update(Job)
                .where(Job.id == job_id,
                       Job.assigned_agent_id == agent_id,
                       Job.status == JobStatus.RUNNING)
                .values(finished_at = datetime.now(),
                        status = JobStatus.Done)
                .returning(Job)
            )

            job = (await session.execute(stmt)).scalar_one_or_none()

            if job is None:
                raise HTTPException(status_code=404, detail="Invalid operation")

            await self._submit_result(agent_id, job_id, payload)
        
    @abstractmethod
    async def _submit_result(self, agent_id, job_id, payload):
        pass

    async def request_job(self, agent_id):
        now = datetime.now()

        async with self.db_sessionmaker.begin() as session:
            candidate = (
                select(Job.id)
                .where(Job.status == JobStatus.PENDING)
                .order_by(Job.created_at)
                .limit(1)
                .scalar_subquery()
            )

            stmt = (
                update(Job)
                .where(Job.id == candidate)
                .values(assigned_agent_id = agent_id,
                        started_at = now,
                        status = JobStatus.RUNNING)
                .returning(Job)
            )

            job = (await session.execute(stmt)).scalar_one_or_none()

            if job is None:
                # search for any stalled job
                timeout = self.config['heartbeat']['timeout']
                cutoff = now - timedelta(seconds=timeout)

                candidate = (
                    select(Job.id)
                    .join(Agent)
                    .where(Job.status == JobStatus.RUNNING,
                           Agent.last_seen < cutoff)
                    .order_by(Job.created_at)
                    .limit(1)
                    .scalar_subquery()
                )

                stmt = (
                    update(Job)
                    .where(Job.id == candidate)
                    .values(assigned_agent_id = agent_id,
                            started_at = now)
                    .returning(Job)
                )

                job = (await session.execute(stmt)).scalar_one_or_none()

                if job is None:
                    return

            return {'job_id': job.id, 'payload': job.payload}

    @abstractmethod
    async def _validate_job(self, job_id, payload):
        pass

    async def add_job(self, payload):
        await self._validate_job( payload)

        async with self.db_sessionmaker.begin() as session:
            job = Job()

            job.id = str(uuid.uuid4())
            job.payload = payload
            job.status = JobStatus.PENDING
            job.created_at = datetime.now()

            session.add(job)

def create_app(api: Api):

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await api.init_database()

        yield


    app = FastAPI(lifespan=lifespan)


    class RegisterAgentRequest(BaseModel):
        hostname: str

    @app.post("/register_agent")
    async def register_agent(req: RegisterAgentRequest):
        return await api.register_agent(req.hostname)



    @app.post("/heartbeat/{agent_id}")
    async def heartbeat(agent_id: str):
        return await api.heartbeat(agent_id)



    @app.post("/request_job/{agent_id}")
    async def request_job(agent_id: str):
        await api.heartbeat(agent_id)

        return await api.request_job(agent_id)



    @app.post("/submit_result/{agent_id}/{job_id}")
    async def submit_result(agent_id: str, job_id: str, payload: bytes = Body(media_type="application/octet-stream")):
        await api.heartbeat(agent_id)

        return await api.submit_result(agent_id, job_id, payload)



    class AddJobRequest(BaseModel):
        payload: dict[str, Any]

    @app.post("/add_job")
    async def add_job(req: AddJobRequest):
        return await api.add_job(req.payload)

    return app

class TestApi(Api):
    async def _submit_result(self, agent_id, job_id, payload):
        pass

    async def _validate_job(self, job_id, payload):
        pass

app = create_app(TestApi())