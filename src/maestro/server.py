from fastapi import FastAPI, HTTPException, Form, File, UploadFile
from pydantic import BaseModel
from sqlalchemy import String, JSON, DateTime, ForeignKey, Enum, Boolean
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import tomllib
from datetime import datetime, timedelta
from typing import Any
import uuid
from contextlib import asynccontextmanager
import aiofiles
from pathlib import Path

from .common import JobResults, JobStatus

ARTIFACTS_DIR = Path('artifacts')

class Base(DeclarativeBase):
    pass

class Job(Base):
    __tablename__ = "jobs"

    id:                 Mapped[str]                     = mapped_column(String, primary_key=True)
    parameters:         Mapped[dict[str, Any]]          = mapped_column(JSON)
    assigned_agent_id:  Mapped[str | None]              = mapped_column(ForeignKey("agents.id"))
    created_at:         Mapped[datetime]                = mapped_column(DateTime)
    started_at:         Mapped[datetime | None]         = mapped_column(DateTime)
    finished_at:        Mapped[datetime | None]         = mapped_column(DateTime)
    status:             Mapped[JobStatus]               = mapped_column(Enum(JobStatus, native_enum=False))
    error:              Mapped[str| None]               = mapped_column(String)
    results:            Mapped[dict[str, Any] | None]   = mapped_column(JSON)
    artifact_present:   Mapped[bool | None]             = mapped_column(Boolean)

class Agent(Base):
    __tablename__ = "agents"

    id:         Mapped[str]         = mapped_column(String, primary_key=True)
    hostname:   Mapped[str]         = mapped_column(String)
    first_seen: Mapped[datetime]    = mapped_column(DateTime)
    last_seen:  Mapped[datetime]    = mapped_column(DateTime)

class Api:
    def __init__(self):
        self._init_config()
        self.start_time = None

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

    async def submit_result(self, agent_id, job_id, results: JobResults, artifact: UploadFile | None):
        await self._receive_artifact(artifact)

        async with self.db_sessionmaker.begin() as session:
            stmt = (
                update(Job)
                .where(Job.id == job_id,
                       Job.assigned_agent_id == agent_id,
                       Job.status == JobStatus.ASSIGNED)
                .values(finished_at = datetime.now(),
                        results = results.results,
                        error = results.error,
                        artifact_present = artifact is not None,
                        status = results.status)
                .returning(Job)
            )

            job = (await session.execute(stmt)).scalar_one_or_none()

            if job is None:
                raise HTTPException(status_code=404, detail="Invalid operation")

    async def _receive_artifact(self, artifact):
        if artifact is not None:
            ARTIFACTS_DIR.mkdir(
                parents=True,
                exist_ok=True,
            )

            artifact_path = ARTIFACTS_DIR / artifact.filename

            async with aiofiles.open(artifact_path, "wb") as f:
                while chunk := await artifact.read(1024 * 1024):
                    await f.write(chunk)

    async def request_job(self, agent_id):
        now = datetime.now()

        async with self.db_sessionmaker.begin() as session:
            # Prefer never-assigned jobs before reclaiming stalled jobs. This gives
            # expired agents the longest possible window to recover and submit results.

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
                        status = JobStatus.ASSIGNED)
                .returning(Job)
            )

            job = (await session.execute(stmt)).scalar_one_or_none()

            # Start looking for stalled jobs only after server being up for more than
            # timeout seconds, to give time to receive heartbeats of agents still alive
            timeout = timedelta(seconds=self.config['heartbeat']['timeout'])
            if job is None and self.start_time is not None and now-self.start_time > timeout:
                cutoff = now - timeout

                candidate = (
                    select(Job.id)
                    .join(Agent)
                    .where(Job.status == JobStatus.ASSIGNED,
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

            return {'job_id': job.id, 'parameters': job.parameters}

    async def add_job(self, parameters):
        async with self.db_sessionmaker.begin() as session:
            job = Job()

            job.id = str(uuid.uuid4())
            job.parameters = parameters
            job.status = JobStatus.PENDING
            job.created_at = datetime.now()

            session.add(job)

def create_app(api: Api):

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await api.init_database()

        api.start_time = datetime.now()

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
    async def submit_result(agent_id: str, job_id: str, results: str = Form(), artifact: UploadFile | None = File(None)) :
        results = JobResults.model_validate_json(results)
  
        await api.heartbeat(agent_id)

        return await api.submit_result(agent_id, job_id, results, artifact)



    class AddJobRequest(BaseModel):
        parameters: dict[str, Any]

    @app.post("/add_job")
    async def add_job(req: AddJobRequest):
        return await api.add_job(req.parameters)

    return app
