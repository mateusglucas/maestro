from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
from sqlalchemy import create_engine, event, String, JSON, DateTime, ForeignKey
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
import tomllib
from enum import StrEnum, auto
from datetime import datetime, timedelta
from typing import Any
import uuid
from abc import ABC
from abc import abstractmethod

class JobStatus(str, StrEnum):
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
        self._init_db()

    def _init_config(self):
        with open("server_config.toml", "rb") as f:
            self.config = tomllib.load(f)

    def _init_db(self):
        database_url = self.config["database"]["url"]

        self.db_engine = create_engine(
            database_url,
            connect_args={
                "check_same_thread": False,
                "timeout": 30,
            }
        )

        @event.listens_for(self.db_engine, "connect")
        def do_connect(dbapi_connection, connection_record):
            # Desativa o gerenciamento implícito do driver nativo do Python
            dbapi_connection.isolation_level = None

        @event.listens_for(self.db_engine, "begin")
        def do_begin(conn):
            # Emite manualmente o comando IMMEDIATE na raiz da transação
            conn.exec_driver_sql("BEGIN IMMEDIATE")

        self.db_sessionmaker = sessionmaker(bind=self.db_engine)

        Base.metadata.create_all(self.db_engine)

    def register_agent(self, hostname):
        with self.db_sessionmaker.begin() as session:
            now = datetime.now()
            id = str(uuid.uuid4())
            
            agent = Agent()

            agent.id = id
            agent.hostname = hostname
            agent.first_seen = now
            agent.last_seen = now

            session.add(agent)

            return {'agent_id': id}

    def heartbeat(self, agent_id):
        with self.db_sessionmaker.begin() as session:           
            agent = (
                session.query(Agent)
                .filter(Agent.id == agent_id)
                .first()
            )

            if agent is None:
                raise HTTPException(status_code=404, detail="Unknown agent")

            agent.last_seen = datetime.now()

    def submit_result(self, agent_id, job_id, payload):
        with self.db_sessionmaker.begin() as session:
            job = (
                session.query(Job)
                .filter(Job.id == job_id)
                .first()
            )

            if job is None:
                raise HTTPException(status_code=404, detail="Job not found")

            if job.assigned_agent_id != agent_id:
                raise HTTPException(status_code=409, detail="Job not assigned to agent")

            if job.status != JobStatus.RUNNING:
                raise HTTPException(status_code=409, detail=f"Job status is not RUNNING ({job.status})")

            job.finished_at = datetime.now()
            job.status = JobStatus.DONE

            self._submit_result(agent_id, job_id, payload)
        
    @abstractmethod
    def _submit_result(self, agent_id, job_id, payload):
        pass

    def request_job(self, agent_id):
        now = datetime.now()

        with self.db_sessionmaker.begin() as session:
            job = (
                session.query(Job)
                .filter(Job.status == JobStatus.PENDING)
                .first()
            )

            if job is None:
                # search for any stalled job
                timeout = self.config['heartbeat']['timeout']
                cutoff = now - timedelta(seconds=timeout)

                job = (
                    session.query(Job)
                    .join(Agent)
                    .filter(Job.status == JobStatus.RUNNING,
                            Agent.last_seen < cutoff)
                    .first()
                )

                if job is None:
                    return

            job.assigned_agent_id = agent_id
            job.started_at = now
            job.status = JobStatus.RUNNING

            return {'job_id': job.id, 'payload': job.payload}

    @abstractmethod
    def _validate_job(self, job_id, payload):
        pass

    def add_job(self, job_id, payload):
        self._validate_job(job_id, payload)

        try:
            with self.db_sessionmaker.begin() as session:
                job = Job()

                job.id = job_id
                job.payload = payload
                job.status = JobStatus.PENDING
                job.created_at = datetime.now()

                session.add(job)
        except IntegrityError:
            raise HTTPException(status_code=409, detail= "Job already exists")

def create_app(api: Api):
    app = FastAPI()



    class RegisterAgentRequest(BaseModel):
        hostname: str

    @app.post("/register_agent")
    def register_agent(req: RegisterAgentRequest):
        return api.register_agent(req.hostname)



    @app.post("/heartbeat/{agent_id}")
    def heartbeat(agent_id: str):
        return api.heartbeat(agent_id)



    @app.post("/request_job/{agent_id}")
    def request_job(agent_id: str):
        api.heartbeat(agent_id)

        return api.request_job(agent_id)



    @app.post("/submit_result/{agent_id}/{job_id}")
    def submit_result(agent_id: str, job_id: str, payload: bytes = Body(media_type="application/octet-stream")):
        api.heartbeat(agent_id)

        return api.submit_result(agent_id, job_id, payload)



    class AddJobRequest(BaseModel):
        job_id: str
        payload: dict[str, Any]

    @app.post("/add_job")
    def add_job(req: AddJobRequest):
        return api.add_job(req.job_id, req.payload)

    return app