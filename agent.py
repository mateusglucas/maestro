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
from multiprocessing import Process, Queue
import uuid
from abc import ABC, abstractmethod

from enum import StrEnum, auto

class Event(StrEnum):
    START_JOB = auto()
    DONE_JOB = auto()
    FAILED_JOB = auto()
    SHUTDOWN_WORKER = auto()

class Agent(ABC):
    def __init__(self, work_fn):
        self._init_config()

        self.work_fn = work_fn
        self.jobs_queue = Queue()
        self.events_queue = Queue()

        self.workers = {}
        
        self._spawn_workers()

    @abstractmethod
    def _validate_job(self, job_id, payload):
        pass

    @abstractmethod
    def _submit_result(self, job_id, payload):
        pass

    def run(self):
        # TODO
        pass

    def _init_config(self):
        with open("agent_config.toml", "rb") as f:
            self.config = tomllib.load(f)

    def _spawn_workers(self):
        for _ in range(self.config['jobs']['n_workers']):
            worker_id = str(uuid.uuid4())

            process = Process(
                target = type(self)._worker_main,
                args = (worker_id, self.work_fn, self.jobs_queue, self.events_queue),
            )
            process.start()

            self.workers[worker_id] = process

    @staticmethod
    def _worker_main(worker_id, work_fn, jobs_queue, events_queue):
        while True:
            job = jobs_queue.get()

            if job is None:
                events_queue.put({
                    'worker_id': worker_id,
                    'type': Event.SHUTDOWN_WORKER,
                })

                return
            
            events_queue.put({
                'worker_id': worker_id,
                'type': Event.START_JOB,
                'job_id': job['job_id'],
            })

            try:
                result = work_fn(job['payload'])

                events_queue.put({
                    'worker_id': worker_id,
                    'type': Event.DONE_JOB,
                    'job_id': job['job_id'],
                    'result': result,
                })

            except Exception as e:
                events_queue.put({
                    'worker_id': worker_id,
                    'type': Event.FAILED_JOB,
                    'job_id': job['job_id'],
                    'error': str(e),
                })