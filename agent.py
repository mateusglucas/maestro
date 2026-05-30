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
import asyncio
import threading
from enum import StrEnum, auto
import time
import httpx
import socket

class Event(StrEnum):
    START_JOB = auto()
    DONE_JOB = auto()
    FAILED_JOB = auto()
    TERMINATE_WORKER = auto()

class Agent(ABC):
    def __init__(self, work_fn):
        self._init_config()

        self.work_fn = work_fn
        self.jobs_queue = Queue()
        self.events_queue = Queue()

        self.workers = {}
        self.pending_worker_terminations = 0

        self.id = None
        self.last_heartbeat = None
        self.client = httpx.AsyncClient()

        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def _init_config(self):
        with open("agent_config.toml", "rb") as f:
            self.config = tomllib.load(f)

    def _spawn_workers(self, n):
        for _ in range(n):
            worker_id = str(uuid.uuid4())

            process = Process(
                target = type(self)._worker_main,
                args = (worker_id, self.work_fn, self.jobs_queue, self.events_queue),
            )
            process.start()

            self.workers[worker_id] = process

    def _remove_dead_workers(self):
        for proc_id, proc in list(self.workers.items()):
            if not proc.is_alive():
                proc.join()
                proc.close()
                self.workers.pop(proc_id)

    def _terminate_workers(self, n):
        for _ in range(n):
            self.jobs_queue.put_nowait(None)

    @staticmethod
    def _worker_main(worker_id, work_fn, jobs_queue, events_queue):
        while True:
            job = jobs_queue.get()

            if job is None:
                events_queue.put({
                    'worker_id': worker_id,
                    'type': Event.TERMINATE_WORKER,
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

    @abstractmethod
    def _validate_job(self, job_id, payload):
        pass

    @abstractmethod
    def _submit_result(self, job_id, payload):
        pass

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)

        asyncio.create_task(self.hearbeat())
        asyncio.create_task(self.event_handler())

        self.loop.run_forever()

        # workers health, add/remove worker
        # hearbeat
        # process events, request work from server, send work to workers, get results from workers, send results to server, register to server

    async def hearbeat(self):
        interval = self.config['hearbeat']['interval']
        retry_interval = 5

        while True:
            time_to_sleep = min(retry_interval, interval) # worst scenario (retry)

            if self.last_heartbeat is not None and (delay:=self.last_heartbeat + interval - time.time()) > 0:
                time_to_sleep = delay
            elif self.id is not None:
                heartbeat_time = time.time()
                response = await self.client.post(f"{self.config['server']['url']}/hearbeat/{self.id}")
                if response.is_success:
                    self.last_hearbeat = heartbeat_time
                    time_to_sleep = interval - (time.time()-heartbeat_time) # update since we had success

            await asyncio.sleep(time_to_sleep)

    async def event_handler(self):
        while True:
            payload = {'hostname': socket.gethostname()}
            ret = await self.client.post(f"{self.config['server']['url']}/register_agent", json=payload)
            if ret.is_success:
                self.id = ret.json()['agent_id']
                if self.id is not None:
                    break

            await asyncio.sleep(5)

        pending_workers_termination = 0

        while True:
            # TODO: precisa adicionar lista de jobs pra controle, pra reencaminhar jobs dos
            # processos que foram mortos (terminados ja ta ok, o problema é os que foram mortos
            # de maneira não graciosa)
            # self._remove_dead_workers()

            workers_to_spawn = self.config['jobs']['n_workers'] - len(self.workers)
            workers_to_terminate = -workers_to_spawn - pending_workers_termination
            if workers_to_spawn > 0:
                self._spawn_workers(workers_to_spawn)
            elif workers_to_terminate > 0:
                self._terminate_workers(workers_to_terminate)
                pending_workers_termination += workers_to_terminate

            # TODO: melhorar esta lógica, vai ficar muito devagar pegar um, esperar sleep, pegar outro...
            if self.jobs_queue.empty():
                ret = await self.client.post(f"{self.config['server']['url']}/request_job/{self.id}")
                if ret.is_success:
                    self.last_hearbeat = time.time()
                    job = ret.json()
                    if job is not None:
                        self.jobs_queue.put_nowait(job)

            try:
                event = self.events_queue.get_nowait()

                if event['type'] == Event.DONE_JOB:
                    # TODO
                    pass
                elif event['type'] == Event.FAILED_JOB:
                    # TODO
                    pass
                elif event['type'] == Event.TERMINATE_WORKER:
                    proc = self.workers[event['worker_id']]
                    proc.join()
                    proc.close()
                    self.workers.pop(event['worker_id'])

                    pending_workers_termination -= 1
                    pass
                elif event['type'] == Event.START_JOB:
                    pass
            except asyncio.QueueEmpty:
                pass


            await asyncio.sleep(5)


# TODO: backtest: talvez adicionar file FAIL se o job falhar, porque eu preciso enviar results pro server e tem q sinalizar de alguma forma q deu falha.