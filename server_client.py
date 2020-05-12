# TODO these should be set by args
VENT_PULL_PORT = 5556
VENT_PUSH_PORT = 5557
VENT_SINK_PUSH_PORT = 5558
SINK_PUB_PORT = 5559


Message = namedtuple('Message', ['id', 'content'])


class ServiceVentilator(threading.Thread):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(f'__main__')
        self.pull_port = VENT_PULL_PORT
        self.push_port = VENT_PUSH_PORT
        self.sink_push_port = VENT_SINK_PUSH_PORT
        self.processes = []
        self.num_worker = 10
        self.is_ready = threading.Event()

    def __enter__(self):
        self.start()
        self.is_ready.wait()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.logger.info('shutting down...')
        self._send_close_signal()
        self.is_ready.clear()
        self.join()
    
    def _send_close_signal(self):
        pass

    def run(self):
        self.context = zmq.Context()

        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.bind(f'tcp://*:{self.pull_port}')

        # Socket to send messages on
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.bind(f'tcp://*:{self.push_port}')

        self.sink = self.context.socket(zmq.PUSH)
        vent_sink_addr = f'tcp://localhost:{self.sink_push_port}'
        self.sink.connect(vent_sink_addr)
   
        # start the sink process
        self.logger.info('start the sink')
        proc_sink = ServiceSink(vent_sink_addr)
        self.processes.append(proc_sink)
        proc_sink.start()

        # The first message is '0' and signals start of batch
        self.sink.send_pyobj('0')

        # start workers
        for worker_id in range(self.num_worker):
            worker = ServiceWorker(worker_id, self.push_port, self.sink_push_port)
            self.processes.append(worker)
            worker.start()

        for p in self.processes:
            # wait until all worker init finish, prevent from confliction
            p.is_ready.wait()

        self.is_ready.set()
        self.logger.info('all set, ready to serve request!')
        
        job_info = OrderedDict()
        while True:
            self.logger.info(f'Ventilator wait...')
            request = self.receiver.recv_pyobj()
            client_id, msg = request

            # client_id is byte type
            client_id = client_id.decode('ascii')

            req_id, (Date, Time, stock_datas, kwargs) = msg
            self.logger.info(f'new request\treq id: {req_id}\tsize: {len(stock_datas)}\t client: {client_id}')

            job_info['client_id'] = client_id
            job_info['req_id'] = req_id
            self._task_vent(job_info, Date, Time, stock_datas, **kwargs)
        
        for p in self.processes:
            p.close()
        self.logger.info('terminated')

    def _task_vent(self, job_info, Date, Time, stock_datas, **kwargs):
        data_size = len(stock_datas)
        codes = list(stock_datas.keys())

        num_worker = self.num_worker
        count_per_worker = max(data_size // num_worker, 1)
        count_per_worker = min(data_size, count_per_worker)

        # print(f'worker count {num_worker}, count_per_worker {count_per_worker}')
        
        task_ids, sub_datas = [], []
        for start_idx in range(0, data_size, count_per_worker):
            end_idx = min(start_idx + count_per_worker, data_size)
            sub_codes = codes[start_idx: end_idx]
            sub_stock_datas = {}
            for code in sub_codes:
                sub_stock_datas[code] = stock_datas[code]
            
            task_ids.append(start_idx)
            sub_datas.append((Date, Time, sub_stock_datas, kwargs))
        
        job_info['num_task'] = len(task_ids)
        for task_id, sub_data in zip(task_ids, sub_datas):
            job_info['task_id'] = task_id
            self.sender.send_pyobj(Message(json.dumps(job_info), sub_data))


class ServiceWorker(multiprocessing.Process):
    def __init__(self, wid, pull_port, push_port):
        super().__init__()
        # self.logger = logging.getLogger(f'Worker-{wid}')
        self.logger = logging.getLogger('__main__')
        self.work_id = wid
        self.pull_port = pull_port
        self.push_port = push_port
        self.is_ready = multiprocessing.Event()

    def close(self):
        self.logger.info('shutting down...')
        self.join()
        self.context.destroy()
        self.logger.info('terminated!')

    def run(self):
        # self.pid = self.pid()

        self.context = zmq.Context()
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.connect(f'tcp://localhost:{self.pull_port}')
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.connect(f'tcp://localhost:{self.push_port}')
        self.service = Service()

        self.is_ready.set()
        self.logger.info(f'worker {self.work_id} ready!')
        while True:
            msg = self.receiver.recv_pyobj()
            content_id = msg.id
            Date, Time, stock_datas, kwargs = msg.content
            # self.logger.info(f'proc {1}: receive content id {content_id}, data size {len(stock_datas)}')
            ret = self.service.serving(Date, Time, stock_datas, **kwargs)
            self.sender.send_pyobj(Message(msg.id, ret))


class ServiceSink(multiprocessing.Process):
    def __init__(self, vent_sink_addr):
        super().__init__()
        self.logger = logging.getLogger('__main__')
        self.pull_port = VENT_SINK_PUSH_PORT
        self.pub_port = SINK_PUB_PORT
        self.vent_sink_addr = vent_sink_addr
        self.is_ready = multiprocessing.Event()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.logger.info('shutting down...')
        self._send_close_signal()
        self.is_ready.clear()
        self.join()
    
    def run(self):
        # self.pid = self.pid()

        self.context = zmq.Context()
        self.receiver = self.context.socket(zmq.PULL)
        receiver_addr = f'tcp://*:{self.pull_port}'
        self.receiver.bind(receiver_addr)

        # Wait for start of batch
        s = self.receiver.recv_pyobj()

        self.sender = self.context.socket(zmq.PUB)
        self.sender.bind(f'tcp://*:{self.pub_port}')

        self.is_ready.set()

        pending_jobs = defaultdict(lambda: SinkJob())
        while True:
            msg = self.receiver.recv_pyobj()
            job_info = json.loads(msg.id)
            job_id = f"{job_info['client_id']}#{job_info['req_id']}"
            pending_jobs[job_id].add_result(job_info['num_task'], msg.content)
            self.logger.info(f'sink: job info{msg.id}')

            # check if there are finished jobs, then send it back to workers
            finished = [(k, v) for k, v in pending_jobs.items() if v.is_done()]
            for job_id, job in finished:
                results = job.results
                self.sender.send_multipart([job_info['client_id'].encode('ascii'), pickle.dumps(Message(job_info['req_id'], results))])
                job.clear()
                pending_jobs.pop(job_id)


class SinkJob:
    def __init__(self):
        self.finished_task = 0
        self.results = {'model_results': {}, 'indicator_signals': {}}

    def is_done(self):
        return self.num_task == self.finished_task

    def add_result(self, num_task, result):
        self.num_task = num_task
        self.results['model_results'].update(result['model_results'])
        self.results['indicator_signals'].update(result['indicator_signals'])
        self.finished_task += 1

    def clear(self):
        self.results.clear()



Message = namedtuple('Message', ['id', 'content'])


from serving import VENT_PULL_PORT, SINK_PUB_PORT
from serving import *

class T0ModelClient:
    def __init__(self, ip='localhost', port=VENT_PULL_PORT, port_out=SINK_PUB_PORT, identity=None, **kwargs):
        self.logger = logging.getLogger('__main__')
        self.server_ip = ip
        self.server_port = port
        self.identity = identity or str(uuid.uuid4()).encode('ascii')
        self.context = zmq.Context()
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.setsockopt(zmq.LINGER, 0)
        # print(f'tcp://{self.server_ip}:{self.server_port}')
        # self.sender.connect(f'tcp://localhost:5556')
        self.sender.connect(f'tcp://{self.server_ip}:{self.server_port}')

        self.reciever = self.context.socket(zmq.SUB)
        self.reciever.setsockopt(zmq.LINGER, 0)
        self.reciever.setsockopt(zmq.SUBSCRIBE, self.identity)
        self.reciever.connect(f'tcp://{self.server_ip}:{port_out}')

        self.request_id = 0
        self.pending_request = set()
        self.pending_response = {}
    
    def serving(self, Date, Time, stock_datas, **kwargs):
        self.logger.info(f'client serving size {len(stock_datas)}')
        req_id = self._send((Date, Time, stock_datas, kwargs))
        res = self._recv(req_id)
        return res.content

    def _send(self, msg):
        self.request_id += 1
        tracker = self.sender.send_pyobj((self.identity, Message(self.request_id, msg)), copy=False, track=True)
        # tracker = self.sender.send_pyobj(0, copy=False, track=True)
        # import pdb; pdb.set_trace()
        # while tracker.pending:
        #     self.logger.info(f'sending...')
        #     time.sleep(1)

        self.logger.info('send done')
        self.pending_request.add(self.request_id)
        return self.request_id

    def _recv(self, wait_for_req_id):
        # TODO future should only need req_id 
        # client_req_id = f'{self.identity}#{wait_for_req_id}'
        client_req_id = wait_for_req_id
        try:
            while True:
                if client_req_id in self.pending_response:
                    response_content = self.pending_response.pop(client_req_id)
                    return Message(client_req_id, response_content)
                # response = self.reciever.recv_pyobj()
                response = self.reciever.recv_multipart()
                response = pickle.loads(response[-1])


                if client_req_id == response.id:
                    self.pending_request.remove(wait_for_req_id)
                    return Message(wait_for_req_id, response.content)
                elif client_req_id != response.id:
                    self.pending_response[response.id] = response.content
                    # wait for the next response

        except Exception as e:
            raise e
        finally:
            if wait_for_req_id in self.pending_request:
                self.pending_request.remove(wait_for_req_id)

    def close(self):
        self.sender.close()
        self.reciever.close()
        self.context.terminated()
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()