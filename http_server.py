from parse import parse
import asyncio
from workforce import WorkForce, OrganisedWorker


class HTTP:
    def __init__(self, method, path, version, headers, payload, writer=None):
        self.method = method
        self.path = path
        self.version = version
        self.headers = headers
        self.payload = payload
        self.writer = writer

    @classmethod
    def parse(cls, raw):
        i = 1
        lines = raw.splitlines()
        p = parse('{method} {path} {version}', lines[0])
        method = p['method']
        path = p['path']
        version = p['version']
        headers = {}

        while lines[i]:
            key, value = lines[i].split(': ')
            headers[key] = value
            i += 1
        payload = ''.join(lines[i:])

        return cls(method, path, version, headers, payload)

    def __str__(self):
        return '\n'.join([self.method, self.path, self.version,
                          self.headers, self.payload])


class Server(WorkForce):

    def __init__(self, threads=1):
        self.workers = {}
        super().__init__(threads)

    def get_worker(self, path):
        w = self.workers
        print('Workers:', w)
        path = path.split('/')
        for p in path[1:-1]:
            try:
                w = w[p]
            except KeyError:
                w = w['_']
        return w[path[-1]]

    def worker(self, name):
        def wrapper(cls):
            path = name.split('/')
            w = self.workers
            for p in path[1:-1]:
                x = '_' if p.startswith('{') else p
                w[x] = {}
                w = w[x]
            w[path[-1]] = cls()
            return cls
        return wrapper

    async def handle_request(self, reader, writer):
        data = bytes()
        while 1:
            part = await reader.read(255)
            if part:
                data += part
            else:
                break
        message = data.decode()
        addr = writer.get_extra_info('peername')
        print(f"Received {message!r} from {addr!r}")
        request = HTTP.parse(data.decode())
        request.writer = writer

        self.schedule_workflow(request.path, request)

    async def _run(self):
        server = await asyncio.start_server(
            self.handle_request, '127.0.0.1', 8888
        )
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')
        async with server:
            await server.serve_forever()

    def start(self):
        asyncio.run(self._run())


server = Server()

@server.worker(name='/company/{c_id}/employee')
class Endpoint(OrganisedWorker):
    def start_workflow(self, request):
        try:
            func = getattr(self, request.method.lower())
            self.run_coro_async(func(request))
        except AttributeError:
            print('Method not supported')

    async def get(self, request):
        print(f'get: {request}')
        request.writer.write(request.payload)
        await request.writer.drain()

    async def post(self, request):
        print(f'post: {request}')

    async def put(self, request):
        print(f'put: {request}')

    async def patch(self, request):
        print(f'patch: {request}')

    async def delete(self, request):
        print(f'delete: {request}')

