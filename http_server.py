from parse import parse
import asyncio
from workforce import WorkForce, OrganisedWorker


class HTTPRequest:
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
        return '\n'.join([f'{self.method} {self.path} {self.version}',
                          str(self.headers), self.payload])


class Server(WorkForce):

    def __init__(self, threads=1):
        super().__init__(threads)

    def get_worker(self, path):
        w = self.workers
        path = path.split('/')
        for p in path[1:]:
            try:
                w = w[p]
            except KeyError:
                w = w['_']
        return w['/']

    def worker(self, name):
        def wrapper(cls):
            print('Loading Worker: ', name)
            path = name.split('/')
            w = self.workers
            for p in path[1:]:
                x = '_' if p.startswith('{') else p
                w[x] = {}
                w = w[x]
            w['/'] = cls(name)
            return cls
        return wrapper

    async def handle_request(self, reader, writer):
        data = bytearray()
        while 1:
            part = await reader.read(255)
            data.extend(part)
            if len(part) < 255:
                break

        message = data.decode('latin1').rstrip()
        addr = writer.get_extra_info('peername')
        print(f"Received {message!r} from {addr!r}")

        request = HTTPRequest.parse(data.decode())
        request.writer = writer

        def callback(task, wf, coro):
            print('Close Connection')
            writer.close()

        try:
            self.schedule_workflow(request.path, request, callback=callback)
        except self.WorkerNotFound:
            print('Response with 404 Not Found')
            writer.close()

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

@server.worker(name='/')
class Root(OrganisedWorker):
    def start_workflow(self, request, loop=None, **kwargs):
        try:
            func = getattr(self, request.method.lower())
            p = parse(self.name, request.path)
            self.run_coro_async(
                func(request, **dict(p.named.items())),
                loop=loop, **kwargs
            )
        except AttributeError:
            print('Method not supported')

    async def get(self, request):
        response = b"""
HTTP/1.1 200 OK
Connection: closed
Content-Type: text/html; charset=utf-8
Content-Length: 26

<p>Made with WorkForce</p>
        """
        print(f'get: {request}')
        request.writer.write(response)
        await request.writer.drain()


@server.worker(name='/company/{c_id}/employee/{e_id}')
class Employee(Root):
    async def get(self, request, c_id, e_id):
        response = b"""
HTTP/1.1 200 OK
Connection: closed
Content-Type: text/html; charset=utf-8
Content-Length: 26

<p>Made with WorkForce</p>
        """
        print(f'get: {request}')
        request.writer.write(response)
        await request.writer.drain()

    async def post(self, request):
        print(f'post: {request}')

    async def put(self, request):
        print(f'put: {request}')

    async def patch(self, request):
        print(f'patch: {request}')

    async def delete(self, request):
        print(f'delete: {request}')

server.start()
