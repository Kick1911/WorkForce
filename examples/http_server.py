import asyncio
from parse import parse
from workforce_async import WorkForce, Worker


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
        path = p['path'] if p['path'] == '/' else p['path'].rstrip('/')
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


async def respond(request, payload, status):
    codes_map = {
        200: 'OK',
        201: 'Created',
        400: 'Bad Request',
        404: 'Not Found',
        500: 'Internal Server Error'
    }
    response = f"""
HTTP/1.1 {status} {codes_map[status]}
Referrer-Policy: no-referrer
Content-Type: text/html; charset=UTF-8
Content-Length: {len(payload)}

{payload}
    """

    request.writer.write(response.encode())
    await request.writer.drain()
    print(f'{request.method} {request.path} {status}')


class Server(WorkForce):

    # Override
    def get_worker(self, request):
        w = self.workers
        path = request.path.split('/')
        for p in path[1:]:
            try:
                w = w[p]
            except KeyError:
                w = w['_']
        return w['/']

    # Override
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
        # print(f"Received {message!r} from {addr!r}")

        request = HTTPRequest.parse(data.decode())
        request.writer = writer

        try:
            self.schedule_workflow(request)
        except self.WorkerNotFound:
            await respond(request, '<strong>Path not Found</strong>', 404)
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
class Root(Worker):
    async def response_handling(self, request):
        try:
            func = getattr(self, request.method.lower())
            p = parse(self.name, request.path)

            payload, status = await func(request, **dict(p.named.items()))
        except AttributeError:
            payload, status = '<strong>Method not supported</strong>', 404
        except Exception:
            payload, status = '<strong>Something went wrong</strong>', 500
        await respond(request, payload, status)
        request.writer.close()

    # Override
    def start_workflow(self, request, **kwargs):
        async def wrapper():
            await self.response_handling(request)

        self.run_coro_async(
            wrapper(),
            **kwargs
        )

    async def get(self, request):
        return (f"""
                <h2>Made with WorkForce</h2>
                <p>{request}</p>
                """), 200


@server.worker(name='/company/{c_id}/employee/{e_id}')
class Employee(Root):
    async def get(self, request, c_id, e_id):
        return f'<p>Company {c_id} has employee {e_id}</p>', 200

    async def post(self, request):
        print(f'post: {request}')

    async def put(self, request):
        print(f'put: {request}')

    async def patch(self, request):
        print(f'patch: {request}')

    async def delete(self, request):
        print(f'delete: {request}')

server.start()
