import json
import asyncio
import traceback
from parse import parse
from urllib.parse import parse_qs, parse_qsl, urlparse
from workforce_async import WorkForce, Worker


class HTTPRequest:
    class ParsePayloadError(Exception):
        pass

    def __init__(self, method, path, query, version, headers, payload,
                 writer=None):
        self.method = method
        self.path = path
        self.query = query
        self.version = version
        self.headers = headers
        self.payload = payload
        self.writer = writer

    @staticmethod
    def parse_payload(content_type, payload) -> dict:
        try:
            func = {
                'application/x-www-form-urlencoded': lambda x: dict(parse_qsl(x)),
                'application/json': json.loads,
            }[content_type]
        except KeyError:
            raise HTTPRequest.ParsePayloadError('Content Type not supported')
        return func(payload)

    @classmethod
    def parse(cls, raw):
        i = 1
        lines = raw.splitlines()
        p = parse('{method} {url} {version}', lines[0])
        method = p['method']
        url = urlparse(p['url'])
        path = url.path if url.path == '/' else url.path.rstrip('/')
        query = parse_qs(url.query)
        version = p['version']
        headers = {}

        while lines[i]:
            key, value = lines[i].split(': ')
            headers[key] = value
            i += 1
        payload = ''.join(lines[i:])

        if payload:
            payload = HTTPRequest.parse_payload(
                headers.get('Content-Type'), payload
            )

        return cls(method, path, query, version, headers, payload)

    def __str__(self):
        return '\n'.join([f'{self.method} {self.path} {self.version}',
                          str(self.headers), str(self.payload)])


async def respond(request, payload, status):
    codes_map = {
        200: 'OK',
        201: 'Created',
        204: 'Deleted',
        400: 'Bad Request',
        404: 'Not Found',
        500: 'Internal Server Error'
    }
    response = f"""
HTTP/1.1 {status} {codes_map[status]}
Referrer-Policy: no-referrer
Connection: close
Content-Type: text/html; charset=UTF-8
Content-Length: {len(payload)}

{payload}
    """

    request.writer.write(response.encode())
    request.writer.write_eof()
    await request.writer.drain()
    print(f'{request.method} {request.path} {status}')


class Server(WorkForce):
    worker_name_delimiter = '/'

    def get_worker(self, request):
        return super().get_worker(request.path)

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

        try:
            # TODO: Processing should not be done here. Should be in a separate thread
            request = HTTPRequest.parse(data.decode())
        except Exception:
            traceback.print_exc()
            writer.close()
            await writer.wait_closed()
            return

        request.writer = writer
        request.server = self

        try:
            self.schedule_workflow(request)
        except self.WorkerNotFound:
            await respond(request, '<strong>Path not Found</strong>', 404)
            writer.close()
            await writer.wait_closed()

    async def run(self):
        server = await asyncio.start_server(
            self.handle_request, '127.0.0.1', 8888
        )
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')
        async with server:
            await server.serve_forever()

    def start(self):
        try:
            asyncio.run(self.run())
        except KeyboardInterrupt:
            print("Shutting down server")


class Endpoint(Worker):
    async def handle_workitem(self, request, *args, **kwargs):
        try:
            func = getattr(self, request.method.lower())
            p = parse(self.name, request.path)

        except AttributeError:
            payload, status = '<strong>Method not supported</strong>', 404
        else:
            try:
                payload, status = await func(request, **dict(p.named.items()))
            except Exception as e:
                traceback.print_exc()
                payload, status = f'<strong>Something went wrong</strong><p>{e}</p>', 500

        await respond(request, payload, status)
        request.writer.close()


server = Server()


@server.worker(name='/')
class Root(Endpoint):
    async def get(self, request):
        workers_name = [w.name for w in request.server.workers_list]
        workers_list = "</li><li>".join(sorted(workers_name))
        return (f"""
                <h2>Made with WorkForce</h2>
                <p>{request}</p>
                <ul><li>{workers_list}</li></ul>
                """), 200


@server.worker(name='/company/{c_id}/employee')
class Employee(Endpoint):
    async def post(self, request, c_id):
        print(request)
        return 'Created Employee', 201


@server.worker(name='/company/{c_id}/employee/{e_id}')
class EmployeeDetail(Endpoint):
    async def get(self, request, c_id, e_id):
        return (f'<p>Company {c_id} has employee {e_id}</p>'
                f'<p>URL query: {request.query}</p>'), 200

    async def put(self, request, c_id, e_id):
        print(request)
        return 'Updated Employee', 200

    async def patch(self, request, c_id, e_id):
        print(request)
        return 'Partially updated Employee', 200

    async def delete(self, request, c_id, e_id):
        print(request)
        return 'Deleted Employee', 204


server.start()
