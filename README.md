# furca

Simple pre-fork library, ideas based on gunicorn minus the WSGI.

## Example

```python3
from os import getpid
from functools import partial

from asyncio import start_server
from furca import Manager, AsyncWorker, TCPResource

# Define a resource
server_socket = TCPResource((None, 5000))

async def work(resources):
	socket = resources[server_socket]
	server = await start_server(got_client, sock=socket, start_serving=False)
	print(f'worker {getpid()}: listening on {", ".join(str(s.getsockname()) for s in server.sockets)}...')
	await server.serve_forever()

async def got_client(reader, writer):
	print(f'worker {getpid()}: got connection from {writer.get_extra_info("peername")}!')
	writer.write(b'hello world!\n')
	writer.close()
	await writer.wait_closed()
	
# if your worker function is blocking, use furca.SyncWorker
worker = partial(AsyncWorker, work)

# pass count=None to use as many workers as detected CPU cores
with Manager(worker, count=None, resources=[server_socket]) as m:
	m.run()
```

## License

[WTFPL](./LICENSE).
