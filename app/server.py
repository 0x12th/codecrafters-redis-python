import asyncio
from dataclasses import dataclass, field
from time import time
from typing import Dict, List, Optional


@dataclass
class Cache:
    storage: Dict[bytes, bytes] = field(default_factory=dict)
    expiry: Dict[bytes, float] = field(default_factory=dict)

    async def get(self, k: bytes) -> bytes:
        if k not in self.storage:
            return b"$-1\r\n"
        exp = self.expiry.get(k)
        if exp and time() > exp:
            del self.storage[k], self.expiry[k]
            return b"$-1\r\n"
        else:
            v = self.storage[k]
            return b"$%d\r\n%s\r\n" % (len(v), v)

    async def set(self, k: bytes, v: bytes):
        self.storage[k] = v

    async def set_px(self, k: bytes, exp: int):
        self.expiry[k] = time() + (exp / 1000.0)


async def handle_error(r: asyncio.StreamReader):
    return await r.readuntil(separator=b"\r\n")


async def handle_simple_string(r: asyncio.StreamReader):
    return await r.readuntil(separator=b"\r\n")


async def handle_integer(r: asyncio.StreamReader):
    return int(await r.readuntil(separator=b"\r\n"))


async def handle_bulk_string(r: asyncio.StreamReader) -> bytes:
    length = int((await r.readuntil(separator=b"\r\n"))[:2])
    value = await r.read(length)
    await r.read(2)
    return value


async def handle_array(r: asyncio.StreamReader) -> Optional[List[bytes]]:
    if not await r.read(1):
        return None
    num_elements = int((await r.readuntil(separator=b"\r\n"))[:2])
    return [await HANDLERS[await r.read(1)](r) for _ in range(num_elements)]


async def handle_ping(w, args: bytes):
    try:
        w.write(b"".join((b"+", b"PONG", b"\r\n")))
    except ValueError:
        print(f"PING has wrong arguments: {args}")


async def handle_echo(w, args: List[bytes]):
    try:
        w.write(b"".join((b"+", args[0], b"\r\n")))
    except ValueError:
        print(f"ECHO has wrong arguments: {args}")


async def handle_get(w, args: List[bytes]):
    try:
        w.write(await CACHE.get(args[0]))
    except ValueError:
        print(f"GET has wrong arguments: {args}")


async def handle_set(w, args: List[bytes]):
    args_len = len(args)
    if args_len < 2:
        raise ValueError(f"SET has wrong arguments: {args}")
    k, v = args[:2]
    if args_len == 2:
        expiry = None
    elif args_len == 4:
        if args[2].upper() != b"PX":
            raise ValueError(f"SET has wrong arguments: {args}")
        expiry = int(args[3])
    else:
        raise ValueError(f"SET has wrong arguments: {args}")
    await CACHE.set(k, v)
    if expiry:
        await CACHE.set_px(k, expiry)
    w.write(b"+OK\r\n")


HANDLERS = {
    b"+": handle_simple_string,
    b"-": handle_error,
    b":": handle_integer,
    b"$": handle_bulk_string,
    b"*": handle_array,
}

COMMANDS = {
    b"PING": handle_ping,
    b"ECHO": handle_echo,
    b"GET": handle_get,
    b"SET": handle_set,
}

CACHE = Cache()


async def handle_connection(r: asyncio.StreamReader, w: asyncio.StreamWriter):
    while True:
        data = await handle_array(r)
        if not data:
            break
        try:
            command_handler = COMMANDS[data[0].upper()]
        except KeyError:
            print(f"Unknown command: {data[0]}")
            break
        await command_handler(w, data[1:])


async def main():
    server = await asyncio.start_server(handle_connection, host="localhost", port=6379)
    async with server:
        await server.serve_forever()
    server.close()


if __name__ == "__main__":
    asyncio.run(main())
