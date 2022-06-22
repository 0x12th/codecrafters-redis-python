import asyncio


async def handle_client(msg: bytes) -> str:
    r, w = await asyncio.open_connection(host="localhost", port=6379)

    print(f"Send: {msg!r}")
    w.write(msg)

    data = await r.read(1024)
    print(f"Received: {data.decode()!r}")

    w.close()

    return data.decode()
