from app import client

import pytest


@pytest.mark.asyncio
async def test_handle_ping():
    msg = b"*1\r\n$4\r\nPING\r\n"
    answer = "+PONG\r\n"
    assert answer == await client.handle_client(msg)


@pytest.mark.asyncio
async def test_echo():
    msg = b"*2\r\n$4\r\nECHO\r\n$11\r\nwatermelons\r\n"
    answer = "+watermelons\r\n"
    assert answer == await client.handle_client(msg)


@pytest.mark.asyncio
async def test_set_get():
    set_msg = b"*3\r\n$3\r\nSET\r\n$5\r\nHello\r\n$5\r\nWorld\r\n"
    await client.handle_client(set_msg)
    get_msg = b"*2\r\n$3\r\nGET\r\n$5\r\nHello\r\n"
    set_answer = "+OK\r\n"
    get_answer = "$5\r\nWorld\r\n"
    assert set_answer == await client.handle_client(set_msg)
    assert get_answer == await client.handle_client(get_msg)


@pytest.mark.asyncio
async def test_wrong_set_get():
    set_msg = b"*3\r\n$3\r\nSET\r\n$4\r\nOpen\r\n$5\r\nClose\r\n"
    get_msg = b"*2\r\n$3\r\nGET\r\n$7\r\nNotOpen\r\n"
    set_answer = "+OK\r\n"
    get_answer = "$-1\r\n"
    assert set_answer == await client.handle_client(set_msg)
    assert get_answer == await client.handle_client(get_msg)


@pytest.mark.asyncio
async def test_set_px_get():
    set_msg = (
        b"*5\r\n$3\r\nSET\r\n$4\r\nSome\r\n$4\r\nBody\r\n$2\r\nPX\r\n$6\r\n100000\r\n"
    )
    get_msg = b"*2\r\n$3\r\nGET\r\n$4\r\nSome\r\n"
    set_answer = "+OK\r\n"
    get_answer = "$4\r\nBody\r\n"
    assert set_answer == await client.handle_client(set_msg)
    assert get_answer == await client.handle_client(get_msg)
