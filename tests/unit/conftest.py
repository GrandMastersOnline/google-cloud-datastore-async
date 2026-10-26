import asyncio

import pytest


@pytest.fixture(autouse=True)
def _current_event_loop(request):
    # Sync tests construct real grpc.aio channels, which need a current loop
    # (Python < 3.14 raises otherwise). Async tests get their loop from pytest-asyncio.
    if asyncio.iscoroutinefunction(request.function):
        yield
        return

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield
    loop.close()
    asyncio.set_event_loop(None)
