# datastore-async

An asyncio client for Google Cloud Datastore, built on top of
[`google-cloud-datastore`](https://pypi.org/project/google-cloud-datastore/).

`AsyncClient` extends the official `Client`, so everything else (keys, entities,
`client.key()`, ...) works as usual. I/O methods have `*_async` variants, e.g.
`get_async`, `put_multi_async`, `delete_async`, `allocate_ids_async`.

> This is an unofficial project and is not affiliated with Google.

## Installation

```
pip install datastore-async
```

Requires Python 3.10+.

## Example

```python
import asyncio

import datastore_async


async def main():
    client = datastore_async.AsyncClient()

    key = client.key("Kind", 1)
    entity = datastore_async.Entity(key)
    entity["name"] = "example"
    await client.put_async(entity)

    print(await client.get_async(key))

    async for entity in client.query(kind="Kind").fetch():
        print(entity)

    # Batches and transactions are async context managers.
    async with client.transaction():
        await client.put_async(entity)


asyncio.run(main())
```

Batches and transactions are tracked per asyncio context, so concurrent tasks
do not see each other's active transaction.

## License

Apache License 2.0, see [LICENSE](LICENSE). Based on `google-cloud-datastore`,
Copyright Google LLC.
