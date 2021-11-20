"""
This module is not meant to be complete
Just some convenience functions
"""

import aiohttp


async def post(**kwargs):
    return await request("POST", **kwargs)


async def put(**kwargs):
    return await request("PATCH", **kwargs)


async def patch(**kwargs):
    return await request("PATCH", **kwargs)


async def delete(**kwargs):
    return await request("DELETE", **kwargs)


async def get(**kwargs):
    return await request("GET", **kwargs)


async def request(method, timeout=2, **kwargs):
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=client_timeout) as session:
        async with aiohttp.request(method, **kwargs) as response:
            if response.headers["content-type"] == "application/json":
                response.payload = await response.json()
            return response
