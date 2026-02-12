import asyncio
from grpc import aio
import logging
import userService_pb2
import userService_pb2_grpc

async def addUser(username, host, port):
    async with aio.insecure_channel(f'{host}:{port}') as channel:
        stub = userService_pb2_grpc.UserManagerStub(channel)
        await stub.AddUser(userService_pb2.UserRequest(username=username))
    print('Added user to whitelist')

async def removeUser(username, host, port):
    async with aio.insecure_channel(f'{host}:{port}') as channel:
        stub = userService_pb2_grpc.UserManagerStub(channel)
        await stub.RemoveUser(userService_pb2.UserRequest(username=username))
    print('Removed user from whitelist')

async def main():
    while True:
        name = input(">").strip()
        command = input("Command(add/rem)?:").lower().strip()

        if command == 'add':
            await addUser(name, 'localhost', 5051)
        else:
            await removeUser(name, 'localhost', 5051)

if __name__ == '__main__':
    logging.basicConfig()
    asyncio.run(main())
