import grpc 
from userService import UserService
import userService_pb2_grpc
import logging
import os
from concurrent import futures

logging.basicConfig(level=logging.INFO)
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    userService_pb2_grpc.add_UserManagerServicer_to_server(UserService(os.environ["WHITELIST_PATH"]), server)
    port = os.environ["USER_SERVICE_PORT"]
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()

