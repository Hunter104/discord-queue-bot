import grpc
import pwd
#import subprocess
from userService_pb2_grpc import UserManagerServicer
from userService_pb2 import UserResponse

class UserService(UserManagerServicer):
    """RPC Server that manages SSH/Unix Users
    """
    def __init__(self, whitelist_path):
        self.whitelist_path = whitelist_path

    @property
    def whitelist(self):
        with open(self.whitelist_path, 'r') as f:
            return f.readlines()

    @whitelist.setter
    def whitelist(self, whitelist):
        with open(self.whitelist_path, 'w') as f:
            f.write(('\n'.join(whitelist)).strip())

    @staticmethod
    def _checkUser(username):
        try:
            pwd.getpwnam(username)
            return True
        except Exception:
            return False
        

    def RemoveUser(self, request, context):
        """Removes user from whitelist and kicks them from active sessions
        """
        #subprocess.run(f"wall 'Usuário {request.username} está prestes a ser expulso de sua sessão'")
        #subprocess.run(f'pkill -u {request.username}')
        if not UserService._checkUser(request.username):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unknown user')
            return UserResponse()

        self.whitelist = [user for user in self.whitelist if not user.strip() == request.username]
        return UserResponse()

    def AddUser(self, request, context):
        """Add user to whitelist
        """
        if not UserService._checkUser(request.username):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unknown user')
            return UserResponse()
        self.whitelist = self.whitelist + [request.username]
        return UserResponse()
