import socket
import helpers as tools
import sys
sys.path.insert(0, '..')
from fs import datanode_fs as fs


class Datanode:
    
    def __init__(self ,my_port=18801):
        """
        nn_ip: ip of Namenode
        """
        self.namenode = socket.socket()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('', my_port))
        self.sock.listen()

    
    def connect_to_server(self, ip='localhost', port=8801):
        self.namenode.connect((ip,port))
        while True:
            data = self.namenode.recv(1500)
            if data:
                print('recv: ', data)
                self.handle(data)
            else:
                print('Client disconnected')
                break
        
    def handle(self, query):
        query = query.decode().split(' ')
        op = query[0]
        input_path = query[1] if len(query) > 1 else None
        client_ip = query[2] if len(query) > 2 else None
        replica_ip = query[3] if len(query) > 3 else None
        replica_port = int(query[4]) if len(query) > 4 else None
        print('op: ', op)
        print('path1: ', input_path)
        print('path2: ', client_ip)
        print('replicaIP: ', replica_ip)
        print('replicaPORT: ', replica_port)
        if op == 'WRITE_REPL':
            print('Going to recv file {}'.format(input_path))
            source_dn, source_addr = self.sock.accept()
            tools.recv_file(source_dn, fs.GetLocalPath(input_path) + str('REPL'))

            # send response to peer
            source_dn = socket.socket()
            source_dn.connect((source_addr[0], int(replica_port)))
            source_dn.send(b'SUCCESS')
        if op == 'WRITE':
            # download file from client
            client = socket.socket()
            client.connect((client_ip, 7777))
            tools.recv_file(client, fs.GetLocalPath(input_path))

            # send file to replica
            replica = socket.socket()
            replica.connect((replica_ip, replica_port))
            tools.send_file(replica, fs.GetLocalPath(input_path))

            # get response from replica
            replica, addr = self.sock.accept()
            result = replica.recv(10)
            print(result)

            # send response to client
            client = socket.socket()
            client.connect((client_ip, 7777))
            client.send(result)
        if op == 'WRITE_ALONE':
            # download file from client
            client = socket.socket()
            client.connect((client_ip, 7777))
            tools.recv_file(client, fs.GetLocalPath(input_path))
            client = socket.socket()
            client.connect((client_ip, 7777))
            client.send(b'SUCCESS')
        if op == 'WRITE_EXISTING_REPL':
            # send file to replica
            replica = socket.socket()
            replica.connect((replica_ip, replica_port))
            tools.send_file(replica, fs.GetLocalPath(input_path))

            # get response from replica
            replica, addr = self.sock.accept()
            result = replica.recv(10)
            print(result)
        if op == 'READ':
            client = socket.socket()
            client.connect((client_ip, 7777))
            tools.send_file(client, input_path)
        if op == 'REMOVE':
            if input_path == '/':
                fs.Initialize()
            pass # rm
        if op == 'COPY':
            pass # cp input output
        if op == 'MOVE':
            pass # cp input output
        if op == 'CREATE':
            fs.FileCreate(input_path)
        if op == 'PING':
            print('PING come')
            self.namenode.send(b'ALIVE')

                
        
if __name__ == '__main__':
    d = Datanode()

    d.connect_to_server()
