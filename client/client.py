import socket
import os
import argparse
import time
import sys

parser = argparse.ArgumentParser()
parser.add_argument("name_ip")
parser.add_argument("name_port")
args = parser.parse_args()

name_ip = args.name_ip
name_port = int(args.name_port)

class Client:
    def __init__(self):
        self.namenode = None

    def connect(self, ip='127.0.0.1', port=8800):
        self.namenode = socket.socket()
        self.namenode.connect((ip, int(port)))

    def __send_msg__(self, msg, recv_label="Status"):
        self.namenode.send(str.encode(msg))
        data = self.namenode.recv(1024).decode()
        print(f'{recv_label}: {data}')
        return data

    def init_cluster(self):
        self.__send_msg__('init')

    def mkdir(self, path):
        self.__send_msg__(f'mkdir {path}')

    def ls(self):
        self.__send_msg__(f'ls')

    def cd(self, path):
        self.__send_msg__(f'cd {path}')

    def rmdir(self, path):
        self.__send_msg__(f'rmdir {path}')

    def create_file(self, filepath):
        self.__send_msg__(f'create_file {filepath}')

    def upload(self, local_path, remote_path):
        data = self.__send_msg__(f"write {local_path} {remote_path}")
        if data.split(' ')[0] == 'ERROR':
            return

        send_file(self.namenode, local_path)
        result = self.namenode.recv(1024)
        print(result)

    def download(self, remote_path, local_path):
        data = self.__send_msg__(f"read {remote_path}")
        if data.split(' ')[0] == 'ERROR':
            return
        recv_file(self.namenode, local_path)

    def rm(self, path):
        self.__send_msg__(f'rm {path}')

    def describe_file(self, path):
        self.__send_msg__(f'info {path}')

    def cp(self, old_path, dest_path):
        self.__send_msg__(f'cp {old_path} {dest_path}')

    def mv(self, old_path, dest_path):
        self.__send_msg__(f'mv {old_path} {dest_path}')

    def exit(self):
        self.namenode.send('close'.encode())
        sys.exit(1)

    def close(self):
        self.namenode.close()
        sys.exit(1)

def send_file(sock, filepath):
    f = open(filepath, "rb")
    size = os.path.getsize(filepath)
    sock.send(str.encode(str(size)))
    time.sleep(2)
    l = f.read(1024)
    while (l):
        sock.send(l)
        l = f.read(1024)
    time.sleep(2)
    sock.send(b'0')
    f.close()


def recv_file(sock, filepath):
    with open(filepath, 'wb') as f:
        data = sock.recv(1024)
        while data != b'0':
            f.write(data)
            data = sock.recv(1024)


def opt(options):
    tokens = input("> ").split(' ')
    while tokens[0] != 'exit':
        option_eval(options, tokens)
        tokens = input("> ").split(' ')


def option_eval(options, tokens):
    if tokens[0] in options:
        param = options[tokens[0]]
        if len(tokens) != param[2]:
            print(param[1])
        else:
            if param[2] == 1:
                param[0]()
            if param[2] == 2:
                param[0](tokens[1])
            if param[2] == 3:
                param[0](tokens[1], tokens[2])
    else:
        print(f'No command: {tokens[0]}')




if __name__ == '__main__':

    c = Client()
    c.connect(args.name_ip, args.name_port)

    options = {
        'write': (c.upload, 'Usage: write /local_path /DFS_path', 3),
        'download': (c.download, 'Usage: download /DFS_path /local_path', 3),
        'rm': (c.rm, 'Usage: rm /DFS_path', 2),
        'info': (c.describe_file, 'Usage: info DFS_file', 2),
        'copy': (c.cp, 'Usage: copy /DFS_path /DFS_dest_path', 3),
        'ls': (c.ls, 'Usage: ls', 1),
        'mv': (c.mv, 'Usage: mv /DFS_path /DFS_dest_path', 3),
        'rmdir': (c.rmdir, 'Usage: rmdir /DFS_path', 2),
        'mkdir': (c.mkdir, 'Usage: mkdir /DFS_path', 2),
        'cd': (c.cd, 'Usage: cd DFS_folder', 2),
        'init': (c.init_cluster, 'Usage: init', 1),
        'create_file': (c.create_file, 'Usage: create_file DFS_filename', 2),
        'close':(c.close, "Usage: close", 1),
        'destroy':(c.exit, "", 1)
    }
    opt(options)

