import os
import shutil
import socket
import sys
import argparse
import time
parser = argparse.ArgumentParser()
parser.add_argument("ip")
parser.add_argument("port")
parser.add_argument("name_ip")
parser.add_argument("name_port")
args = parser.parse_args()

out = "/var/storage"
TCP_IP = args.ip
TCP_PORT = int(args.port)

name_ip = args.name_ip
name_port = int(args.name_port)
BUFFER_SIZE = 1024


def initialization():
    if not os.path.exists(out):
        os.makedirs(out)
    else:
        shutil.rmtree(out)
        os.makedirs(out)


def read_file(path, s1):
    f = open(path, 'rb')
    l = f.read(1024)

    if (l == b''):
        s1.send(b'0')
    else:
        s1.send(b'1')
        time.sleep(2)
        while (l):
            s1.send(l)
            l = f.read(1024)
        # s1.send(b'')
        f.close()


def delete_file(path):
    os.remove(path)


def copy_file(path1, path2):
    return shutil.copyfile(path1, path2)


def move_file(path1, path2):
    return shutil.move(path1, path2)


def delete_directory(path):
    return shutil.rmtree(path)


def make_directory(path):
    return os.makedirs(path)


def create_file(path):
    return os.mknod(path)


def write(filename, conn, s1):
    with open(filename, 'wb') as handle:
        l = conn.recv(1024)

        while (l != b'0'):
            handle.write(l)
            l = conn.recv(1024)
            print(l)

        handle.close()


def command(command, s1, conn):
    data = command.decode().split(' ')
    if data[0] == "init":
        initialization()
    if data[0] == "create":
        create_file(data[1])
    elif data[0] == "copy":
        copy_file(data[1], data[2])
    elif data[0] == "move":
        move_file(data[1], data[2])
    elif data[0] == "delete":
        delete_file(data[1])
    elif data[0] == "makedir":
        make_directory(data[1])
    elif data[0] == "deletedir":
        delete_directory(data[1])
    elif data[0] == "read":  # !!!!!!
        read_file(data[1], s1)
    elif data[0] == "write":
        write(data[1], conn, s1)


def get_message():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(5)

    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s1.connect((name_ip, name_port))

    s1.send(bytes(str(TCP_PORT).encode()))

    conn, addr = s.accept()
    print('Connection address:', addr)
    while 1:
        try:
            data = conn.recv(BUFFER_SIZE)
            if not data or data == b'close':
                conn.close()
                s1.close()
                s.close()
                sys.exit("0")
            print("received data:", data)
            command(data, s1, conn)
        except SystemExit:
            break
        except:
            print("Something's wrong")
            continue
    conn.close()
    sys.exit("0")

if __name__ == '__main__':
    name = ""
    get_message()
