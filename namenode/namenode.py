import socket
from threading import Thread
import tqdm
import os
from time import sleep
import sys
import psycopg2
import pathlib
import subprocess
import time
import random
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("ip")
parser.add_argument("port")
parser.add_argument("client_port")

args = parser.parse_args()

IP = args.ip
PORT = int(args.port)
CLIENT_IP = args.ip
CLIENT_PORT = int(args.client_port)
BUFFER_SIZE = 1024
datanodes_number = 2
sockets = {}
conn = {}
datanodes = []
current_dir = "/"
storage = "/var/storage"
client_conn = None

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((IP, PORT))
sock.listen(5)
print('Listening..')


def handle_conn():
    while True:
        node_conn, addr = sock.accept()
        node_port = node_conn.recvfrom(1024)
        print("Node at {}: connected".format(addr))
        index = addr[0] + ":" + node_port[0].decode()
        datanodes.append(index)
        sockets[index] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sockets[index].connect((addr[0], int(node_port[0].decode())))
        conn[index] = node_conn


def check_nodes_activity():
    while True:
        for datanode in datanodes:
            node_addr = datanode.split(":")
            response = subprocess.getstatusoutput("ping -c 1 " + node_addr[0])
            if response[0] == 1:
                print("Node at {} stopped working, starting backup process".format(node_addr))
                backup(datanode)
        time.sleep(5)


def initialize_storage():
    for i in sockets.values():
        i.send(bytes("init", "utf-8"))
    make_query("DROP TABLE IF EXISTS filesdb;", is_return=False)
    make_query(
        "CREATE TABLE filesdb (filename Text, datanode1 TEXT, datanode2 TEXT, dir TEXT, is_dir BOOLEAN, size TEXT);",
        False)


def is_exists(filename):
    if current_dir != '/':
        path = current_dir + "/" + filename
    else:
        path = current_dir + filename
    is_exist_path(path)


def is_exist_path(filepath):
    length = len(make_query("SELECT * From filesdb where filename='{}'".format(filepath), True))
    if length == 0:
        return False
    return True


def get_file_ips(filepath):
    requested_file = make_query("SELECT * FROM filesdb WHERE filename='{}'".format(filepath), True)
    return requested_file[0][1], requested_file[0][2]


def get_ips():
    count = random.sample(datanodes, 2)
    return count


def mkdir(addr, path):
    sockets[addr].send(bytes("makedir " + storage + path, "utf-8"))
    client_conn.send("OK".encode())


def mkdir_current(new_path):
    if is_exists(new_path) != True:
        if current_dir != "/":
            path = current_dir + "/" + new_path
        else:
            path = current_dir + new_path
        make_query(
            "Insert into filesdb(filename, datanode1, datanode2, dir, is_dir, size) VALUES ('{0}','{1}','{2}','{3}', {4}, '-')".format(
                path, "_", "_", current_dir, True), False)
        for i in sockets.values():
            i.send(bytes("makedir " + storage + path, "utf-8"))
        client_conn.send("OK".encode())
    else:
        client_conn.send(("Already Exists:'{}'".format(new_path)).encode())


def create_file(filename):
    if is_exists(filename) != True:
        ips = get_ips()
        if current_dir != "/":
            sockets[ips[0]].send(bytes("create " + storage + current_dir + "/" + filename, "utf-8"))
            sockets[ips[1]].send(bytes("create " + storage + current_dir + "/" + filename, "utf-8"))
            make_query(
                "Insert into filesdb(filename, datanode1, datanode2, dir, is_dir, size) VALUES ('{0}','{1}','{2}','{3}', {4}, '0')"
                    .format(current_dir + "/" + filename, ips[0], ips[1], current_dir, False), False)
        else:
            sockets[ips[0]].send(bytes("create " + storage + current_dir + filename, "utf-8"))
            sockets[ips[1]].send(bytes("create " + storage + current_dir + filename, "utf-8"))
            make_query(
                "Insert into filesdb(filename, datanode1, datanode2, dir, is_dir, size) VALUES ('{0}','{1}','{2}','{3}', {4}, '0')"
                    .format(current_dir + filename, ips[0], ips[1], current_dir, False), False)
        client_conn.send("OK".encode())
    else:
        if current_dir != '/':
            path = current_dir + "/" + filename
        else:
            path = current_dir + filename
        client_conn.send(("Already exists:'{}'".format(path)).encode())


def read(filename):
    if is_exists(filename):
        if current_dir == "/":
            path = current_dir + filename
            ips = get_file_ips(path)
        else:
            path = current_dir + "/" + filename
            ips = get_file_ips(path)
        sock = sockets[ips[0]]
        node_conn = conn[ips[0]]
        sock.send(bytes("read " + storage + path, "utf-8"))
        pathlib.Path('/received_files' + path[:path.rfind("/") + 1]).mkdir(parents=True, exist_ok=True)
        with open('/received_files' + path, 'wb') as handle:
            if node_conn.recv(1024) == b'1':
                s = node_conn.recv(1024)
                handle.write(s)
                print("Ok")
                while (len(s) > 1024):
                    print("Receiving...")
                    s = node_conn.recv(1024)
                    handle.write(s)
                    print(s)
                handle.close()
            else:
                open('/received_files/' + filename, 'w+').close()
        client_conn.send("OK".encode())
    else:
        if current_dir != '/':
            path = current_dir + "/" + filename
        else:
            path = current_dir + filename
        client_conn.send(("Not found:'{}'".format(path)).encode())


def write(path, fs_path):
    ips = get_ips()
    if len(fs_path[:fs_path.rfind('/')]) == 0:
        make_query(
            "INSERT INTO filesdb(filename, datanode1, datanode, dir,is_dir, size) VALUES ('{}','{}','{}', '{}',{}, '{}')".
            format(fs_path, ips[0], ips[1], '/', False, os.path.getsize(path)), False)
    else:
        make_query(
            "INSERT INTO filesdb(filename, datanode1, datanode, dir,is_dir, size) VALUES ('{}','{}','{}', '{}',{}, '{}')".
            format(fs_path, ips[0], ips[1], fs_path[:fs_path.rfind('/')], False, os.path.getsize(path)), False)
    for i in ips:
        send_file(path, fs_path, i)
    client_conn.send("OK".encode())


def send_file(path, fs_path, addr):
    sock = sockets[addr]
    sock.send(bytes("write {}".format(storage + fs_path), "utf-8"))
    time.sleep(2)
    file1 = open(path, 'rb')
    content = file1.read(1024)
    while (content):
        print(content)
        sock.send(content)
        content = file1.read(1024)
    time.sleep(2)
    sock.send(b'0')
    file1.close()


def delete_dir(dirname):
    if is_exists(dirname):
        if current_dir != "/":
            path1 = current_dir + "/" + dirname  # /abc => /abc/1488
        else:
            path1 = current_dir + dirname  # / => /abc
        requested_dir = make_query("SELECT * FROM filesdb WHERE dir='{}'".format(path1), True)
        path = storage + path1

        if len(requested_dir) == 0:
            for i in sockets.values():
                i.send(bytes("deletedir " + path, "utf-8"))
                make_query("DELETE FROM filesdb where filename='{}'".format(path1), False)

        else:
            print("The folder is not empty. Are you sure you want to delete it? [y/n]\n>")
            answer = input()
            if answer == 'y':
                for i in sockets.values():
                    i.send(bytes("deletedir " + path, "utf-8"))
                make_query("DELETE FROM filesdb where dir='{}'".format(path1), False)
                make_query("DELETE FROM filesdb where filename='{}'".format(path1), False)
        client_conn.send("OK".encode())
    else:
        if current_dir != '/':
            path = current_dir + "/" + dirname
        else:
            path = current_dir + dirname
        client_conn.send(("Not found:'{}'".format(path)).encode())


def delete_file(filename):
    if is_exists(filename):
        if current_dir != "/":
            path1 = current_dir + "/" + filename
        else:
            path1 = current_dir + filename
        requested_file = make_query("SELECT * FROM filesdb WHERE filename='{}'".format(path1), True)
        path = storage + path1
        if len(requested_file) == 0:
            print("File doesn't exist")
            return
        else:
            make_query("DELETE FROM filesdb Where filename='{}'".format(path1), False)
            sockets[requested_file[0][1]].send(bytes("delete {}".format(path), "utf-8"))
            sockets[requested_file[0][2]].send(bytes("delete {}".format(path), "utf-8"))
        client_conn.send("OK".encode())
    else:
        if current_dir != '/':
            path = current_dir + "/" + filename
        else:
            path = current_dir + filename
        client_conn.send(("Not found:'{}'".format(path)).encode())


def ls():
    requested_files = make_query("SELECT * FROM filesdb WHERE dir = '{0}';".format(current_dir), True)
    response = ""
    for requested_file in requested_files:
        if requested_file[4]:
            response = response + "Directory {}".format(requested_file[0]) + "\n"
        else:
            response = response + "File {}".format(requested_file[0]) + "\n"
    response = "empty" if response == "" else response
    client_conn.send(response.encode())


def cd(path):
    global current_dir
    if path == "/" or is_exists(path):
        print(path)
        if path == "/":
            current_dir = "/"
        else:
            if current_dir == "/":
                current_dir = current_dir + path
            else:
                current_dir = current_dir + "/" + path  # /
        client_conn.send("OK".encode())
    else:
        client_conn.send(("Not found:'{}'".format(path)).encode())


def cp(src, dest):
    if is_exist_path(src) == True and is_exist_path(dest) == False:
        ips = get_file_ips(src)
        make_query(
            "Insert into filesdb(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
                .format(dest, ips[0], ips[1], dest[:dest.rfind('/')], False), False)
        sockets[ips[0]].send(bytes("copy {} {}".format(storage + src, storage + dest), "utf-8"))
        sockets[ips[1]].send(bytes("copy {} {}".format(storage + src, storage + dest), "utf-8"))
        client_conn.send("OK".encode())
    elif is_exist_path(src) == False:
        client_conn.send(("Not found:'{}'".format(src)).encode())
    else:
        client_conn.send(("Already Exists:'{}'".format(dest)).encode())


def mv(src, dest):
    if is_exist_path(src):
        ips = get_file_ips(src)
        make_query(
            "Insert into filesdb(filename, datanode1, datanode2, dir, is_dir) VALUES ('{0}','{1}','{2}','{3}', {4})"
                .format(dest, ips[0], ips[1], dest[:dest.rfind('/')], False), False)
        make_query("DELETE FROM filesdb Where filename='{}'".format(src), False)
        print(ips)
        sockets[ips[0]].send(bytes("move " + storage + src + " " + storage + dest, "utf-8"))
        sockets[ips[1]].send(bytes("move " + storage + src + " " + storage + dest, "utf-8"))
        client_conn.send("OK".encode())
    else:
        client_conn.send(("Not found:'{}'".format(src)).encode())


def make_query(query, is_return):
    conn = psycopg2.connect(dbname='postgres', user='postgres', password='postgres', host='localhost', port="5432")
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    if is_return:
        result = cursor.fetchall()
    cursor.close()
    conn.close()
    if is_return:
        return result


def backup(addr):
    backup_files = make_query(
        "SELECT * FROM filesdb Where (datanode1='{}' OR datanode2='{}') AND is_dir=FALSE;".format(addr, addr), True)
    backup_dir = '/backup_{}'.format(addr)
    for backup_file in backup_files:
        if backup_file[1] != addr:
            backup_read(backup_file[0], backup_file[1], backup_dir)  # TODO
        else:
            backup_read(backup_file[0], backup_file[2], backup_dir)
        backup_write(backup_file, backup_file[0], addr, backup_dir)
    print("Finished backup of node at {}".format(addr))
    datanodes.remove(addr)


def backup_read(filename, addr, backup_dir):
    sock = sockets[addr]
    node_conn = conn[addr]
    sock.send(bytes("read " + storage + filename, "utf-8"))
    pathlib.Path(backup_dir + filename[:filename.rfind("/") + 1]).mkdir(parents=True, exist_ok=True)
    with open(backup_dir + filename, 'wb') as handle:
        s = node_conn.recv(1024)
        if s == b'1':
            s = node_conn.recv(1024)
            handle.write(s)
            print("Ok")
            while (len(s) > 1024):
                print("Receiving...")
                s = node_conn.recv(1024)
                handle.write(s)
                print(s)
            handle.close()
        else:
            handle.close()


def backup_write(fileinf, filename, addr, backup_dir):
    if len(datanodes) <= datanodes_number:
        print("Cannot create replica. Could not find some datanodes")
    else:
        for datanode in datanodes:
            if datanode != fileinf[1] and datanode != fileinf[2]:
                send_file(backup_dir + filename, filename, datanode)
                if addr == fileinf[1]:
                    make_query("UPDATE filesdb set datanode1='{}'".format(datanode), False)
                else:
                    make_query("UPDATE filesdb set datanode='{}'".format(datanode), False)


def close():
    for i in datanodes:
        sockets[i].send(bytes("Closing..", "utf-8"))
        sockets[i].close()
        conn[i].close()
    sock.close()
    sock.detach()


if __name__ == "__main__":
    make_query(
        "CREATE TABLE IF NOT EXISTS filesdb (filename Text, datanode1 TEXT, datanode2 TEXT, dir TEXT, is_dir BOOLEAN, size TEXT);",
        False)

    thread1 = Thread(target=handle_conn)
    thread2 = Thread(target=check_nodes_activity)
    thread1.daemon = True
    thread2.daemon = True
    thread1.start()
    thread2.start()

    interval = 0
    while len(datanodes) < datanodes_number:
        print("Waiting for data servers to connect..")
        interval += 5
        interval = min(15, interval)
        time.sleep(interval)

    client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_sock.bind((CLIENT_IP, CLIENT_PORT))
    client_sock.listen(5)
    print('Listening..')
    client_conn, client_addr = client_sock.accept()
    while True:
        try:
            data = client_conn.recv(BUFFER_SIZE)
            command = data.decode().split(" ")
            if command[0] == "init":
                initialize_storage()
            elif command[0] == "cd":
                cd(command[1])
            elif command[0] == "ls":
                ls()
            elif command[0] == "mkdir":
                mkdir_current(command[1])
                ls()
            elif command[0] == 'read':
                read(command[1])
            elif command[0] == "rmdir":
                delete_dir(command[1])
                ls()
            elif command[0] == "close":
                close()
                sys.exit(0)
            elif command[0] == "rm":
                delete_file(command[1])
            elif command[0] == 'create_file':
                create_file(command[1])
            elif command[0] == 'mv':
                mv(command[1], command[2])
            elif command[0] == 'cp':
                cp(command[1], command[2])
            elif command[0] == 'write':
                write(command[1], command[2])
            else:
                client_conn.send((command[0] + ": Command not found").encode())
        except SystemExit:
            client_conn.send("Stop".encode())
            sys.exit(0)
        except KeyboardInterrupt:
            client_conn.send("Stop".encode())
            sys.exit(0)
        except:
            client_conn.send("Something went wrong".encode())
            continue
