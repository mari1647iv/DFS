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

HOST = "localhost"
PORT = 8080
BUFFER_SIZE = 1024
datanodes_number = 2
sockets = {}
conn = {}
datanodes = []
current_dir = "/"
storage = "/var/storage"

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((HOST, PORT))
sock.listen(5)
print('Listening..')

def handle_conn():
    while True:
            node_conn, addr = sock.accept()
            node_port =  node_conn.recvfrom(1024)
            print("Node at {}: connected". format(addr))
            index = addr[0] + ":" + node_port[0].decode()
            datanodes.append(index)
            sockets[index] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sockets[index].connect((addr[0], int(node_port[0].decode())))
            conn[index] =  node_conn

def check_nodes_activity():
    while True:
        for datanode in datanodes:
            node_addr = datanode.split(":")
            response = subprocess.getstatusoutput("ping -c 1 " + node_addr[0])
            if response[0] == 1:
                print("Node at {} stopped working, starting backup process". format(node_addr))
                backup(datanode)
        time.sleep(5)

def initialize_storage():
    for i in sockets.values():
        i.send(bytes("Initialization..", "utf-8"))
    make_query("DROP TABLE IF EXISTS filesdb;", is_return=False)
    make_query("CREATE TABLE filesdb (filename Text, path TEXT, datanode1 TEXT, datanode2 TEXT, is_dir BOOLEAN, size TEXT);", False)

def is_exists(filename):
    if current_dir != '/':
        path = current_dir + "/" + filename
    else:
        path = current_dir + filename
    length = len(make_query("SELECT * From filesdb where filename='{}'". format(path), True))
    if length == 0:
        print("Not found:'{}'". format(path))
        return False
    print("Already Exists:'{}'". format(path))
    return True

def get_file_ips(filepath):
    requested_file = make_query("SELECT * FROM filesdb WHERE filename='{}'". format(filepath), True)
    return requested_file[0][1], requested_file[0][2]

def get_ips():
    count = random.sample(datanodes, 2)
    return count

def send_file(path, fs_path, addr):
    sock = sockets[addr]
    sock.send(bytes("write {}". format(storage + fs_path), "utf-8"))
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

def mkdir(path, dirname):
    pass

def create_file(filename):
    pass

def read(filename):
    pass

def write(filename, str):
    pass

def delete_dir(dirname):
    pass

def delete_file(filename):
    pass

def ls():
    pass

def cd(path):
    pass

def cp(src, dest):
    pass

def mv(src, dest):
    pass

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
    backup_files = make_query("SELECT * FROM filesdb Where (datanode1='{}' OR datanode2='{}') AND is_dir=FALSE;". format(addr, addr), True)
    backup_dir = '/backup_{}'. format(addr)
    for backup_file in backup_files:
        if backup_file[1] != addr:
            backup_read(backup_file[0], backup_file[1], backup_dir)  # TODO
        else:
            backup_read(backup_file[0], backup_file[2], backup_dir)
        backup_write(backup_file, backup_file[0], addr, backup_dir)
    print("Finished backup of node at {}". format(addr))
    datanodes.remove(addr)

def backup_read(filename, addr, backup_dir):
    sock = sockets[addr]
    node_conn = conn[addr]
    sock.send(bytes("read " + storage + filename, "utf-8"))
    pathlib.Path(backup_dir+filename[:filename.rfind("/")+1]).mkdir(parents=True, exist_ok=True)
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
                    make_query("UPDATE filesdb set datanode1='{}'". format(datanode), False)
                else:
                    make_query("UPDATE filesdb set datanode='{}'". format(datanode), False)
    

def close():
    for i in datanodes:
        sockets[i].send(bytes("Closing..", "utf-8"))
        sockets[i].close()
        conn[i].close()
    sock.close()
    sock.detach()


if __name__ == "__main__":
    initialize_storage()

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

    while True:
        try:
            print(current_dir + ">", end=" ")
            inpt = input()
            commands = inpt.split(" ")
            if commands[0] == "init":
                initialize_storage()
            elif commands[0] == "cd":
                cd(commands[1])
            elif commands[0] == "ls":
                ls()
            elif commands[0] == "mkdir":
                mkdir(commands[1], commands[2])
                ls()
            elif commands[0] == 'read':
                read(commands[1])
            elif commands[0] == "delete_dir":
                delete_dir(commands[1])
                ls()
            elif commands[0] == "close":
                close()
                sys.exit(0)
            elif commands[0] == "delete":
                delete_file(commands[1])
            elif commands[0] == 'create_file':
                create_file(commands[1])
            elif commands[0] == 'mv':
                mv(commands[1], commands[2])
            elif commands[0] == 'cp':
                cp(commands[1], commands[2])
            elif commands[0] == 'write':
                write(commands[1], commands[2])
            else:
                print(commands[0] + ": Command not found")
        except SystemExit:
            print("Stop")
            sys.exit(0)
        except KeyboardInterrupt:
            print("Stop")
            sys.exit(0)
        except:
            print("Something went wrong")
            continue


