import socket, os, time

def send_file(s, filepath):
	f = open (filepath, "rb")  
	filesize = os.path.getsize(filepath)
	stat = lambda itr, filesize: int(itr * 1028 / filesize * 100)
	# print('Size is: ', filesize)
	counter = 0
	l = f.read(1024)
	prev_pecent = 0
	while (l):
	    s.send(l)
	    counter += 1
	    progress = stat(counter, filesize)
	    progress = progress if progress<100 else 100
	    if progress != prev_pecent:
	      # print(str(progress) + '%', end=' ')
	      prev_pecent = progress
	    l = f.read(1024)
	f.close()
	s.close()



def recv_file(sock, filepath):
	while True:
		data = sock.recv(1024)
		if data:
			with open(filepath, 'ab') as f:
				f.write(data)
		else:
			sock.close()
			break
