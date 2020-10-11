import os
import shutil as shu


#root_path = '/var/storage' # The path to the local storage
root_path = './tmp'

node_id = 1 # Hardcoded node id (may be should be passed via command line arguments, idk)

log = True # Whether to print log information or not


def Initialize() -> int:
    """
    Removes all the files in the root directory
    ---
    Returns:
    - number of bytes that available for storage
    """
    if not os.path.exists(root_path): os.makedirs(root_path)
    for root, dirs, files in os.walk(root_path):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shu.rmtree(os.path.join(root, d)) 
    
    total, used, free = shu.disk_usage(root_path)
    
    if log:
        print('The node was initialized successfully.')

    return free


def FileCreate(filepath: str) -> int:
    """
    Creates empty file with given name in given directory
    ---
    Attributes:
    - filepath: DFS path to the file
    ---
    Returns:
    1 if OK, -1 if not OK
    """
    
    return FileWrite(filepath, None)

def FileRead(filepath: str):
    """
    Opens and reads the file 
    --- 
    Attributes:
    - filepath: DFS path to the file
    ---
    Returns:
    An array of bytes if OK, -1 if not OK
    """

    path = GetLocalPath(filepath)

    if os.path.exists(path):
        f = open(path, 'rb')
        file_data = f.read()
        return file_data
    else:
        print('The file', path, 'does not exists!')
        return -1

def FileWrite(filepath: str, file_data) -> int:
    """
    Creates a new file or updates existing one with given path, name, and data
    ---
    Attrubutes:
    - filepath: DFS path to the file
    - file_data: array of bytes transfered from client
    ---
    Returns:
    1 if OK, -1 if not OK
    """
    
    path = GetLocalPath(filepath)
    i = len(path) - 1
    filename = ''
    while path[i] != '/':
        filename += path[i]
        i -= 1
    path = path[:i+1]
    filename = filename[::-1]

    if os.path.isdir(path):
        f = open(path + filename, "w+")
        if file_data != None:
            f.write(file_data)
        f.close()

        if log:
            print('A new file {}{}was created.'.format(path, filename))
        return 1

    else:
        print('The directory', path, 'does not exists!')
        return -1
    
def FileDelete(filepath: str) -> int:
    """
    Deletes given file in given directory
    ---
    Attrubutes:
    - filepath: DFS path to the file
    ---
    Returns:
    1 if OK, -1 if not OK
    """
    if os.path.isdir(root_path + filepath):
    
        path = GetLocalPath(filepath)
            
        if os.path.exists(path):
            os.remove(path)
            if log:   
                print('The file', path, 'was removed.')
            return 1
        else:
            print('The file', path, 'does not exists!')
            return -1

    else:
        print('The directory', filepath, 'does not exists!')
        return -1

def FileInfo(filepath: str) -> str:
    """
    Returns information about given file in given directory
    ---
    Attrubutes:
    - filepath: DFS path to the file
    ---
    Returns:
    Info if OK, None if not OK
    """

    info = ''

    path = GetLocalPath(filepath)

    if os.path.exists(path):
        size = os.path.getsize(path)

        filename = path.split('/')[-1]

        info += 'Filename: ' + filename + '\n'
        info += 'Directory: ' + path + '\n'
        info += 'Size: ' + size + ' bytes\n'
        info += 'Node ID: ' + node_id + '\n'

        return info
    else:
        print('The file', path, 'does not exists!')
        return None

def FileCopy(filepath: str, dest: str) -> int:
    """
    Copies the given file in the given directory into given destination
    ---
    Attributes:
    - filepath: DFS path to the file
    - dest: to where the file should be copied
    ---
    Returns:
    1 if OK, -1 if not OK
    """

    filepath = GetLocalPath(filepath)
    dest = GetLocalPath(dest)

    if os.path.isdir(dest):
        if os.path.exists(filepath):
            shu.copy2(filepath, dest)
            if log:
                print('The file', filepath, 'was copied to', dest, '.')
            return 1
        else:
            print('The file', filepath, 'does not exists!')
            return -1
    else:
        print('The destination directory', dest, 'does not exists!')
        return -1

def FileMove(filepath: str, dest: str) -> int:
    """
    Moves given file to the given location
    ---
    Attributes:
    - filepath: DFS path to the file
    - dest: to where the file should be moved
    """
    
    filepath = GetLocalPath(filepath)
    dest = GetLocalPath(dest)

    if os.path.isdir(dest):
        if os.path.exists(filepath):

            # Collecting filename for the funciton move
            filename = ''
            i = len(filepath) - 1
            while True:
                if i == -1:
                    break
                if filepath[i] == '/':
                    break
                filename += filepath[i]
                i -= 1
            filename = filename[::-1]
            if dest[-1] != '/':
                dest = dest + '/'
            dest = dest + filename

            shu.move(filepath, dest)

            if log:
                print('The file', filepath, 'was moved to', dest, '.')
            return 1

        else:
            print('The file', filepath, 'does not exists!')
            return -1
    else:
        print('The directory', dest, 'does not exists!')
        return -1

def DirectoryMake(path: str) -> int:
    """
    Creates new directory in given path
    ---
    Attributes:
    ---
    Returns:
    1 if OK, -1 if not OK
    """

    path = GetLocalPath(path)

    if not os.path.exists(path):
        os.makedirs(path)
        if log:
            print('A new directory', path, ' was created.')
        return 1
    
    else:
        print('The given directory already exists!')
        return -1

def DirectoryDelete(path: str) -> int:
    """
    Deletes directory with any content in it
    """

    path = GetLocalPath(path)

    if os.path.exists(path):
        if log:
            shu.rmtree(path)
             
            print('The directory', path, ' was deleted.')
        return 1
    
    else:
        print('The directory', path, 'does not exists!')
        return -1

def GetLocalPath(dfs_path: str) -> str:
    """
    Returns DFS path converted to local path
    """
    
    if dfs_path[0] != '/':
        dfs_path = '/' + dfs_path

    return root_path + dfs_path

def main():
    
    Initialize()
    DirectoryMake('/dir1')
    DirectoryMake('/dir1/dir11')
    FileCreate('/dir1/dir11/file1.txt')
    FileMove('/dir1/dir11/file1.txt', '/')
    #DirectoryDelete('/dir1')

    return


if __name__ == '__main__':
    main()
