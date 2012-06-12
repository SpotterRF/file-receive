#!/usr/bin/env python

# Import 'with' statement for Python 2.5 and earlier. 'with' and 'as' are
# keywords in Python 2.6.
from __future__ import with_statement
from contextlib import closing

import os
import sys
import SocketServer
import tarfile
import uuid
import time
import re

# Some globals:
tmpDir          = '/opt/tmp/'  # MUST END IN /
tmpFilePrefix   = 'recvr-'
targetDir       = '/opt/tiles/'  # ALSO MUST END IN /

class TCPUploadReceive(SocketServer.StreamRequestHandler):

    def handle(self):
        tempFile    = tmpDir + tmpFilePrefix + uuid.uuid4().hex
        done        = False
        f           = open(tempFile, 'w')
        http        = False
        bufferSize  = 4096
        recvLength   = 0
        index       = 0
        expectedLength = 0

        log(tempFile + " opened for writing.")

        # str.format was not included until Python 2.6. The % operator in
        # earlier versions has a similar function to str.format.
        if sys.version_info < (2, 6):
            log('Receiving file from %s...' % (self.client_address[0]))
        else:
            log("Receiving file from {}...".format(self.client_address[0]))

        def finishHttp(good):
            log("Finishing HTTP connection")
            if good:
                self.wfile.write('HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            else:
                self.wfile.write('{ "success": false, "message": "Bad mime type." }')
                log("Bad mime type detected on HTTP POST. Killing transfer.")
            self.request.close()

        while not done:

            bufferData = self.request.recv(bufferSize) #.strip()

            if index == 0 and checkHttp(bufferData):
                log("on first chunk and found HTTP header")
                http = True
                matches = re.search('([\w\W]+?)(\r\n\r\n|\r\r|\n\n){1}([\w\W]*)', bufferData, re.M)
                headers = matches.group(1) 
                headers = headers.split('\r\n')
                print(headers)
                body = matches.group(3)
                for header in headers:
                    if re.search('^expect.*', header, re.I):
                        self.wfile.write('HTTP/1.1 100 Continue\r\n\r\n')
                    if re.search('^Content-Length', header, re.I):
                        log('found content length header')
                        expectedLength = int(re.search('^content-length:\s+(.*)', header, re.I).group(1))
                f.write(body)
                recvLength += len(body)
                index += 1
                continue

            log("%i/%i" % (recvLength, expectedLength))
            
            if recvLength < expectedLength:
                recvLength += len(bufferData)
                f.write(bufferData)
                if http and recvLength == expectedLength:
                    log("starting the end")
                    finishHttp(True)
                    break
            else:
                log('We have hit the done.')
                done = True

            index += 1
        f.close()

        unTarFile(tempFile, targetDir)
        rmFile(tempFile)

        self.wfile.write('{ "success": true }')
        log("Finished!\n")

            

def unTarFile(tarPath, target):
    log("Extracting...")
    with closing(tarfile.open(tarPath, 'r|*')) as tarball:
        tarball.extractall(target)

def rmFile(path):
    log("Removing temp file...")
    os.unlink(path)

def log(message):
    logTime = '%H:%M:%S%%%y-%m-%d - '
    sys.stdout.write(time.strftime(logTime))
    sys.stdout.write(message)
    sys.stdout.write('\n')
    sys.stdout.flush()


def checkHttp(bufferData):
    headMatch = '^POST.*HTTP.*'
    contMatch = "Content-type: application\/(x-gzip|x-tar|x-bz2|x-bzip|x-bzip2)"

    if re.search(headMatch, bufferData, re.I) and re.search(contMatch, bufferData, re.I | re.M):
        return True

    return False

if __name__ == "__main__":
    # Some environment fixing, before we begin:
    if not os.path.exists(tmpDir):
        os.makedirs(tmpDir)
    if not os.path.exists(targetDir):
        os.makedirs(targetDir)

    HOST, PORT = "0.0.0.0", 9999

    # Create the server, binding to localhost on port 9999
    server = SocketServer.TCPServer((HOST, PORT), TCPUploadReceive)
    log("Receiver now listening on port " + str(PORT))
    server.serve_forever()