import time
import sys
import struct
import uuid
import socket
import telnetlib

READ_TIMEOUT_SEC = 3

# Convert specified bytes in list to 2-byte int, assuming bytes are in
# network order
def bytesToShort(bytes, offset) :

    if (len(bytes) - offset < 2) :
        raise Exception("Not enough bytes to convert to long")

    buf = ''
    buf = buf.join(bytes[offset:offset+2])

    ### print "buf: " + buf
    result = struct.unpack('>h', buf)
    return result[0]

# Convert specified bytes in list to 4-byte int, assuming bytes are in
# network order
def bytesToInt(bytes, offset) :

    if (len(bytes) - offset < 4) :
        raise Exception("Not enough bytes to convert to long")

    buf = ''
    buf = buf.join(bytes[offset:offset+4])

    ### print "buf: " + buf
    result = struct.unpack('>i', buf)
    return result[0]


# Convert specified bytes in list to 8-byte long, assuming bytes are in
# network order
def bytesToLong(bytes, offset) :
    
    if (len(bytes) - offset < 8) :
        raise Exception("Not enough bytes to convert to long")

    result = 0
    for i in range(8) :
        shifter = ord(bytes[i+offset]) & 0x00000000000000FFL
        result += (shifter << (7 - i) * 8)

    return result
    


class Puck:

    # Standard baud rates
    bauds = [9600, 19200, 38400, 57600, 1200, 2400, 4800, 115200, 230400]

    def __init__(self, host, port, digiLine=1, username='root', password='dbps'):
        print "__init__"
        self.digiHost = host
        self.digiPort = port
        self.digiLine = digiLine
        self.digiUser = username
        self.digiPassword = password
        socket.setdefaulttimeout(2)
        print "create socket"
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        timeout = socket.getdefaulttimeout()
        print "default socket timeout: " + str(timeout)
        print 'connect socket'
        self.sock.connect((host, port))
        print 'got socket connection'
        self.flushInput()

        self.setPuckMode(2)
        self.setOffset(0)
        data = []
        n = self.read(data, 96)
        print "data: " + str(data)
        print "data type: " + str(type(data))
        print "got " + str(n) + " bytes for datasheet"
        print "len(data)=" + str(len(data))

        buf = ""
        for x in data[:16]:
            buf += "%02x" % ord(x)

        print "buf: " + buf + ", len: " + str(len(buf))
        uid = uuid.UUID(buf)
        print "uuid: " + str(uid)
        name = ''
        print "name: " + name.join(data[32:])
        print "datasheet ver: " + str(bytesToShort(data, 16))
        datasheetSize = bytesToShort(data, 18);
        print "datasheet size: " + str(datasheetSize)
        print "mfctr ID: " + str(bytesToInt(data, 20))
        print "mfctr model: " + str(bytesToShort(data, 24))
        print "mfctr ver: " + str(bytesToShort(data, 26))
        print "serial number: " + str(bytesToInt(data, 28))
        print "payload size: " + str(self.size() - datasheetSize)


    # End PUCK write session; no other write() calls should be attempted
    # following flush() until new write session is initiated with erase
    def flush(self):
        self.sock.writeall("PUCKFM\r")
        resp = self.readLine(timeout=READ_TIMEOUT_SEC)

    # Set PUCK memory pointer
    def setOffset(self, offset):
        print "setOffset()"
        cmd = "PUCKSA " + str(offset) + "\r"
        self.sock.sendall(cmd)
        self.readLine(timeout=READ_TIMEOUT_SEC)

    # Return current PUCK memory pointer 
    def getOffset(self):
        print "getOffset()"

        self.sock.sendall("PUCKGA\r")
        val = self.readLine(timeout=READ_TIMEOUT_SEC)
        print "getOffset() - val: " + val
        # Eat PUCKRDY prompt
        dummy = self.readLine(timeout=READ_TIMEOUT_SEC)
        return int(val)

    # Return PUCK storage size in bytes, including datasheet
    def size(self):
        print "size()"
        self.flushInput()
        self.sock.sendall("PUCKSZ\r")
        val = self.readLine(timeout=READ_TIMEOUT_SEC)
        print "size()=" + val
        # Eat PUCKRDY prompt
        dummy = self.readLine(timeout=READ_TIMEOUT_SEC)
        return int(val)


    # Put device into PUCK mode
    def setPuckMode(self, triesPerBaud):
        print "setPuckMode()"
        for baud in self.bauds:
            print "Set baud to ", baud

            self.setDigiBaud(baud)

            for j in range(triesPerBaud):
                # KLUDGE: send a few newlines, as some instruments apparently
                # need these to wake from low-power mode
                for k in range(3):
                    self.sock.sendall("\r")

                # Send PUCK soft break
                self.sock.sendall("@@@@@@")
                time.sleep(0.75)
                self.sock.sendall("!!!!!!")
                time.sleep(0.5)

                # Try to get PUCK prompt
                if self.getPuckPrompt(2):
                    return

        raise Exception("Couldn't get to PUCK mode")


    # Put device into instrument mode
    def setInstrumentMode(self):
        self.sock.sendall("PUCKIM\r")


    # Get to 'PUCKRDY' prompt
    def getPuckPrompt(self, maxTries):
        print "getPuckPrompt()"

        # First flush the input
        self.flushInput()

        for i in range(maxTries):
            self.sock.sendall("PUCK\r")

            try:
                resp = self.readLine(timeout=READ_TIMEOUT_SEC)
                print "getPuckPrompt() - resp: " + resp
                if resp.startswith('PUCKRDY') :
                    print "Got PUCK prompt"
                    return True
                else:
                    print "Didn't get PUCK prompt"
            except:
                print 'getPuckPrompt() timed out'
                pass

        return False


    # Read requested number of bytes, starting at current PUCK pointer
    # offset. Returns number of bytes actually read
    def read(self, payload, nRequested):
        print "read()"
        # If number of requested bytes exceeds what is actually available
        # given current pointer offset, then downsize the request
        nRequested = min(nRequested, self.size() - self.getOffset())
        # Flush input
        self.flushInput()

        nTotalRead = 0
        nRemaining = nRequested
        while nRemaining > 0 :
            # Ask device for as many bytes as possible
            nGet = min(1024, nRemaining)
            cmd = "PUCKRM " + str(nGet) + "\r"
            self.sock.sendall(cmd)

            print("Look for response start, marked by open bracket '['")
            nGot = 0
            found = False
            while nGot < 100 :
                c = self.sock.recv(1, socket.MSG_WAITALL)
                nGot += 1
                print "nGot=" + str(nGot) + ", c=" + c
                if c == '[' :
                    found = True
                    break

            if found == False :
                raise Exception("PUCK read could not find data start")

            print("Read data from device")
            nGot = 0
            while nGot < nGet :

                # Bytes are available
                bytes = self.sock.recv(nGet-nGot)
                nGot += len(bytes)
                nTotalRead += len(bytes)
                print "read(): len(bytes)=" + str(len(bytes))
                payload += bytes;
                print "read(): len(payload)=" + str(len(payload))

            nRemaining -= nGot

        # Eat PUCKRDY prompt
        dummy = self.readLine()

        return nTotalRead


    def flushInput(self):
        print 'flushInput()'
        try:
            self.sock.recv(4096)
        except:
            print "flushInput(): timed out"

    def setDigiBaud(self, baud):
        """
        Log into Digi via telnet and set port baud rate
        """
        tn = telnetlib.Telnet(self.digiHost)
        tn.read_until("login: ")
        tn.write(self.digiUser + "\n")
        tn.read_until("password: ")
        tn.write(self.digiPassword + "\n");
        cmd = "set line range=" + str(self.digiLine) + " baud=" + str(baud) + "\n"
        print "setDigiBaud() - cmd: " + cmd

        tn.write(cmd)
        tn.write("show line\n")
        tn.write("exit\n")

        print "telnet response: " + tn.read_all()

        tn.close()


    def readLine(self, maxsize=None, timeout=1):
        """ maxsize is ignored, timeout in seconds for complete line """
        eol = '\r'
        line = ''
        start = time.clock()
        print 'readLine()'
        while 1:

            c = self.sock.recv(1)
            if not isascii(c):
                continue

            # Append next byte from input
            line += c

            pos = line.find(eol)

            if pos >= 0:
                print 'readLine() - done: %r' % line
                return line


            now = time.clock()

            if now - start > timeout:
                print 'readLine() timed out'
                raise Exception('readLine() timed out')

        return line


def isascii(c):
    o = ord(c)
    return (0x20 <= o <= 0x7e or c== '\n' or c == '\r')


class Datasheet :
    def __init__(self, bytes):
        self.bytes = bytes


    def toString() :
        pass





    
if __name__ == "__main__":
    import digipuck
    puck=digipuck.Puck("134.89.11.173", 2001)

