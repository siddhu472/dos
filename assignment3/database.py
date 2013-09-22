'''
Created on May 4, 2013
@author: siddharthct and gouthamdl 
'''

import socket
import threading
import time 
from random import randint 

class database(object):
    
    def __init__(self,port,npigs):
        self.port = port ;  
        self.associatedthreads = [] ;   # To keep track of the open threads 
        self.filename = 'townrecord.txt' 
        self.pigids = [] ; 
        self.piglocations =[]; 
        self.pigstatus =[]; 
        self.npigs = npigs ; 
        self.associatedthreads = [] ;
        self.lockaccess = True 
        
    def mainloop(self):
        
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('localhost', self.port))
        s.listen(5)
        while True:
            try:
                peersock,peeraddr = s.accept()
                peersock.settimeout(None)
                # Create a thread whenever the db gets any connection
                # The thread will in turn call the handle peer function
                t = threading.Thread(target = self.handlePeer, args = [peersock])
                t.start()
            except KeyboardInterrupt:
                print 'KeyboardInterrupt: stopping mainloop'
                break
            except:
                print 'Error : connection with db not established '
                break
        s.close()
    def clearcontents(self):
        open(self.filename, 'w').close()
        self.pigids = [] ; 
        self.piglocations =[]; 
        self.pigstatus =[]; 
        
    # Writing content to the townrecord     
    def writeline(self,line):
   
        while(not self.lockaccess):
            time.sleep(0.1)

        self.lockaccess = False 
        data = line.split(',')
        pigid = int(data[0])
        pigposition = int(data[1])
        status = data[2]
        self.pigids.append(pigid); 
        self.piglocations.append(pigposition); 
        self.pigstatus.append(status);
        with open(self.filename, "a") as myfile:
            myfile.write(line)
        self.lockaccess = True 
     
    # Supporting look up operations
    def findlocation(self,pigid):
        for position, item in enumerate(self.pigids):
            if item == pigid:
                return self.piglocations[position],self.pigstatus[position]; 
    
    def findid(self,location):
        for position, item in enumerate(self.piglocations):
            if item == location:
                return self.pigids[position],self.pigstatus[position]
            
    
    def sendToSinglePeer(self,port,message):
        # Sends a message to the peer port. Threaded.
        # All messages are threaded and hence no blocking 
        t = threading.Thread(target = self.sendToPeer, args = [port,message])
        self.associatedthreads.append(t)
        t.start()
        
    def sendToPeer(self,port,message):
        # Send message to the specified port
        # We also introduce a random delay
        delay = randint(1,self.npigs) 
        time.sleep(0.1*delay)
        address = ('localhost',port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(address)
        sock.sendall(message)
        sock.close()
            
    
    def handlePeer(self,peerSock):
        
        host, port = peerSock.getpeername()
        data = peerSock.recv(1024)
        a = data.split(',')
        
        if a[0] =='1' : 
            # Co-ordinator sends the status of a pig as soon as it knows it 
            senderport = int(a[1])
            for i in range(2,len(a),3):
                line = a[i]+','+a[i+1]+','+a[i+2]+'\n'; 
                self.writeline(line)
            message = '11' 
            self.sendToPeer(senderport, message);
            
        
        elif a[0] =='2' :
            # Leader requesting for Pig info since the other leader is asleep
            port = int(a[1])
            with open(self.filename) as f:
                piginfo = f.readlines()
            message = '8'
            for i in range(0,len(self.pigids)):
                message += ',' + str(self.pigids[i]) + ',' + str(self.piglocations[i]) + ',' + self.pigstatus[i] 
            self.sendToSinglePeer(port, message)
            
    def display(self):
#        print ' No. of entries ' + str(len(self.pigids))
#        for i in range(0,len(self.pigids)):
#            print 'pig with id ' + str(self.pigids[i]) + ' is at position ' + str(self.piglocations[i]) + ' with status :' + self.pigstatus[i]
        with open(self.filename) as f :
            for line in f.readlines():  
                print line ; 
        f.close();
        