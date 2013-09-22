'''
Created on Feb 28, 2013
'''

import socket
import threading
import time
import traceback

class Pig(object):

    def __init__(self,id,port,peers,position,stones,gridsize):
        self.id = id                    # Unique id assigned to this Pig
        self.port = port                # Port Number of this Pig
        self.peers = peers              # A list of the Pig's Peers (values are the port numbers)
        self.position = position        # The Pig's position on the grid
        self.startTime = time.time()    # The Game's start time. 
        self.hit = False          # Tells whether if the bird had any effect on the Pig. True means it either got hit by the bird or by a Pig/Stone Column
        self.gridsize = gridsize        # Size of our grid. The Pig needs to know this to update its position
        self.score = 0                  # The Game score after the bird has hit
        self.stones = stones            # Where the stone columns are in the grid
        self.neighbors = []             # The physical neighbors of the pig
        self.washitcount = 0            # Makes sure that the washit packet was recieved from all the pigs to which the status packet was sent
        # In the mainGame , the main thread is made to sleep until the was hit packets are recieved from all the pigs . 
        # By doing this we make sure that we do not calculate the score before getting the was hit packets from the birds
        self.birdpacketcount = 0        # Make sure all the birds reply to the bird appraoching packet before quering the pigs for its status 
        self.birdinfo = False           # As a part of the implementation , we make sure that the bird reply is handled just once 
        
    def mainloop(self):
        
        # Creae the main socket. The Pig wil listen to its peers on this socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('localhost', self.port))
        s.listen(5)
        while True:
            try:
                peersock,peeraddr = s.accept()
                peersock.settimeout(None)
                # Create a thread whenever the Pig gets any connection
                # The thread will in turn call the handle peer function
                t = threading.Thread(target = self.handlePeer, args = [peersock])
                t.start()
            except KeyboardInterrupt:
                print 'KeyboardInterrupt: stopping mainloop'
                break
            except:
                print 'Error'
                break
        s.close()

    def handlePeer(self,peerSock):
        """
        This is in charge of handling any message a Pig will receive from its peers
        """
        host, port = peerSock.getpeername()
        data = peerSock.recv(1024)
        a = data.split(',')
        if a[0] == '1':
            # 1 signifies the bird approaching packet
            # Message format : 1, bird landing position, hopcount, birdtime, path
            #print str(self.id) +  ' Received BIRD APPROACHING packet '
            birdposition = int(a[1])
            hopcount = int(a[2])
            birdtime = float(a[3])
            
            # Extract the path. The path starts from the 4th element of the list a
            path = [] 
            for i in range(4,len(a)):
                path.append(int(a[i]))
            
            # If hopcount is greater than 0, send it to its peers, otherwise Ignore the packet 
            # Sleep for a second before sending the packet.
            if(hopcount>0):
                hopcount = hopcount-1
                path.append(self.port)
                time.sleep(1)
                self.birdapproaching(birdposition, hopcount,birdtime,path)  
                   
            # Acknowledge the receipt of the bird approaching packet.
            # path[0] returns the Pig that initiated the bird approaching message
            if(self.birdinfo==False):
                self.birdpacketreply(path[0],path)
            
            self.birdinfo = True
            
            # Check if there is a stone column next to the Pig and if the bird is colliding with the stone column
            stoneflag = False
            for stone in self.stones : 
                if birdposition == stone and (self.position == stone+1) and stoneflag == False : 
                    # If the bird collision has not yet happened, then get the Pig out & notify neighbors
                    if(time.time()< self.startTime + birdtime):
                        self.handleIncoming(birdtime)
                    # If the time is up, then the Pig is dead. pig_neighbor will store the id of a neighboring Pig (if any).
                    # We will have to kill the neighboring pig as well
                    else :
                        self.hit = True 
                        for n in self.neighbors : 
                            if self.position+1 == n :
                                pig_neighbor = n 
                                flag = True 
                                break 
                            
            # IF there is a stone column next to the current pig, then we have to kill the other Pig(if any) next to the stone column
            if(stoneflag == True) : 
                # contains a neighbour that has to be killed 
                hopcount = 5 # can be made random later
                time.sleep(1)
                self.killneighbor(self.id,pig_neighbor,hopcount)
            
            # If the current Pig's position matches with the bird landing position, proceed further
            if (birdposition == self.position):
                # Check if the Pig's current system time is less than the bird landing time
                if time.time() < self.startTime + birdtime : 
                    self.handleIncoming(birdtime)
                # Otherwise the Pig is hit. Set its hit variable to True
                else:
                    if self.hit == True :
                        return 
                    self.hit = True
                   
                    
                    flag = False
                    for stone in self.stones: 
                        if self.position+1 == stone and flag == False:
                            for n in self.neighbors : 
                                if self.position+2 == n : 
                                    pig_neighbor = n 
                                    flag = True 
                                    break 
                      
                    if not flag:
                        for n in self.neighbors : 
                            if (self.position+1) == n :
                                pig_neighbor = n 
                                flag = True 
                                break 
                    
                    if(flag == True) : 
                        # contains a neighbour that has to be killed 
                        hopcount = 5 # can be made random later
                        time.sleep(1)
                        self.killneighbor(self.id,pig_neighbor,hopcount)
                    
        elif a[0] == '2':
            # 2 is for Status Query
            # Has the form 2,sender,hopcount,path(varying length)
            #print str(self.id) +  ' Received STATUS packet ' 
            path = []
            pigid = int(a[1])   # id of the Pig the status was sent for
            hopcount = int(a[2])    
            for i in range(3,len(a)):   # Extract the path the message traversed.
                path.append(int(a[i]))
            
            # If the current Pig's id matches the status Pig id, then return the Pig status (By calling wasHit)
            if self.id == pigid:
                time.sleep(1)
                self.wasHit(self.position,self.hit,path)
            # Otherwise, circulate the message if hopcount > 0
            else:
                if hopcount>0 : 
                    hopcount = hopcount-1  
                    path.append(self.port) 
                    time.sleep(1)
                    self.status(pigid,hopcount,path)
                    
        elif a[0] =='3' :
            self.washitcount +=1
            # 3 is for was hit 
            # Has the form 3,port of the pig that sent the status message , affected pigs id,path 
            #print str(self.id) + ' Received WAS_HIT packet'
            statussenderpig = int(a[1])     # id of the pig that initiated the status
            affectedpig = int(a[2])         # The Pig from which status is requested
            
            # IF the current pig was the one that initiated the status request, then keep track of the score
            if(self.port == statussenderpig):
                print 'Result from pig at position ' + str(affectedpig) + ' WAS HIT ? :' + a[3] 
                if(a[3]=='True'):
                    self.score+=1      
            # Otherwise, Send washit to its peers       
            else :
                path = [] 
                for i in range(4,len(a)):
                    path.append(int(a[i]))
                time.sleep(1)
                if(a[3]=='True'):
                    result = True ;
                else:
                    result = False ; 
                time.sleep(1)
                self.wasHit(affectedpig,result,path)    
        
        elif a[0] == '4' :
            # Take shelter message
            # IF the current Pig is in the danger zone specified in the message, then change its position
            #print str(self.id) + ' Received TAKE_SHELTER packet'
            pos = self.position
            affectedpos = int(a[1])
            if (pos <= affectedpos + 2) & (pos >= affectedpos - 2):
                self.updatePosition()
                
        elif a[0] == '5' : 
            # Kill neighbor message
            sender = int(a[1]) 
            reciever = int(a[2])
            hopcount = int(a[3])
            
            # If the current Pig's position is the same that on the message, then it needs to be killed
            # So set its self.hit to True
            if(self.position == reciever): 
                self.hit = True
            # Otherwise if hopcount is >0, then send the message to its peers (after decreasing the hopcount)
            else : 
                if hopcount>0 : 
                    hopcount = hopcount -1 
                    time.sleep(1)
                    self.killneighbor(a[1],a[2],hopcount)
    
        elif a[0] == '6':
            # This packet makes sure that all the pigs are informed about their physical neighbors 
            # We define neighbors as the pigs in the range {-2 to 2] from its position 
            # The address is sent through the peer to peer network only using flooding
            physicalid = int(a[1])
            hopcount = int(a[2])
            if(physicalid == self.position+1 or physicalid == self.position-2  or physicalid == self.position-1  or physicalid == self.position+2  ):
                if physicalid not in self.neighbors:
                    self.neighbors.append(physicalid)
            if hopcount > 0 : 
                hopcount= hopcount -1 ; 
                self.sendaddress(a[1],str(hopcount))
    
        elif a[0] == '7':
            # Status all message
            # If the status of all the pigs is to be queried the status all packet is made use of 
    
            print str(self.id) +  ' Received STATUSALL packet ' 
            path = []
            hopcount = int(a[1]) 
            for i in range(2,len(a)):
                path.append(int(a[i]))
            
            time.sleep(1)
            self.wasHit(self.id,self.hit,path)
            
            if hopcount>0 : 
                hopcount = hopcount-1  
                path.append(self.port) 
                time.sleep(1)
                self.statusall(hopcount,path)
                
        elif a[0] == '8' :
            # To receive the birdreply and handle it .
            # if the port matches with the sender then the bird packet count is updated 
            sender = int(a[1]) 
            path = []
            # Obtain the path from the message 
            for i in range(2,len(a)):
                path.append(int(a[i]))
            
            # If the sender gets back the reply packet , the count is updated 
            if self.port == sender:
                self.birdpacketcount+=1
            # otherwise the packet is sent through the network using the path , there is no flooding in this case 
            else: 
                time.sleep(1)
                self.birdpacketreply(a[1],path)    
        
    
    def birdpacketreply(self,sender,path):
        # Retrace the path of the birdapproaching packet 
        previous = int(path[-1])
        # remove the last node from the path and send the packet to that node 
        message = '8,' + str(sender)
        path.remove(path[-1])
        for p in path : 
            message +=',' + str(p)
        self.sendToSinglePeer(previous,message)
        
    def sendToPeers(self,message):
        # Send message to all he current Pig's peers
        # Note that a thread is created and all socket communication is handled within this thread
        for port in self.peers:
            t = threading.Thread(target = self.sendToSinglePeer, args = [port,message])
            t.start()

    def sendToSinglePeer(self,port,message):
        # Send message to the specified port
        address = ('localhost',port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(address)
        sock.sendall(message)
        sock.close() 
     
    def sendaddress(self,id,hopcount):
        # Query for physical neighbors
        message = '6,' + str(id) + ',' + str(hopcount)
        self.sendToPeers(message)
                
    
    def statusall(self,hopcount,path):
        # Query the status of all the pigs in the grid
        message = '7,' + str(hopcount)
        for p in path : 
            message+= ',' + str(p)  
        self.sendToPeers(message)
    
    
    def killneighbor(self,sender,reciever,hopcount):
        # Kill Neighbor whose id is reciever 
        message = '5,' + str(sender) + ',' + str(reciever) +',' + str(hopcount) 
        self.sendToPeers(message)
        
    def handleIncoming(self,birdtime):
        #Evade the incoming bird and notify physical neighbors
        pos = self.position
        self.updatePosition()
        time.sleep(1)
        self.takeShelter()  # Tell the physical neighbors
    
    def status(self, pigid,hopcount,path):
        # Request for status
        # 2 is the message id, pig whose status is enquired , hop count 
        message = '2,' +  str(pigid) + ',' + str(hopcount)
        for p in path : 
            message+= ',' + str(p)  
        self.sendToPeers(message)
    
    def wasHit(self,iid,result,path):
        # 3 is the message id , the pig that sent the status enquiry , the pig that was queried , result on if it was hit or not,path
        previous = int(path[-1])
        message = '3,' + str(path[0]) + ',' + str(iid) + ',' +  str(result) 
        path.remove(path[-1])
        for p in path : 
            message +=',' + str(p)
        self.sendToSinglePeer(previous,message)
        
    def takeShelter(self):
        # 4 is the message id , followed by the position of the affected Pig
        # All pigs within range+/- of the current pig's position
        message = '4,' + str(self.position)
        self.sendToPeers(message)
    
    def birdapproaching(self,position,hopcount,birdtime,path):
        # This function sends the Bird Approaching message
        # Called by the main game on whichever Pig saw the bird move
        message = '1,'+ str(position) + ',' + str(hopcount)+',' + str(birdtime) 
        for p in path : 
            message+= ','+str(p) ;
        self.sendToPeers(message)

    def getCurrentTime(self):
        #Returns the time elapsed since the game started
        return round((time.time() - self.startTime),2)
    
    def setStartTime(self):
        # Initialize the Start time
        self.startTime = time.time()
         
    def updatePosition(self):
        # Move thePig to a new position the evade the bird
        pos = self.position
        # If the next position is not beyond the grid, then move the Pig there
        if pos < self.gridsize and (pos+1) not in self.stones:
            newpos = pos + 1
            self.position = newpos
        # Otherwise move it to one position before it
        else:
            if (pos-1) not in self.stones : 
                newpos = pos - 1
                self.position = newpos
            else : 
                self.hit = True ;
    def getPosition(self):
        return self.position
        
