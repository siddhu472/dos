'''
Created on Feb 28, 2013
'''

import socket
import threading
import time
from random import randint 
import random  

class Pig(object):

    def __init__(self,id,port,peerMap,position,stones,gridsize,npigs,failedpigs,dbport):
        
        self.dbport = dbport 
        self.id = id                    # Unique id assigned to this Pig
        self.port = port                # Port Number of this Pig
        self.failedpigs = failedpigs    # A list of failed pigs 
        self.npigs = npigs
        self.leader = -1 ;              # The leaders id. -1 indicates no leader has been elected so far.
        self.peerMap = peerMap          # A dictionary with the a mapping between all the peers id and their addresses 
        self.posMap = {}                # This is non-empty only for the leader. Maintains a mapping of pig ids to their locations
        self.statusMap = {}
        self.position = position        # The Pig's position on the grid
        self.startTime = time.time()    # The Game's start time. 
        self.hit = False                # Tells whether if the bird had any effect on the Pig. True means it either got hit by the bird or by a Pig/Stone Column
        self.gridsize = gridsize        # Size of our grid. The Pig needs to know this to update its position
        self.score = 0                  # The Game score after the bird has hit
        self.stones = stones            # Where the stone columns are in the grid
        self.warningRecvd = False       # Indicates whether the Pig received bird warning message
        self.landingRecvd = False       # Indicates whether the Pig received bird landing message
        self.pigs_hit = []              # List of all Pigs that got affected by the bird
        self.recvdScores = False        # boolean to indicate whether the leader got the scores of all the affected Pigs.
        self.pigsToBeWarned = []        # A list of all the Pigs to be warned by the leader
        self.deadpigs =[]               # Pigs that are dead 
        self.pigsReplied = 0            # Number of Pigs who replied with their status (hit/not-hit) to the leader
        self.s = None;                  # To keep track of the open socket 
        self.associatedthreads = [] ;   # To keep track of the open threads 
        self.ACKcounter = 0             # To keep track of acknowledgement from pigs
        self.isAsleep = False           # boolean value keeping track of whether the leader is asleep or not
        self.leader2 = -1               # Saved by the leader Pigs to keep track of the other leader
        self.hasRecvdLeaderACK = False  # boolean keeping track of the leader recieving ACKs from the other leader
        self.hasRecvdDBAck = False      # boolean keeping track of whether the leader received pig info from DB
        self.haswritten = False         # to make sure that the leader writes to the db
        self.DBMap = {}                 # Mapping of ids to position and status. This is the info DB sends
        self.initalized = False         # To make sure the resources are set before proceeding 
        self.isawake = False 
    def mainloop(self):
        
        # Create the main socket. The Pig will listen to its peers on this socket
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind(('localhost', self.port))
        self.s.listen(5)
        while True:
            try:
                peersock,peeraddr = self.s.accept()
                peersock.settimeout(None)
                # Create a thread whenever the Pig gets any connection
                # The thread will in turn call the handle peer function
                t = threading.Thread(target = self.handlePeer, args = [peersock])
                t.start()
                self.associatedthreads.append(t); 
            except KeyboardInterrupt:
                print 'KeyboardInterrupt: stopping mainloop'
                break
            except:
                print 'Error'
                break
        self.s.close()

    def handlePeer(self,peerSock):
        """
        This is in charge of handling any message a Pig will receive from its peers
        """
        # If the Pig is asleep, then just return
        if self.isAsleep:
            return
                
        host, port = peerSock.getpeername()
        data = peerSock.recv(1024)
        a = data.split(',')
        if a[0] == '1':
            # 1 signifies the bird approaching packet
            # Message format : 1 
            
            print 'Received BIRD APPROACHING message - ID : ' + str(self.id) 
            self.warningRecvd = True
            # Check if the pig received bird landing message before the bird approaching one
            # The Pig moves to a new position with the check on the edge of the grid 
            if not self.landingRecvd:
                oldposition = self.position
                if(self.position+1<self.gridsize):
                    self.position = self.position +1
                else :
                    self.position = self.position -1 ; 
                print 'Pig with id ' + str(self.id) + ' with position : ' + str(oldposition) + ' takes evasive action moves to : ' + str(self.position);
                           
        elif a[0] == '2' : 
            # Packet received from the co-ordinator Pigs
            # The other Pigs will store this Leader ID
            leaderID = int(a[1])
            #print ' leader :' + a[1] 
            self.leader = leaderID 
            # Now send an acknowledgement to the leader saying it recognises the leader
            message = '5'
            leader_port = self.peerMap[self.leader]
            self.sendToSinglePeer(leader_port, message)
    
        elif a[0] == '3':
            # Received from the co-ordinators indicating the bird has landed
            # Message format : 3 
            # NEED TO WORK ON THIS
            # Pig already received the warning message and updated its position. So just send a not-hit reply to the leader.
            self.landingRecvd = True ;
            if self.warningRecvd:
                message = '4,' + str(self.id) + ',' + str(self.position) + ',not-hit'
                leader_port = self.peerMap[self.leader]
                self.sendToSinglePeer(leader_port, message)

            # the warning time is not set if the bird approaching message hasnt reached 
            else:
                # Then the Pig has been hit. So notify the Leader
                # Every message sent has to have the time 
                self.hit = True ; 
                message = '4,' + str(self.id) + ',' + str(self.position) + ',hit' 
                leader_port = self.peerMap[self.leader]
                self.sendToSinglePeer(leader_port, message)
            
        elif a[0] == '4': 
            # Status packet  Format :4,senderid, status  
            # Received by the leader from affected Pigs telling their status after the bird landed.
            pigid = int(a[1])
            pos = int(a[2])
            status = a[3]
            # Checks if the pig has been hit 
            # Pig replied to make sure all the pigs respond
            if status =='hit':
                print 'Received BIRD HIT at (leader):'
                self.pigs_hit.append(pigid)
                self.posMap[pigid] = -1 ; 
                if int(a[1]) not in self.deadpigs : 
                    self.deadpigs.append(int(a[1]))
                print 'Pig ' + a[1] + ' Killed !'
                self.pigsReplied += 1
            elif status == 'not-hit':
                self.posMap[pigid] = pos; 
                self.pigsReplied += 1
            
            # If the Number of pigs replied is equal to the number of afected pigs, then we set the boolean to true so the main game can proceed
            if self.pigsReplied == len(self.pigsToBeWarned):
                self.recvdScores = True
                print 'Received status from all Pigs'

        elif a[0] == '5':
            # Recieved by leader as an acknowledgement from the Pigs it is incharge of 
            self.ACKcounter += 1
        
        elif a[0] == '6':
            # Received by one leader from another asking for its status
            if self.isAsleep:
                return
            message = '7'
            leaderPort = self.peerMap[self.leader2]
            self.sendToSinglePeer(leaderPort, message)
        
        elif a[0] == '7':
            # Received by a leader from another leader giving its status
            self.hasRecvdLeaderACK = True
        
        elif a[0] == '8':
            # Received from the Database Server giving pig information
            self.hasRecvdDBAck = True
            self.DBMap = {}
            for i in range(1,len(a),3):
                if i+2<len(a):
                    current_id = int(a[i])
                    current_loc = int(a[i+1])
                    current_stat = a[i+2]
                    self.DBMap[current_id] = [current_loc,current_stat]
                    
        elif a[0] == '9':
            # Received from one leader by another telling that its awake
            # The leader will then proceed to assign some pigs to the just awakened leader
            leaderIDs = [self.id, self.leader2]
            # Randomly select pigs the 2 leaders will be incharge of
            remainingPigs = set(range(self.npigs)) - set(leaderIDs) 
            
            piglist1 = random.sample(remainingPigs, (self.npigs-2)/2)
            piglist2 = list(remainingPigs - set(piglist1))
            
            for pig in piglist1:
                if pig in self.deadpigs:
                    piglist1.remove(pig);
            
            for pig in self.deadpigs: 
                if pig in piglist2:
                    self.deadpigs.remove(pig)
             
            for pig in piglist2 : 
                if pig in self.posMap:
                    del self.posMap[pig]
                           
            self.assignPigs(piglist1)
            
                    
#            print 'list for ' + str(self.id)
#            print self.pigList
            print self.deadpigs 
            self.setPigPos(self.posMap)
            
            
            message = '10'
            for pigid in piglist2:
                message += ',' + str(pigid)
            port = self.peerMap[self.leader2]
            self.sendToSinglePeer(port, message)
            
        
        elif a[0] == '10':
            # Received by the now awake leader from another leader giving the ids of the pigs it is incharge of
            for i in range(1,len(a)):
                self.pigList.append(int(a[i]))
            self.ACKcounter = 0; 
            self.notifyPigs()
            
            # Wait till all the pigs are notified 
            while self.ACKcounter != len(self.pigList):
                time.sleep(0.1)
            # Include a wait here 
            
            self.ACKcounter = 0; 
            # Request info from DB
            self.hasRecvdDBAck = False ; 
            message = '2,' + str(self.port) 
            self.sendToSinglePeer(self.dbport, message)
            # Wait until DB replies back
            while not self.hasRecvdDBAck:
                time.sleep(0.1)
            self.hasRecvdDBAck = False
            
            for pigid in self.DBMap:
                location,status = self.DBMap[pigid]
                if pigid in self.pigList:
                    if 'F' in status :
                        self.posMap[pigid] = location
                    else:
                        self.pigList.remove(pigid)
                        self.deadpigs.append(pigid);  
            
#            print 'list for pig with id ' + str(self.id)
#            print self.pigList
#            print self.deadpigs
            self.isawake = True ; 
        elif a[0] == '11':
            self.haswritten = True ; 
            
        
    def birdapproaching(self,position,birdtime):
        
        # This function sends the Bird Approaching message
        # Called by the main game on whichever Pig is the leader
        # Find out which pigs are affected
        
        # If the leader is asleep, then just return
        if self.isAsleep:
            return
        
        self.pigsToBeWarned = [] 
        # First checks if it is the pig that is going to be affected 
        move = False
        
        # To handle the special case of the affected pig being the coordinator 
        
        if(self.position == position):
            print 'The bird is falling at the leaders position'
            move = True ; 
            print 'Leader takes evasive action and escapes '
            pigpositions = self.posMap.values()
            # Check if the Pig has a neighboring stone column to the right
            if (position + 1) in self.stones:
                if (position + 2) in pigpositions:
                    key = self.getKeyFromValue(self.posMap, position + 2)
                    self.pigsToBeWarned.append(key)
                    
            # Otherwise check if there is a pig to the right
            if (position + 1) in pigpositions:
                key = self.getKeyFromValue(self.posMap, position + 1)
                self.pigsToBeWarned.append(key)
            
            # No pigs are to be warned the main thread can continue 
            if(len(self.pigsToBeWarned)==0):
                self.recvdScores = True ;
        else : 
            if (self.position == position+1 and position == self.getKeyFromValue(self.posMap, position ) ): 
                move = True ; 
            if (self.position == position+2 and position+1 in self.stones and self.getKeyFromValue(self.posMap, position + 1) ): 
                move = True ; 
            self.pigsToBeWarned = self.getAffectedPigs(position)
        if(move == True):
            if(self.position+1<self.gridsize): 
                self.position = self.position + 1 ; 
            else :
                self.position = self.position - 1 ; 
         
        print 'Pigs to be warned : ' + str(self.pigsToBeWarned)
        if(len(self.pigsToBeWarned)==0):
            self.recvdScores = True ;
        # Send the warning to those pigs. Also sends its current logical time.
        warning_message = '1'
        self.sendToAffectedPigs(self.pigsToBeWarned, warning_message)
        # Sleep until the birdtime has elapsed and then send a bird landed message to the affected pigs
        time.sleep(0.1*birdtime)
        landing_message = '3'
        self.sendToAffectedPigs(self.pigsToBeWarned, landing_message,'no')
    
    
    def sendToAffectedPigs(self,pigsToBeWarned,message, delay='yes'):
        # Sends a bird approaching warning to the affected Pigs
        # ALL messages are threaded and hence no blocking
        for pig in pigsToBeWarned:
            port = self.peerMap[pig]
            self.sendToSinglePeer(port,message,delay)
    
    def getAffectedPigs(self,position):
        # Get all the Pig Positions
        pigpositions = self.posMap.values()
        
        # Maintain a list of Pigs we need to notify
        
        pigsToBeWarned = []
        # First check if there is a pig in the bird landing position
        if position in pigpositions:
            key = self.getKeyFromValue(self.posMap, position)
            pigsToBeWarned.append(key)
            
            # Check if the Pig has a neighboring stone column to the right
            if (position + 1) in self.stones:
                if (position + 2) in pigpositions:
                    key = self.getKeyFromValue(self.posMap, position + 2)
                    pigsToBeWarned.append(key)
                    
            # Otherwise check if there is a pig to the right
            if (position + 1) in pigpositions:
                key = self.getKeyFromValue(self.posMap, position + 1)
                pigsToBeWarned.append(key)
        
        # If there is no pig, check if there is a stone column in the bird landing position
        if position in self.stones:
            # Now, Check if there is a pig to the right
            if (position + 1) in pigpositions:
                key = self.getKeyFromValue(self.posMap, position + 1)
                pigsToBeWarned.append(key)
        return pigsToBeWarned
    
    def getLeaderID(self):
        # returns the leaders id 
        return self.leader
    
    def setLeaderID(self, leader):
        # sets the leaders id 
        self.leader = leader
        
    def getKeyFromValue(self,d,val):
        # Return the key with value v in dictionary d
        for key in d:
            if d[key] == val:
                return key
      
    # to write to the database 
    def writetodb(self):
        #get all the pig ids , their positions and status
        if self.isAsleep:
            self.haswritten = True ; 
            return
        message = '1,' + str(self.port)
        for pig in self.pigList:
            pos = self.posMap[pig]
            if pig not in self.deadpigs :
                stat = 'F'
                message += ','+str(pig)+','+str(pos)+','+stat; 
        for pig in self.deadpigs:
            message +=','+str(pig)+','+'-1,T'
        # Removing dead pigs 
        self.sendToSinglePeer(self.dbport, message)
        for pig in self.pigList:
            if pig in self.deadpigs:
                self.pigList.remove(pig)
    
          
    def sendToSinglePeer(self,port,message, delay = 'yes'):
        # Sends a message to the peer port. Threaded.
        # All messages are threaded and hence no blocking 
        t = threading.Thread(target = self.sendToPeer, args = [port,message,delay])
        self.associatedthreads.append(t) ; 
        t.start()
        
    def sendToPeer(self,port,message,allowDelay):
        # Send message to the specified port
        # We also introduce a random delay
        # The allow Delay argument is just to make sure that the bird landing message is sent without delay 
        # All message are non blocking since they use a threading mechanisam(sendToSinglePeer) is used in every case 
        delay = 0
        if allowDelay == 'yes':
            delay = randint(1,self.npigs) 
        time.sleep(0.1*delay)
        address = ('localhost',port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(address)
        sock.sendall(message)
        sock.close()
    
    def getScore(self):
        # To obtain the score 
        if len(self.pigs_hit) > 0 :
            print 'The Pigs that were hit : ' + str(self.pigs_hit)
        return len(self.pigs_hit)
    
    def hasRecvdScores(self):
        # to wait till the coordinator has received all scores , for the main thread to continue 
        return self.recvdScores
   
    def setPigPos(self, pigPos):
        for pig in pigPos:
            if pig in self.pigList:
                self.posMap[pig] = pigPos[pig]
            
    
    def assignPigs(self, pigList):
        self.pigList = pigList
    
    def notifyPigs(self):
        # Tell the Pigs that it is the leader
        self.leader = self.id
        message = '2,' + str(self.id)  
        for pig in self.pigList:
            port = self.peerMap[pig]
            self.sendToSinglePeer(port, message)
    
    def hasRecvdACK(self):
        # Returns True if this Pig (which is the leader) has recieved ACKs from all the pigs it is incharge of
        if self.isAsleep == True :
            return True ;
        
        if self.ACKcounter == len(self.pigList):
            return True
        else:
            return False  
    
    def initialize(self):
        # Below variables have to be reset for each game iteration
        
        # If Pig is asleep, then just return
        if self.isAsleep:
            self.initalized = True ; 
            self.recvdScores = True ;
            return
        
        self.resetVariables()
        
        # Now check if the other leader is up
        self.hasRecvdLeaderACK = False ; 
        leaderPort = self.peerMap[self.leader2]
        message = '6'
        self.sendToSinglePeer(leaderPort, message)
        
        # Wait for 2*npigs secs which is the max possible delay
        time.sleep(3*self.npigs*0.1)
        
        if not self.hasRecvdLeaderACK :
            # If no ACK is received till now, then the other leader is quite likely asleep
            # So request all Pig info from the Database
            self.hasRecvdDBAck = False ; 
            message = '2,' + str(self.port) 
            self.sendToSinglePeer(self.dbport, message)
            
            # Include wait 
            # Using Dback here, make sure there is no conflict anywhere else 
            while not self.hasRecvdDBAck :
                time.sleep(0.1)
            self.hasRecvdDBAck = False ;
            self.pigList = [] # these pigs have to be notified about the new leader     
            for pigid in self.DBMap:
                location,status = self.DBMap[pigid]
                print str(pigid) + ' ' + str(location) + ' ' + status
                if 'F' in status : 
                    self.pigList.append(pigid)
                    self.posMap[pigid] = location
                else :
                    if pigid not in self.deadpigs : 
                        self.deadpigs.append(pigid)
                #print'for' + str(self.id) + ' appending :' + pigid +':' + location +':'+ status
            
            # Include notify new set of pigs 
            self.ACKcounter = 0 ; 
            self.notifyPigs();
            
            while self.ACKcounter != len(self.pigList):
                time.sleep(0.1)
                   
        self.initalized = True ; 
    
    def resetVariables(self):
        self.ACKcounter = 0
        self.score = 0
        self.warningRecvd = False        
        self.landingRecvd = False        
        self.pigs_hit = []            
        self.recvdScores = False      
        self.pigsToBeWarned = []       
        self.pigsReplied = 0   
        #self.deadpigs =[]
        self.hasRecvdLeaderACK = False
        self.haswritten = False ;
        self.DBMap = {}
            
    def fallAsleep(self):
        self.isAsleep = True
        # deallocating all the variables for that leader 
        self.pigList = []
        self.posMap = {}
        self.deadpigs = []
        self.resetVariables()
    
    def setOtherLeader(self, id):
        self.leader2 = id
    
    def wakeup(self):
        # Wakes up the current leader and notifies the other leader that its awake
        self.isAsleep = False
        message = '9'
        port = self.peerMap[self.leader2]
        self.sendToSinglePeer(port, message)
