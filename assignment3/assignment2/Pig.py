'''
Created on Feb 28, 2013
'''

import socket
import threading
import time
from random import randint 
import random  
class Pig(object):

    def __init__(self,id,port,peerMap,position,stones,gridsize,npigs,failedpigs):
        
        self.id = id                    # Unique id assigned to this Pig
        self.port = port                # Port Number of this Pig
        self.failedpigs = failedpigs    # A list of failed pigs 
        self.npigs = npigs
        self.leader = -1 ;              # The leaders id. -1 indicates no leader has been elected so far.
        self.peerMap = peerMap          # A dictionary with the a mapping between all the peers id and their addresses 
        self.posMap = {}                # This is non-empty only for the leader. Maintains a mapping of pig ids to their locations
        self.okmessagereceived= False  # To check if a ok message has been received
        self.Iwonmessagesent = False    # To make just one copy of the iwon message is sent
        self.position = position        # The Pig's position on the grid
        self.startTime = time.time()    # The Game's start time. 
        self.hit = False                # Tells whether if the bird had any effect on the Pig. True means it either got hit by the bird or by a Pig/Stone Column
        self.gridsize = gridsize        # Size of our grid. The Pig needs to know this to update its position
        self.score = 0                  # The Game score after the bird has hit
        self.stones = stones            # Where the stone columns are in the grid
        self.clock = 0                  # Logical clock. Initially set to 0. Gets incremented everytime the pig receives any message.
        self.warningtime = -1           # Stores the time of bird approaching message. 0 indicates it has not received it
        self.landingtime = -1           # Stores when the bird landed at the pig
        self.pigs_hit = []              # List of all Pigs that got affected by the bird
        self.recvdPos = False           # boolean to indicate whether the leader has received all the pig positions
        self.recvdScores = False        # boolean to indicate whether the leader got the scores of all the affected Pigs.
        self.pigsToBeWarned = []        # A list of all the Pigs to be warned by the leader
        self.pigsReplied = []           # A list of all the Pigs who replied with their status (hit/not-hit) to the leader
        self.s = None;                  # To keep track of the open socket 
        self.associatedthreads = [] ;   # To keep track of the open threads 
   
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
        
        # Since the pig received a message, increment the clock by a random number between 1 & 5 
        self.clock += randint(1,5)
        host, port = peerSock.getpeername()
        data = peerSock.recv(1024)
        a = data.split(',')
        if a[0] == '1':
            # 1 signifies the bird approaching packet
            # Message format : 1, leaders time 
            
            # Assuming there are internal events . we update the clock before receiving the message randomly 
            self.clock += random.randrange(0,5); 
            leadertime = float(a[1])
            oldclock = self.clock ; 
            
            # Lamport clock synchronization 
            self.clock = max(self.clock,leadertime) + 1 ; 
            self.warningtime = leadertime
            
            print 'Received BIRD APPROACHING message - ID : ' + str(self.id) + ' Leaders Clock value : ' + a[1] + ' receivedpigs clock time : ' + str(oldclock) + ' synchronizedto : ' + str(self.clock);
            
            # Check if the pig received bird landing message before the bird approaching one
            # The Pig moves to a new position with the check on the edge of the grid 
            if self.landingtime == -1 :
                oldposition = self.position ; 
                if(self.position+1<self.gridsize):
                    self.position = self.position +1 ; 
                else :
                    self.position = self.position -1 ; 
                print ' pig with id ' + str(self.id) + ' with position : ' + str(oldposition) + ' takes evasive action moves to : ' + str(self.position);
                           
        elif a[0] == '2' : 
            # ASSUMPTION : we are assuming no logical clocks for the election part 
            # To receive the election packet 
            # Message format : 2, senders id  
            
            sender = int(a[1]); 
            if self.id > sender : 
                # send OK packet and start new election 
                self.sendOKmessage(sender)
                self.startelection()
                
                # The wait has been calculated such that the pigs time out until all the message exchange has happened 
                timeout = (self.npigs-len(self.failedpigs))*(self.npigs-len(self.failedpigs))*0.1; 
                time.sleep(timeout)

                # After the timeout period we check if the current pig has received a OK message 
                # In no OK message has been received then the current pig is the leader 
                # And iwonmessagesent is used to make sure the iwon message is sent only once 
                if self.okmessagereceived== False and self.Iwonmessagesent==False: 
                    self.sendIwonMessage();
                    self.setLeaderID(self.id)
                    self.Iwonmessagesent = True ;
    
        elif a[0] == '3':
            # ASSUMPTION : we are assuming no logical clocks for the election part 
            # On receiving the Ok packet the process understands that it is not a leader
            # Message format : 1, leaders time 
            
            self.okmessagereceived= True 
            
            # No the pig waits for an iwon message 
            
        
        elif a[0] == '4': 
            # ASSUMPTION : we are assuming no logical clocks for the election part 
            # On receiving the Iwon packet
            # format : 4,leadersid 
            
            # Set the leader id and send the position back to the leader
            leaderID = int(a[1]); 
            if leaderID>self.leader : 
                self.leader = leaderID ; 
             
            # Now pigs send their position to the leader  
            port = self.peerMap[self.leader] 
            
            # Hence forth we use the logical clocks for syncronization 
            # WIth the assumption that there are internal events we update clock randomly before the message is sent 
            self.clock += random.randrange(0,10); 
            message = '5,' + str(self.id) + ',' + str(self.clock) + ',' +  str(self.position)
            self.sendToSinglePeer(port,message)
            
        elif a[0] == '5':
            # Position packet , format 5,pigs id , its time  , its position 
            #Position packet is received by the leader 
            
            # To simulate the occurrence of the internal events we add some random number to the leaders clock 
            self.clock += random.randrange(0,2);
            pigid = int(a[1])
            pigpos = int(a[3])            
            sendersclock = float(a[2])
            
            # Lamport clock syncronization 
            self.clock = max(self.clock,sendersclock) + 1 
            self.posMap[pigid] = pigpos
            
            # The pig displays a message saying it has received all the positions 
            # Have to check if the leader has received all the pig positions
            npigs = self.npigs - 1 - len(self.failedpigs)
            if len(self.posMap) == npigs:
                print 'PIG POSITION received. Total number of pigs alive  - ' + str(npigs)
                self.recvdPos = True
        
        elif a[0] == '6':
            # Bird landed message , format : 6,sendersclock 
            # Received by the affected Pigs indicating the bird has landed
            self.clock+=random.randrange(0,5) ; 
            self.landingtime = float(a[1])
            oldclock = self.clock ; 
            self.clock = max(self.clock,self.landingtime) + 1 ; 
            print 'received BIRD LANDING  message : ' +str(self.id) + ' Leaders Clock: ' + a[1] + ' recipientsClock synchronizedfrom ' + str(oldclock) + ' to : ' + str(self.clock);
            
           
            # Pig already received the warning message and updated its position. So just send a not-hit reply to the leader.
            if self.warningtime > 0 and self.warningtime < self.landingtime:
                message = '7,' + str(self.id) + ',' + 'not-hit' + ',' + str(self.clock)
                leader_port = self.peerMap[self.leader]
                self.sendToSinglePeer(leader_port, message)

            # the warning time is not set if the bird approaching message hasnt reached 
            if self.warningtime==-1 or self.landingtime < self.warningtime:
                # Then the Pig has been hit. So notify the Leader
                # Every message sent has to have the time 
                self.hit = True ; 
                message = '7,' + str(self.id) + ',' +  'hit' + ',' + str(self.clock) 
                leader_port = self.peerMap[self.leader]
                self.sendToSinglePeer(leader_port, message)
        
        elif a[0] == '7':
            # Status packet  Format :7,senderid, status , clockvalue 
            # Received by the leader from affected Pigs telling their status after the bird landed.
            pigid = int(a[1])
            status = a[2]
            sendertime = float(a[3])
            self.clock = random.randrange(0,5) ; 
            self.clock = max(self.clock,sendertime) + 1 ;
            
            # Checks if the pig has been hit 
            # Pig replied to make sure all the pigs respond
            if status =='hit':
                print 'Received BIRD HIT at (leader):  synchronizedtime at ' + str(self.id) + ' : ' + str(self.clock);
                self.pigs_hit.append(pigid)
                print 'Pig ' + a[1] + ' Killed !'
                self.pigsReplied.append(a[1])
            elif status == 'not-hit':
                self.pigsReplied.append(a[1])
            
            # If the Number of pigs replied is equal to the number of afected pigs, then we set the boolean to true so the main game can proceed
            if len(self.pigsReplied) == len(self.pigsToBeWarned):
                self.recvdScores = True
            
    def startElectionThread(self,sleeptime):
        # The election is conducted using multiple threads to make sure there no blocking 
        t = threading.Thread(target = self.startelection, args = [sleeptime])
        self.associatedthreads.append(t);
        t.start()
            
    def startelection(self):
        # Called by the main game on any random pig at the start of the game
        # Sleep time denotes how long the pig will wait until it assumes either a leader has been elected or decides to elect itself. 
        current_id = self.id 
        message = '2,' + str(current_id)  
        for key in self.peerMap: 
            if key>current_id : 
                receiver = self.peerMap[key]
                self.sendToSinglePeer(receiver,message)
    
    def sendOKmessage(self,sender):
        # Sends the OK message if the Id of the current pig is higher than the pig which inititated the election 
        message = '3,' + str(self.id) ;
        reciever = self.peerMap[sender]
        time.sleep(0.1); 
        self.sendToSinglePeer(reciever,message)
            
    def sendIwonMessage(self):
        # Once the leader is elected the leader send the iwon message to all the pigs 
        message = '4,' + str(self.id) ; 
        for key in self.peerMap : 
            reciever = self.peerMap[key] 
            self.sendToSinglePeer(reciever,message);
    
    def birdapproaching(self,position,birdtime):
        
        # This function sends the Bird Approaching message
        # Called by the main game on whichever Pig is the leader
        # Find out which pigs are affected
        
        self.pigsToBeWarned = [] 
        # First checks if it is the pig that is going to be affected 
        move = False ; 
        
        # To handle the special case of the affected pig being the coordinator 
        
        if(self.position == position):
            print ' the bird is falling at the leaders position'
            move = True ; 
            print ' leader takes evasive action and escapes '
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
        warning_message = '1,' + str(self.clock)
        self.sendToAffectedPigs(self.pigsToBeWarned, warning_message)
        self.clock += randint(1,5)   # Increment the clock by some random number since we sent a message
        # Sleep until the birdtime has elapsed and then send a bird landed message to the affected pigs
        time.sleep(0.1*birdtime)
        landing_message = '6,' + str(self.clock)
        self.sendToAffectedPigs(self.pigsToBeWarned, landing_message,'no')
        self.clock += randint(1,5)
    
    
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
            delay =randint(1,self.npigs) 
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
    
    def hasRecvdPos(self):
        # to wait till the coordinator has received all the positions , for the main thread to continue
        return self.recvdPos
    
    def hasRecvdScores(self):
        # to wait till the coordinator has received all scores , for the main thread to continue 
        return self.recvdScores
   
