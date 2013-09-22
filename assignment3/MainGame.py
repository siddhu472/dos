'''
Created on Mar 1, 2013
@author: gouthamdl and siddharthct
'''
from Pig import Pig
import threading
import random 
from collections import defaultdict
import time
import sys
from database import  database 

def callFunc(func1, func2, funcargs):
    # Creates 2 threads and calls the 2 functions
    t1 = threading.Thread(target = func1, args = funcargs)
    t1.start()
    #leader2.birdapproaching(birdposition, birdtime)
    t2 = threading.Thread(target = func2, args = funcargs)
    t2.start()

iteration = 1;  
continueIteration = True; 

response = input('Enter the number of pigs in the network : ')
npigs = int(response)
# Assuming the number of stone blocks are half as the number of pigs

score = 0; 
nstoneblocks = npigs/2
asleepLeaders = []
# Creating ports for the pigs 
ports = [] 
for x in range(0,npigs):
    t = random.randrange(1025,40000); 
    while ports.__contains__(t ):
        t = random.randrange(1025,40000); 
    ports.append(t) ;  

# Defining the grid 
dbport = random.randrange(1025,40000); 
while ports.__contains__(dbport):
    dbport = random.randrange(1025,40000);
    
db = database(dbport,npigs)
failedpigs =[];
stones = [];
gridsize = npigs*2 # Size of the grid
grid = [0]*gridsize 
pigs = []
posMap = {} # Maintains a mapping of the pig id and its location

# Assigning random locations to the stone columns such that no stone columns are together
for s in range(0,nstoneblocks):
    position = random.randrange(0,gridsize)
    # To avoid two continuous stone blocks 
    while grid[position] != 0 or (position+1 < gridsize and grid[position+1] == 2) or (position-1 >0 and grid[position-1] == 2): 
            position = random.randrange(0,gridsize)   
    grid[position] = 2 
    stones.append(position)
      
# Assigning random locations to the pigs
for i in range(0,npigs):
    if i not in failedpigs :
        distance = random.randrange(0,gridsize)  
        while grid[distance]!=0:
            distance = random.randrange(0,gridsize) 
        grid[distance] = 1   
        posMap[i] = distance
        peerMap = dict()
        for j in range(0,npigs): 
            if i!=j :
                if j not in failedpigs : 
                    peerMap[j] = ports[j] 
        pig = Pig(i,ports[i],peerMap,distance,stones,gridsize-1,npigs,failedpigs,dbport)
        pigs.append(pig)
    else : 
        # All the failed pigs are given no ports and are made dormamnt 
        peerMap = dict(); 
        pig = Pig(i,0,peerMap,0,[],gridsize-1,npigs,failedpigs,dbport,db)
        pigs.append(pig)

print "Displaying the 1d grid \n0-unoccupied \n1-Pig \n2-stone \npositions denoted from 0 to " + str(gridsize-1) + " with 0 being on the left"
print grid
threads = []
# Start the main loop for each Pig which will handle all p2p communications for that pig
for i,p in enumerate(pigs):
    if i not in failedpigs:
        t = threading.Thread(target=pigs[i].mainloop)
        t.start()
        threads.append(t)
        
t2 = threading.Thread(target = db.mainloop)
t2.start(); 
threads.append(t2);

# Randomly select 2 Pigs to be the leader
leaderIDs = [1,2]
leader1 = pigs[leaderIDs[0]]
leader2 = pigs[leaderIDs[1]]
# Assign n/2 randomly selected Pigs to Leader 1 and rest to Leader 2
# First create a set of Pig ids excluding the leader IDs
remainingPigs = set(range(npigs)) - set(leaderIDs)

# Then Randomly assign Pigs to one of the 2 leaders
piglist1 = random.sample(remainingPigs, (npigs-2)/2)
leader1.assignPigs(piglist1)
leader1.setPigPos(posMap)
leader1.notifyPigs() 
   
# Tell the Pigs it is in charge of that it is the leader
# Next 3 lines just for printing
pigs1 = [pigs[pigid] for pigid in piglist1]
pigpos1 = [pig1.position for pig1 in pigs1]
#print 'Pigs leader 1 is incharge of : ' + str(pigpos1)

piglist2 = list(remainingPigs - set(piglist1))
leader2.assignPigs(piglist2)
leader2.setPigPos(posMap)
leader2.notifyPigs()

leader1.setOtherLeader(leaderIDs[1]) 
leader2.setOtherLeader(leaderIDs[0])   

# Next 3 lines just for printing
pigs2 = [pigs[pigid] for pigid in piglist2]
pigpos2 = [pig2.position for pig2 in pigs2]
#print 'Pigs leader 2 is incharge of : ' + str(pigpos2)

# Wait until both leaders have recieved acknowledgement from the pigs
# This is to ensure all the Pigs know who the leader is before we goto the Bird warning part
print 'Informing Pigs about the leaders '
while not leader1.hasRecvdACK() or not leader2.hasRecvdACK():
    time.sleep(0.1)
# For future iterations 
leader1.hasRecvdACK = False ; 
leader2.hasRecvdACK = False ; 


while continueIteration == True:
    
    print '-----------------------------------------------------------------------------------'
    print 'Iteration : ' + str(iteration);
    leader1.initalized = False ; 
    leader2.initalized = False ; 
    # Initializes the 2 leaders. Also checks if the other leader is up
    callFunc(leader1.initialize, leader2.initialize, [])
    
    while not leader1.initalized or not leader2.initalized : 
        time.sleep(0.1) 
    
    # Setting the variables up for the next iteration 
    leader1.initalized = False ; 
    leader2.initalized = False ; 
    
    if(not leader1.isAsleep):
        print 'Position of leader 1 : ' + str(leader1.position) 
        print 'Pigs leader 1 is incharge of : ' + str(leader1.pigList)
        print 'Dead pigs under leader 1 :' + str(leader1.deadpigs)
    else : 
        print 'Leader 1 is asleep'
    if(not leader2.isAsleep):
        print 'Position of leader 2 : ' + str(leader2.position) 
        print 'Pigs leader 2 is incharge of : ' + str(leader2.pigList)
        print 'Dead pigs under leader 1 :' + str(leader2.deadpigs)
    else :
        print 'Leader 2 is asleep'

    
    # Create random values for bird landing time and position
    birdtime = random.randrange(1,2*npigs); 
    birdposition = random.randrange(0,gridsize);
    # The Bird's landing position and time taken is given below
    print '---------------------------'
    print "Bird landing position :" + str(birdposition)
    print "Time taken by the bird :" + str(birdtime)
    
    print "The Leader gets the bird time and bird landing positions and sends it to the affected pig "
    # Give the bird details to the 2 leaders. Below functions calls the bird approaching function of
    # the 2 leaders using threading.
    callFunc(leader1.birdapproaching, leader2.birdapproaching, [birdposition, birdtime])
    
    #Waiting till the leader has received all the statuses
    while not leader1.recvdScores or not leader2.recvdScores:
        time.sleep(0.1)
    
    db.clearcontents()
    callFunc(leader1.writetodb, leader2.writetodb, [])
    
    while not leader1.haswritten or not leader2.haswritten : 
        time.sleep(0.1)
    
    currentscore = leader1.getScore() + leader2.getScore()
    print 'Score in the current iteration : ' + str(currentscore) ; 
    score+=currentscore ;
    print 'Total Score : ' + str(score)
    
    db.display() 
    
    if(score == npigs-2):
        print ' all pigs are dead ' ; 
    exitresponse = raw_input('y to continue / n to exit : ' ); 
    if exitresponse == 'n':
        continueIteration = False ;  
        print ' Done'  
         
    else :
        iteration+=1
        
        # If one of the leaders is asleep, then wake them up with a probability of 0.5
        if len(asleepLeaders) != 0:
            leader = pigs[asleepLeaders[0]]
            if random.random() > 0.5:
                leader.isawake = False ; 
                leader.wakeup()
                while not leader.isawake : 
                    time.sleep(0.1)
                leader.isawake = False ; 
                leaderIDs.append(leader.id)
                print 'Leader ' + str(leader.id) + ' waking up.'
                asleepLeaders = []
        # Otherwise, put one of the leaders to sleep if both of them are awake
        elif len(leaderIDs) == 2:
            # Randomly select 1 leader and with probability 0.5 make it fall asleep
            if random.random() > 0.5:
                leaderToSleepID = random.choice(leaderIDs)
                leaderToSleep = pigs[leaderToSleepID]
                print 'Leader ' + str(leaderToSleepID) + ' falling asleep'
                #print 'Leader IDs : ' + str(leaderIDs)
                leaderToSleep.fallAsleep()
                leaderIDs.remove(leaderToSleepID)
                asleepLeaders.append(leaderToSleepID)
                time.sleep(npigs*0.1)
            else:
                print 'No leader is asleep'
            
    
for pig in pigs : 
    if pig.id not in failedpigs : 
        for thread in pig.associatedthreads:
            if thread.isAlive():
                try:
                    thread._Thread__stop()
                except:
                    print(str(thread.getName()) + ' could not be terminated')
        for thread in pig.associatedthreads : 
            thread.join();        


# Closing the pig threads  
for thread in threads:
    if thread.isAlive():
        try:
            thread._Thread__stop()
        except:
            print(str(thread.getName()) + ' could not be terminated')
for thread in threads : 
        thread.join();        

# Closing the open sockets 
for i in range(0,npigs) : 
        if i not in failedpigs : 
            pigs[i].s.close() ; 

print ' All threads closed '