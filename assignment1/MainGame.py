'''
Created on Mar 1, 2013
@author: gouthamdl and siddharthct
'''
from Pig import Pig
import threading
import random 
from collections import defaultdict
import time

# Getting the number of pigs from user 
response = input('Enter the number of pigs in the network')
npigs = int(response)
# Assuming the number of stone blocks are half as the number of pigs
nstoneblocks = npigs/2

# Creating a configuration file 
ports = [] 
for x in range(0,npigs):
    t = random.randrange(1025,40000); 
    while ports.__contains__(t ):
        t = random.randrange(1025,40000); 
    ports.append(t) ;     

peerMap = dict()

# Mapping pigs to their logic neighbors in the peer to peer to network 
# Assuming a ring topology but no message passing is based on the topology 
for i in range(0,npigs):
    peerMap[ports[i]] = [ports[(i+1)%npigs]]   


gridsize = npigs*2 # Size of the grid
grid = [0]*gridsize
pigs = []
stones = [] 

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
    distance = random.randrange(0,gridsize)  
    while grid[distance]!=0:
        distance = random.randrange(0,gridsize) 
    grid[distance] = 1 
    pig = Pig(i,ports[i],peerMap[ports[i]],distance,stones,gridsize-1)
    pigs.append(pig)

# The hopcount is fixed for a given run . Hopcounts is assigned to the number of pigs just to make sure that all pigs recieve packets in csae of flooding 
hopcount = npigs ; 


# Defining the grid 
# 0 - defines blank 
# 1 - defines pig 
# 2 - defines stone block


print "Displaying the 1d grid \n0-unoccupied \n1-Pig \n2-stone \npositions denoted from 0 to " + str(gridsize-1) + " with 0 being on the left"
print grid

threads = []
# Start the main loop for each Pig which will handle all p2p communications for that pig
for i,p in enumerate(ports):
    t = threading.Thread(target=pigs[i].mainloop)
    t.start()
    threads.append(t)

print " Pig network set up, All pigs listening "
#Pigs sending their locations through the p2p network , in order to identify their physical neighbors
for pig in pigs : 
    pig.sendaddress(pig.position,hopcount)

# Set the start time for the Pigs
# Note that there is no global clock. The start time will be the current time on the system where the Pig object resides.
for pig in pigs:
    pig.setStartTime()

# The Bird's landing position and time taken is given below
birdposition = random.randrange(0,gridsize); 
currenttime = time.time() 
birdtime  = random.randrange(0,npigs*2);

print "Bird landing position :" + str(birdposition)
print "Time taken by the bird :" + str(birdtime)
print "Hop count for the system :" + str(hopcount)
print "Bird Approaching packets are being sent ... "
# Start sending the bird approaching packet to the network
# Pig 0 will send it to its neighboring peers, those peers in turn will send it to their peers and so on. 
path = [pigs[0].port]
pigs[0].birdapproaching(birdposition,hopcount,birdtime,path)

# Sleep till all the bird packets replies have been recieved
while pigs[0].birdpacketcount < npigs : 
    time.sleep(1)

print "All pigs have received the bird approaching packet " 
print "Status packets are being sent ... "
# Pig 0 queries the other Pigs for their status
path = [pigs[0].port]  
for pig in pigs: 
    pigs[0].status(pig.id,hopcount,path)   

# Wait until all the pigs have replied back with their status
while pigs[0].washitcount<npigs: 
    time.sleep(1)

# Print the Score
print "All pigs have received the Status packet " 
print 'Score based on the number of pigs killed'
print "SCORE : " + str(pigs[0].score) 

# Just to visually represent the results with the grid 
print "A visualization representing the result "
print "Displaying the 1d grid \n'0'-unoccupied \n'1'-Pig \n'2'-stone \n'D'-Deadpig \n'11'- Two pigs" 
finalgrid = ['0']*gridsize
for pig in pigs : 
    if(pig.hit==True):
        finalgrid[pig.position] = 'D' 
    elif(finalgrid[pig.position]=='0'):
        finalgrid[pig.position] = '1'
    else:
        finalgrid[pig.position] +='1'
         
for s in stones :
    if(birdposition == s):
        finalgrid[birdposition] = '0' ; 
    elif(finalgrid[s-1]=='D'):
        finalgrid[s] = '0' 
    else:
        finalgrid[s] = '2'
  
print finalgrid                 
