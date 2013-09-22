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

iteration = 1;  
continueIteration = True; 
olderport = [] ;
response = input('Enter the number of pigs in the network : ')
npigs = int(response)
# Assuming the number of stone blocks are half as the number of pigs
nstoneblocks = npigs/2

while continueIteration == True:
    
    # Creating ports for the pigs 
    ports = [] 
    for x in range(0,npigs):
        t = random.randrange(1025,40000); 
        while ports.__contains__(t ) or olderport.__contains__(t):
            t = random.randrange(1025,40000); 
        ports.append(t) ; 
        olderport.append(t) ; 

    print 'Some pigs might fail in the network at random ' ; 
    print 'the pigs that failed in the current iteration are : ' ; 
    # The number of failures every iteration is calculated 
    nfail = random.randrange(0,npigs//2)
    failedpigs = [] ; 
    for i in range(0,nfail): 
        x = random.randrange(0,npigs); 
        while failedpigs.__contains__(x):
            x = random.randrange(0,npigs); 
        failedpigs.append(x);   
    print failedpigs ; 
    
    # Defining the grid 
    gridsize = npigs*2 # Size of the grid
    grid = [0]*gridsize
    pigs = []
    stones = []
    print 'Iteration : ' + str(iteration);
    
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
            peerMap = dict()
            for j in range(0,npigs): 
                if i!=j :
                    if j not in failedpigs : 
                        peerMap[j] = ports[j] 
            pig = Pig(i,ports[i],peerMap,distance,stones,gridsize-1,npigs,failedpigs)
            pigs.append(pig)
        else : 
            # All the failed pigs are given no ports and are made dormamnt 
            peerMap = dict(); 
            pig = Pig(i,0,peerMap,0,[],gridsize-1,npigs,failedpigs)
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
        
    # LEADER ELECTION 
    print 'Leader election using the bully algorithm '
    current_pig = random.randrange(0,npigs); # This pig starts the leader election algorithm 
    while current_pig in failedpigs :
        current_pig = random.randrange(0,npigs);  
    print "Pig which starts the election = pig with id " + str(current_pig); 
    pigs[current_pig].startelection(); 
    
    # Timeout for the leader to be elected ( The time for the election to complete ) 
    sleep_time = (npigs-len(failedpigs))*(npigs-len(failedpigs));
    time.sleep(sleep_time*0.1);
   
    # Checking if the pig received the Ok message in the due course of the election 
    if pigs[current_pig].okmessagereceived== False :
        pigs[current_pig].sendIwonMessage(); 
        pigs[current_pig].setLeaderID(current_pig)
    
    # PRinting the leader election results only after all the pigs know the leader  
    while pigs[current_pig].getLeaderID() == -1:
        time.sleep(0.5);
    
    leader = pigs[pigs[current_pig].getLeaderID()]
    print 'LEADER ELECTED :' + str(leader.id); 
                    
    # Now we wait until all the pigs have sent their position to the leader
    while not leader.hasRecvdPos():
        time.sleep(1)
    
    birdtime = random.randrange(1,2*npigs); 
    birdposition = random.randrange(0,gridsize);
    # The Bird's landing position and time taken is given below
    print "Bird landing position :" + str(birdposition)
    print "Time taken by the bird :" + str(birdtime)
    
    print "The Leader gets the bird time and bird landing positions and find the affected pig "
    leader.birdapproaching(birdposition, birdtime)
    
    print 'Waiting till the leader has received all the statuses'
    while not leader.hasRecvdScores():
        time.sleep(0.1)
    
    print 'Score : ' + str(leader.getScore())
    
    print "A visualization representing the result "
    print "Displaying the 1d grid \n'0'-unoccupied \n'1'-Pig \n'2'-stone \n'D'-Deadpig \n'11'- Two pigs" 
    finalgrid = ['0']*gridsize
    for pig in pigs : 
        if pig.id not in failedpigs : 
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
    
    # Closing all the threads opened for sending messages without blocking at the pigs 
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
    
    
    exitresponse = raw_input('y to continue / n to exit' ); 
    if exitresponse == 'n':
        continueIteration = False ;  
        print ' Done'  
        sys.exit()
         
    else :
        iteration+=1 ; 
           
