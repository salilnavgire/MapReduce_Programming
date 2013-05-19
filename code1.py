


#!/usr/bin/python

import sys
import pickle
import random
import time
import os
from multiprocessing import Pool
import cPickle
from time import gmtime, strftime



class Finance:
    Date = 0;
    Time = 0;
    Open = 0;
    High = 0;
    Low = 0;
    Close = 0;
    Volume = 0;
    Day = "";
    Week = 0;


    def __init__(self,Date,Time,Open,High,Low,Close,Volume,Day,Week):
        self.Date = Date
        self.Time = Time
        self.Open = Open
        self.High = High
        self.Low = Low
        self.Close = Close
        self.Volume = Volume
        self.Day = Day
        self.Week = Week



def parse(f):
    counter = 0;
    list = []
    a = []
    dt = []
    o = 0

    #f=open('sample.txt')

    #print 'L'
    for line in f:
        a=(line.split(','))
        #print 'k'
        if a[0] == '"Date"' :
            continue
        #print'm'
        dt = time.strptime(a[0], "%m/%d/%Y")
        #print dt[6]
        #print 'go'
        a.append(dt[6])
        o = strftime("%U", dt)
        a.append(o)
        #print 'yess'
        #print a[1]
        list.append(Finance(a[0],a[1],a[2],a[3],a[4],a[5],a[6],a[7],a[8]))
        dt = []
        a = []
    return list


def split_seq(seq, num_pieces):
    start = 0
    for i in xrange(num_pieces):
        stop = start + len(seq[i::num_pieces])
        yield seq[start:stop]
        start = stop

def chunks(l, n):
    i = 0
    return [l[i:i+n] for i in range(0, len(l), n)]

def chunk(input, size):
    #pool = Pool(8)
    return map(None, *([iter(input)] * size))

if __name__== '__main__':
    print 'start'
    num = 1
    numlines = 200
    list = []
    j=2
    L =[]
    G = []
    finl = []
    M = []
    buy = 0
    sell = 0
    profit = 0
    e = []
    s = 0
    lst_wk = 0
    pot = 0


    #list=parse('sample.txt')

    #print list[190].High
    #f=open('sample.txt')
    #f=open('AAPL.txt').readlines()
    #print f
    #list=parse(f)

     
    for i in range(1,9):
      list = []
    	L = []
    	G = []
		#list = []
	#	L = []
		#G = []

        with open('AAPL.txt','r') as g:
            f=g.readlines()
            #print len(f)
            t_start = time.time()
            pool = Pool(num)

            list = pool.map(parse,(f[line:line+numlines] for line in xrange(0,len(f),numlines) ))
            #print len(list)
            pool.close()
            pool.join()

            for j in range(0,len(list)):
                #print 'salll'
                for k in range(0,len(list[j])):
                    L.append(list[j][k])


            print len(L)

            #list = []
            #for L in split_seq(L, 1000):
            #print L 
            
            #pass

            G = chunks(L,100000)

            
            print len(G)

            #print G[0]

            k=0
            

            for k in range(0, len(G)):

            #pickle.dump(G[k], open( "test"+str(k)+".p", "wb" ))
                pickle.dump(G[k], open( "AAPL"+str(k)+".p", "wb" ))
            t_end = time.time()

        

    	print(num,'time used: ', t_end-t_start)
    	num = num +1

	#print 'done'


	
    print 'end'


