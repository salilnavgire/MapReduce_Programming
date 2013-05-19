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


def func_buy(M):
    buy = 0
    for w in range (0,len(M)):
        if M[w].Day == 1:
            #print finl[w].Time
            if M[w].Time == '1000' :
                if M[w].Close == '' :
                    count = count +1
                    continue

                #print finl[w].Close
                #buy = finl[w].Close
                #print float(buy)
                #buy = finl[w]
                #print finl[w].Date
                #print finl[w].Time
                #print buy
                buy = buy + float(M[w].Close)
    return buy


def func_sell(M):
    count = 0
    lst_wk = 0
    sell = 0
    for w in range (0,len(M)):
        e = int(M[w].Day) - int(M[w-1].Day)
        #print e

        #if M[w].Day == 0:
            #if M[w].Day == 1:

        #if (int(M[w].Week) - int(M[w-1].Week)) != 0:
         #   print (M[w].Week,M[w-1].Week)


        if M[w].Day == 2:
            #print finl[w].Time
            #print M[w].Week
            if M[w].Time == '1200' :
                #print 'w',w
                #print M[w].Date,M[w].Week,lst_wk
                pot = 0

                if (int(M[w].Week) - lst_wk)>1:
                    sell = sell + float(M[w].Close)
                    #print lst_wk

                    #print ("error",M[w].Week,lst_wk)
                    for z in range (0,len(M)):
                        #print lst_wk+1,M[z].Week
                        if pot == 0:
                            if int(M[z].Week) == lst_wk + 1:
                                #print '2dddd'
                                if int(M[z].Day) > 1:
                                    #print '3'
                                    if M[z].Time == '1200':
                                        print M[z].Date,M[z].Time,M[z].Week,M[z].Close
                                        sell = sell + float(M[z].Close)
                                        print sell
                                        pot = 1
                                        #print sell
                                        #print 'done',w
                                        #pass
                else:
                    sell = sell + float(M[w].Close)
                    #print 'w',w


                if M[w].Close == '' :
                    count = count + 1
                    continue
                    #print finl[w].Close
                #print sell
                lst_wk = int(M[w].Week)

    return sell


if __name__== '__main__':
    print 'start'
    num = 1
    numlines = 200
    list = []
    j=2
    L =[]
    G = []
    finl = []
    AAPL = []
    AMZN = []
    BBY = []
    buy = 0
    buy1 = 0
    sell = 0
    sell1 = 0
    profit = 0
    e = []
    s = 0
    lst_wk = 0
    pot = 0
    full = []
    x = []



    print 'unpickling'


# 12 chunks for sample1 


    #for k in range(0,12):
     #   v = open("test"+str(k)+".p", "rb")
      #  finl.append(pickle.load( v ))

    AAPL = pickle.load(open("AAPL.p","rb"))
    AMZN = pickle.load(open("AMZN.p","rb"))
    BBY = pickle.load(open("BBY.p","rb"))



    
    #AAPL = pickle.load(open("AAPL.p","rb"))
    #AMZN = pickle.load(open("AMZN.p","rb"))
    #BBY = pickle.load(open("BBY.p","rb"))

    #AMZN = 



    #M = finl

    print len(full)

    #for j in range(0,len(finl)):
        #print 'salll'
    #    for k in range(0,len(finl[j])):
     #       M.append(finl[j][k])

    






    print len(AAPL)
    print len(AMZN)
    print len(BBY)

    print BBY[1].Date

    count = 0

    # 0 is monday and so on
    for num in range(1,7):
        t_start = time.time()
        list1 = []
        list2 = []
        list3 = []
        j=2
        L =[]
        G = []
        #finl = []
        #M = []
        buy = 0
        buy1 = 0
        sell = 0
        sell1 = 0
        profit = 0
        e = []
        s = 0
        lst_wk = 0
        pot = 0

        list1 = []
        list2 = []
        list3 = []
        pool = Pool(num)

        #buy1 = func_buy(M)
        #print "buy is   asdasd ",buy1
        list1 = pool.map(func_buy,(AAPL[line:line+numlines] for line in xrange(0,len(AAPL),numlines) ))
        pool.close()
        pool.join()
        pool = Pool(num)

        list2 = pool.map(func_buy,(AMZN[line:line+numlines] for line in xrange(0,len(AMZN),numlines) ))
        pool.close()
        pool.join()
        pool = Pool(num)

        list3 = pool.map(func_buy,(BBY[line:line+numlines] for line in xrange(0,len(BBY),numlines) ))
        pool.close()
        pool.join()
        pool = Pool(num)

        #print "buy  is ",sum(list)
        buy1 = (sum(list1)) + (sum(list2)) + (sum(list3))


        print "buy is ",buy1
        #print "missed buys is", count
        #print ('last week is',lst_wk)

        pool = Pool(num)
        list1 = []
        list2 = []
        list3 = []

        #sell1 = func_sell(M)
        #print "sell is",sell1
        list1 = pool.map(func_sell,(AAPL[line:line+numlines] for line in xrange(0,len(AAPL),numlines) ))
        pool.close()
        pool.join()
        pool = Pool(num)

        list2 = pool.map(func_sell,(AMZN[line:line+numlines] for line in xrange(0,len(AMZN),numlines) ))
        pool.close()
        pool.join()
        pool = Pool(num)

        list3 = pool.map(func_sell,(BBY[line:line+numlines] for line in xrange(0,len(BBY),numlines) ))
        pool.close()
        pool.join()
        pool = Pool(num)
        #print "sum is",sum(list)
        sell1 = (sum(list1)) + (sum(list2)) + (sum(list3))
        print "sum is",sell1

            


            #if e != 0:
            #print ('error',M[w-1].Day,M[w-1].Date,M[w].Day,M[w].Date)


        #print "gogogogo"
        #e = [float(i) for i in e]
        #s = sum(e)
        #print "e is",s
        #print sell1
        #print "missed sell is", count

        profit = sell1 - buy1

        print "profit is",profit

                #print L[2]

                #print len(L)
                #pickle.dump(L, open( "lola"+str(j)+".p", "wb" ))

                #print list[0]
        t_end = time.time()
        print(num,'time used: ', t_end-t_start)
        list1 = []
        list2 = []
        list3 = []

                #num *= 2
    #pickle.dump(list, open( "pickle.p", "wb" ))

    #print len(list)
    #print list[1][0].High
    print 'end'





