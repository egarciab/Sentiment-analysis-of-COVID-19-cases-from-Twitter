'''
#Vader greek lexicon, with a parallel for loop
v.3.3 multithread support
'''
from time import sleep
from random import random
from multiprocessing import Pool
import pickle
import threading
import time
import io
import os
#from threading import Thread
import warnings
warnings.filterwarnings('ignore')
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

multithreading=2 #0:single, 2:multi thread

class globals:
  def __init__(self):
    start
    lines_to_write
    sent
    final
    overall
    result=[]
  result2=[]
  total_len=0
  count=0

#task to execute in another process
def open_file():
    #ReadFile='DataGreekCovid_test_part.txt'
    global total_len
    global lines
    ReadFile='DataGreekCovid_test_part.txt'

    try:
      f = io.open(ReadFile, mode='r', encoding='utf-8',
                errors='ignore') # for text files
    except:
      print('error in reading')
    lines=f.readlines()
    total_len=len(lines)
    f.close()

    return total_len, lines
  
def sentiment_scores(sentence):    
    #Create a SentimentIntensityAnalyzer object. 
	sid_obj = SentimentIntensityAnalyzer()
	# This script uses polarity_scores method of SentimentIntensityAnalyzer
	# Includes positive, negative, neutral and compound scores. 
	sentiment_dict = sid_obj.polarity_scores(sentence)	        
	global sent
	global final
	global overall
	sent= sid_obj.polarity_scores(sentence)
	sent=(str(sent)).replace('\n', '')	
	#print (sent)     
	# Calculate sentiment as positive, negative and neutral 
	if sentiment_dict['compound'] >= 0.05 :
		final = 'Positive'
                #print("Positive")
	elif sentiment_dict['compound'] <= - 0.05 : 
		final = 'Negative' 
                #print("Negative")                
	else : 
		final = 'Neutral' 
                #print("Neutal")

def start_vader(arg):
        #start vader computing
        global sent
        global lines_to_write
        result=[]
        result2=[]
        count=0
        lines_to_write=[]
        for x in arg: #in all lines
            L=''
            result.append(x.split('\t')[2]) #2 for the third column
            result2.append(x.split('\t')[0]) #1 for the second column
            L= str(count + 1) + '\t' + result2[count].replace('\n', '') #if new line exists
            sentiment_scores (result[count])
            sent=(str(sent)).strip('\n')            
            L=L + '\t' + str(sent) + '\n'
            #ff.write(L+'\n')
            lines_to_write.append(L)
            count=count+1
            L=''            
        return lines_to_write

def calculate(arg):
  global value
  value+=1
  return value
  
#entry point for the program
if __name__ == '__main__':
    #print ('Number of active threads:', threading.activeCount())
           
    if multithreading==0: #signle thread
        print('single thread')
        print ('Number of active threads:', threading.activeCount())
        start = time.time()
        #1.open file
        open_file()
        print ('total records:', total_len)
        #2.calculate scores
        start_vader(lines)
        print (type(lines_to_write), len(lines_to_write))
        #3.write scores
        SaveFile='SentimentScore.txt'
        ff = open(SaveFile,'w')
        try:
          ff.write(''.join(lines_to_write))
          ff.close()
        except:
          print('error in writing')
        
    elif multithreading==1: #multi thread one job
        pool= Pool(6)
        print('multi thread:')
        print ('Number of active threads:', threading.activeCount())
        #1.open file
        start = time.time()
        open_file()
        print('total records:',total_len, len(lines)) #len(lines)
        lines_to_test=[]
        lines_to_test.append(lines)
        lines_to_write=[]
        #2.calculate scores
        with pool as pool:
          #result=pool.apply_async(start_vader(1), lines)
          result=pool.map(start_vader, lines_to_test)
          #alternatives: pool.apply_async, .starmap, .map_async, .imap
        pool.close()
        pool.join()
        #value = result.get()
        print (type(result), len(result))
        str_result2=''.join(result[0]) #convert to string
        #str_result2=str_result.split('}')
        print('str len:', len(str_result2.split('}'))-1)
        #3.write scores
        SaveFile='SentimentScore_parallel.txt'
        ff = open(SaveFile,'w')
        try:
          ff.write(str_result2)
          ff.close()
        except:
          print('error in writing')

    elif multithreading==2: #multi thread: many jobs
        pool= Pool(6)
        print('multi thread:')
        print ('Number of active threads:', threading.activeCount())
        #1.open file
        start = time.time()
        open_file()
        print('total records:',total_len, len(lines)) #len(lines)
        lines_to_test=[]
        lines_to_test.append(lines)
        lines_to_write=[]
        line_count=0
        #2.calculate scores
        with pool as pool:
        #call the same function with different data in parallel
          for i in range(0, len(lines_to_test)):
            line_test=lines_to_test[i]
            #print(line_test)
            result=pool.map(start_vader, [line_test])
            # alternatives: .map, .apply (only one) 
            lines_to_write.append(result)
        #pool.close()
        #pool.join()
        #value = result.get()
        print (type(lines_to_write), len(lines_to_write))
        str_result2=''.join(result[0]) #convert to string
        #str_result2=str_result.split('}')
        print('str len:', len(str_result2.split('}'))-1)
        #3.write scores
        SaveFile='SentimentScore_parallel.txt'
        ff = open(SaveFile,'w')
        try:
          ff.write(str_result2)
          ff.close()
        except:
          print('error in writing')
    
    end = time.time()
    print('total time:', round((end - start),4) , 'sec')
    print('--------------------------------------------')
    input('press any key to exit')
