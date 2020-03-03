#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class RecordComparator {
	
private:
	
	OrderMaker *sortorder;
    
	
public:
	
	RecordComparator (OrderMaker *order);
	bool operator() (Record* left, Record* right);
	
};


class Run {
	
private:
	
	Page currentPage;
	
public:
	
	Record* currentRecord;
    OrderMaker* sortedOrder;
    File *runsFile;
	
    int pOffset;
    int runSize;
	
	Run (int runSize, int pageOffset, File *file, OrderMaker *order);
	Run (File *file, OrderMaker *order);
    ~Run();
    
    int GetFirstRecord();
	
};

class RunComparator {
	
private:
	
    OrderMaker *sortorder;
	
public:
	
    bool operator() (Run* left, Run* right);
	
}; 



class BigQ {

private:
	int maxPages;
    Pipe *inPipe;
    Pipe *outpipe;
    pthread_t workerThread;
    char *fileName;
    friend bool RecordComparator (Record* left, Record* right);
    priority_queue<Run*, vector<Run*>, RunComparator> priorityQueue;
    OrderMaker *sortorder;
	
	bool FlushRuns (int runLocation);
 
    

public:
	
	File runFile;
    vector<Record*> recordVector;
    int runlength;
	
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ () {};
	
	void RunGeneration();
    void RunMerge ();
	
    static void *StartMainThread (void *start) {
		
        BigQ *bigQ = (BigQ *)start;
        bigQ->RunGeneration();
        bigQ->RunMerge ();
        return 0;
		
    }
	
};

#endif
