#include "BigQ.h"

RecordComparator :: RecordComparator (OrderMaker *order) {
	
    sortorder = order;
    
	
}

bool RecordComparator::operator() (Record* left, Record* right) {
    
	ComparisonEngine engine;

    
    if (engine.Compare (left, right, sortorder) < 0) {
		
        return true;
		
	} else {
		
		return false;
		
	}
	
}

bool RunComparator :: operator() (Run* left, Run* right) {
	
    ComparisonEngine engine;
    
    if (engine.Compare (left->currentRecord, right->currentRecord, left->sortedOrder) < 0) {
		
        return false;
		
    } else {
		
        return true;
		
	}
	
}

/* Run Implementation */
Run :: Run (int run_length, int page_offset, File *file, OrderMaker* order) {
    
    runSize = run_length;
    pOffset = page_offset;
    currentRecord = new Record ();
    runsFile = file;
    sortedOrder = order;
    runsFile->GetPage (&currentPage, pOffset);
    GetFirstRecord ();
	
}

Run :: Run (File *file, OrderMaker *order) {
	
    currentRecord = NULL;
    runsFile = file;
    sortedOrder = order;
	
}

Run :: ~Run () {
	
    delete currentRecord;
	
}

int Run :: GetFirstRecord () {
    
    if(runSize <= 0) {
        return 0;
    }
    
    Record* record = new Record();
    
    // try to get the Record, get next page if necessary
    if (currentPage.GetFirst(record) == 0) {
        pOffset++;
        runsFile->GetPage(&currentPage, pOffset);
        currentPage.GetFirst(record);
    }
    
    runSize--;
    
    currentRecord->Consume(record);
    
    return 1;
}


bool BigQ :: FlushRuns (int runLocation) {
    
    
    
    Page* p = new Page();
    int listSize = recordVector.size();
    
    int pageOffset = maxPages;
    int counter = 1;
    
    for (int i = 0; i < listSize; i++) {
		
        Record* record = recordVector[i];
		
        if ((p->Append (record)) == 0) {
            
            counter++;
            
            runFile.AddPage (p, maxPages);
            maxPages++;
            p->EmptyItOut ();
            p->Append (record);
			
        }
		
        delete record;
		
    }
	
    runFile.AddPage(p, maxPages);
    maxPages++;
    p->EmptyItOut();
    
    recordVector.clear();
    delete p;
	
    
    Run* run = new Run(listSize, pageOffset, &runFile, sortorder);
    priorityQueue.push(run);
    
    return true;
	
}


void BigQ :: RunGeneration() {
	
    Page* p = new Page();
    int pCounter = 0;
    int runLocation = 0;
	
    Record* record = new Record();
	
    srand (time(NULL));
    fileName = new char[100];
    sprintf (fileName, "%d.txt", (int)(size_t)workerThread);
    
    runFile.Open (0, fileName);
    
    while (inPipe->Remove(record)) {
		
        Record* copyOfRecord = new Record ();
		copyOfRecord->Copy (record);
        
		
        
        if (p->Append (record) == 0) {
            
            pCounter++;
			
            if (pCounter == runlength) {
                
                sort (recordVector.begin (), recordVector.end (), RecordComparator (sortorder));
                runLocation = (runFile.GetLength () == 0) ? 0 : (runFile.GetLength () - 1);
                
                int recordListSize = recordVector.size ();
                
                FlushRuns(runLocation);
                
                pCounter = 0;
				
            }
			
            p->EmptyItOut ();
            p->Append (record);
			
        }
        
        recordVector.push_back (copyOfRecord);
        
    }
    
    
    // Last Run
    if(recordVector.size () > 0) {
		
        sort (recordVector.begin (), recordVector.end (), RecordComparator (sortorder));
        
        
        if(runFile.GetLength() == 0)
        {
            runLocation = 0;
        }
        else
        {
            runLocation = runFile.GetLength() - 1;
        }
        
        
        
        int recordListSize = recordVector.size ();
		
        FlushRuns(runLocation);
        
        p->EmptyItOut ();
		
    }
	
    delete record;
    delete p;
	
}

void BigQ :: RunMerge () {
    
    Run* run = new Run (&runFile, sortorder);
   
    
   
	
    while (!priorityQueue.empty ()) {
		
        Record* record = new Record ();
        run = priorityQueue.top ();
        priorityQueue.pop ();
            
        record->Copy (run->currentRecord);
        outpipe->Insert (record);
		
        if (run->GetFirstRecord () > 0) {
			
            priorityQueue.push(run);
        
		}
		
        delete record;
		
    }

    runFile.Close();
    remove(fileName);
    
    outpipe->ShutDown();
    delete run;
	
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	
    this->sortorder = &sortorder;
    inPipe = &in;
    outpipe = &out;
    runlength = runlen;
    maxPages = 1;
    
    pthread_create(&workerThread, NULL, StartMainThread, (void *)this);
	
}
