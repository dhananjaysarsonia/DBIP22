#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    
    this->sortorder = &sortorder;
    inPipe = &in;
    outpipe = &out;
    runlength = runlen;
    maxPages = 1;
    
    pthread_create(&workerThread, NULL, StartMainThread, (void *)this);
    
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
