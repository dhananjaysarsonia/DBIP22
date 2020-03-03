//
//  SortedFileHandler.cpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include "SortedFileHandler.h"



SortedFileHandler :: SortedFileHandler (OrderMaker *order, int runLength) {
    
    this->query = NULL;
    this->bigq = NULL;
    this->file = new File ();
    this->currentPage = new Page ();
    this->order = order;
    this->runLength = runLength;
    this->buffsize = 100;
    
}

SortedFileHandler :: ~SortedFileHandler () {
    
    delete query;
    delete file;
    delete currentPage;
    
}

int SortedFileHandler :: Create (const char *fpath) {
    
    writingMode = false;
    pIndex = 0;
    
    this->fpath = new char[100];
    strcpy (this->fpath, fpath);
    
    currentPage->EmptyItOut ();
    file->Open (0, this->fpath);
    
    return 1;
    
}

int SortedFileHandler :: Open (char *fpath) {
    
    writingMode = false;
    pIndex = 0;
    
    this->fpath = new char[100];
    strcpy (this->fpath, fpath);
    
    currentPage->EmptyItOut ();
    file->Open (1, this->fpath);
    
    if (file->GetLength () > 0) {
        
        file->GetPage (currentPage, pIndex);
        
    }
    
    
    return 1;
    
}

int SortedFileHandler :: Close () {
    
    if (writingMode) {
        
        FlushPipeToFile ();
        
    }
    
    file->Close ();
    return 1;
    
}

void SortedFileHandler :: Add (Record &addme) {
    
    if (!writingMode) {
        
        startSortingThread();
        
    }
    
    inPipe->Insert (&addme);
    
}

void SortedFileHandler :: Load (Schema &myschema, const char *loadpath) {
    
    FILE *tableFile = fopen(loadpath, "r");
    Record temp;
    
    if (tableFile == NULL) {
        
        cout << "Can not open file " << loadpath << "!" << endl;
        exit (0);
        
    }
    
    pIndex = 0;
    currentPage->EmptyItOut ();
    
    while (temp.SuckNextRecord (&myschema, tableFile)) {
        
        Add (temp);
        
    }
    
    fclose (tableFile);
    
}

void SortedFileHandler :: MoveFirst () {
    
    if (writingMode) {
        
        FlushPipeToFile ();
        
    } else {
        
        currentPage->EmptyItOut ();
        pIndex = 0;
        
        if (file->GetLength () > 0) {
            
            file->GetPage (currentPage, pIndex);
            
        }
        
        if (query) {
            
            delete query;
            
        }
        
    }
    
}

int SortedFileHandler :: GetNext (Record &fetchme) {
    
    if (writingMode) {
        
        FlushPipeToFile ();
        
    }
    
    if (currentPage->GetFirst (&fetchme)) {
        return 1;
        
    } else {
        pIndex++;
        if (pIndex < file->GetLength () - 1) {
            file->GetPage (currentPage, pIndex);
            currentPage->GetFirst (&fetchme);
            
            return 1;

        } else {
            // if already reach EOF
            return 0;
            
        }
        
    }
    
}

int SortedFileHandler :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    
    if (writingMode) {
        
        FlushPipeToFile ();
        
    }
    

    
    if (!query) {

        query = new OrderMaker;
        
        if (NewOrderGenerator (*query, *order, cnf) > 0) {
        
            query->Print ();
            if (BinarySearchInSorted (fetchme, cnf, literal)) {
    
                return 1;
                
            } else {
    
                return 0;
                
            }
            
        } else {

            return GetNextInSequence(fetchme, cnf, literal);
            
        }
        
    } else {
        
        if (query->numAtts == 0) {
            // invalid query
            return GetNextInSequence (fetchme, cnf, literal);
            
        } else {
            // valid query
            return GetNextWithCNF (fetchme, cnf, literal);
            
        }
        
    }
    
}

int SortedFileHandler :: GetNextWithCNF(Record &fetchme, CNF &cnf, Record &literal) {
    
    ComparisonEngine engine;
    
    while (GetNext (fetchme)) {
        
        if (!engine.Compare (&literal, query, &fetchme, order)){
            
            if (engine.Compare (&fetchme, &literal, &cnf)){
                
                return 1;
                
            }
        
        } else {
            
            break;
            
        }
        
    }
    
    return 0;
    
}

int SortedFileHandler :: GetNextInSequence(Record &fetchme, CNF &cnf, Record &literal) {
    
    ComparisonEngine engine;
    
    while (GetNext (fetchme)) {
        
        if (engine.Compare (&fetchme, &literal, &cnf)){
            
            return 1;
            
        }
        
    }
    
    return 0;
    
}

int SortedFileHandler :: BinarySearchInSorted(Record &fetchme, CNF &cnf, Record &literal) {
    
    off_t start = pIndex;
    off_t end = file->GetLength () - 1;
    off_t mid = pIndex;
    
    Page *page = new Page;
    
    ComparisonEngine engine;
    
    while (true) {
        
        mid = (start + end) / 2;
        
        file->GetPage (page, mid);
        
        if (page->GetFirst (&fetchme)) {
            
            if (engine.Compare (&literal, query, &fetchme, order) <= 0) {
                
                end = mid - 1;
                if (end <= start) break;
                
            } else {
                
                start = mid + 1;
                if (end <= start) break;
                
            }
            
        } else {
            
            break;
            
        }
        
    }
    
    if (engine.Compare (&fetchme, &literal, &cnf)) {
        
        delete currentPage;
        
        pIndex = mid;
        currentPage = page;
        
        return 1;
        
    } else {
    
        delete page;
        
        return 0;
        
    }
    
}

void SortedFileHandler :: startSortingThread() {
    
    writingMode = true;
    
    inPipe = new Pipe (buffsize);
    outPipe = new Pipe (buffsize);
    
    bigq = new BigQ(*inPipe, *outPipe, *order, runLength);
    
}

void SortedFileHandler :: FlushPipeToFile () {
    
    inPipe->ShutDown ();
    
    writingMode = false;
    
    if (file->GetLength () > 0) {
        
        MoveFirst ();
        
    }
    
    Record *recFromPipe = new Record;
    Record *recFromFile = new Record;
    
    HeapHandler *newFile = new HeapHandler;
    newFile->Create ("bin/temp.bin");
    
    int flagPipe = outPipe->Remove (recFromPipe);
    int flagFile = GetNext (*recFromFile);
    
    ComparisonEngine comp;
    
    while (flagFile && flagPipe) {
        
        if (comp.Compare (recFromPipe, recFromFile, order) > 0) {
            
            newFile->Add (*recFromFile);
            flagFile = GetNext (*recFromFile);
            
        } else {
            
            newFile->Add (*recFromPipe);
            flagPipe = outPipe->Remove (recFromPipe);
            
        }
        
    }
    
    while (flagFile) {
        
        newFile->Add (*recFromFile);
        flagFile = GetNext (*recFromFile);
        
    }
    
    while (flagPipe) {
        
        newFile->Add (*recFromPipe);
        flagPipe = outPipe->Remove (recFromPipe);
        
    }
    
    outPipe->ShutDown ();
    newFile->Close ();
    delete newFile;
    
    file->Close ();
    
    remove (fpath);
    rename ("bin/temp.bin", fpath);
    
    file->Open (1, fpath);
    
    MoveFirst ();
    
}

int SortedFileHandler :: NewOrderGenerator(OrderMaker &query, OrderMaker &order, CNF &cnf) {
    //Query was failing order generation with default method provided
    //After a lot of discussion with classmates, we have made some changes and pasted here
    
    query.numAtts = 0;
    bool found = false;
    
    for (int i = 0; i < order.numAtts; ++i) {
        
        
        for (int j = 0; j < cnf.numAnds; ++j) {
            
            if (cnf.orLens[j] != 1) {
                
                continue;
                
            }
            
            if (cnf.orList[j][0].op != Equals) {
                
                continue;
                
            }
            
            if ((cnf.orList[i][0].operand1 == Left && cnf.orList[i][0].operand2 == Left) ||
               (cnf.orList[i][0].operand2 == Right && cnf.orList[i][0].operand1 == Right) ||
               (cnf.orList[i][0].operand1==Left && cnf.orList[i][0].operand2 == Right) ||
               (cnf.orList[i][0].operand1==Right && cnf.orList[i][0].operand2 == Left)) {
                
                continue;
                
            }

            
            if (cnf.orList[j][0].operand1 == Left &&
                cnf.orList[j][0].whichAtt1 == order.whichAtts[i]) {
                
                query.whichAtts[query.numAtts] = cnf.orList[i][0].whichAtt2;
                query.whichTypes[query.numAtts] = cnf.orList[i][0].attType;
                
                query.numAtts++;
                
                found = true;
                
                break;
                
            }
            
            if (cnf.orList[j][0].operand2 == Left &&
                cnf.orList[j][0].whichAtt2 == order.whichAtts[i]) {
                
                query.whichAtts[query.numAtts] = cnf.orList[i][0].whichAtt1;
                query.whichTypes[query.numAtts] = cnf.orList[i][0].attType;
                
                query.numAtts++;
                
                found = true;
                
                break;
                
            }
            
        }
        
        if (!found) {
            
            break;
            
        }
        
    }
    
    return query.numAtts;
    
}



