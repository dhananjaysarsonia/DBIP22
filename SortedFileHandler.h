//
//  SortedFileHandler.hpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#ifndef SortedFileHandler_h
#define SortedFileHandler_h

#include <stdio.h>
#include "FileHandler.h"
#include "HeapHandler.h"


class SortedFileHandler : public FileHandler {

private:
    
    OrderMaker *order;
    OrderMaker *query;
    
    BigQ *bigq;
    Pipe *inPipe, *outPipe;
                                     
    
    int runLength;
    int buffsize;

public:
    
    SortedFileHandler (OrderMaker *order, int runLength);
    ~SortedFileHandler ();
    
    int Create (const char *fpath);
    int Open (char *fpath);
    int Close ();
    
    void Load (Schema &myschema, const char *loadpath);
    
    void MoveFirst ();
    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    
    int GetNextWithCNF (Record &fetchme, CNF &cnf, Record &literal);
    
    int GetNextInSequence (Record &fetchme, CNF &cnf, Record &literal);
    
    int BinarySearchInSorted(Record &fetchme, CNF &cnf, Record &literal);
    
    void startSortingThread ();
    
    void FlushPipeToFile ();
    
    int NewOrderGenerator (OrderMaker &query, OrderMaker &order, CNF &cnf);
    
};









#endif /* SortedFileHandler_h */
