//
//  Run.cpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include "Run.h"


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
