//
//  HeapHandler.cpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include "HeapHandler.h"

HeapHandler :: HeapHandler () {
    
    file = new File ();
    currentPage = new Page ();
    
}

HeapHandler :: ~HeapHandler () {
    
    delete currentPage;
    delete file;
    
}

int HeapHandler :: Create (const char *fpath) {
    
    writingMode = true;
    
    this->fpath = new char[100];
    strcpy (this->fpath, fpath);
    
    pIndex = 0;
    currentPage->EmptyItOut ();
    file->Open (0, this->fpath);
    
    return 1;
    
}

void HeapHandler :: Load (Schema &f_schema, const char *loadpath) {
    
    FILE *tblfile = fopen(loadpath, "r");
    Record temp;
    
    if (tblfile == NULL) {
        
        cout << "Can not open file " << loadpath << "!" << endl;
        exit (0);
        
    }
    
    pIndex = 0;
    currentPage->EmptyItOut ();
    
    while (temp.SuckNextRecord (&f_schema, tblfile) == 1)
        Add (temp);
    
    file->AddPage (currentPage, pIndex);
    currentPage->EmptyItOut ();
    
    fclose (tblfile);
    
}

int HeapHandler :: Open (char *fpath) {
    
    this->fpath = new char[100];
    
    strcpy (this->fpath, fpath);
    pIndex = 0;
    
    currentPage->EmptyItOut ();
    file->Open (1, this->fpath);
    
    return 1;
    
}

void HeapHandler :: MoveFirst () {
    
    if (writingMode && currentPage->GetNumRecs () > 0) {
        
        file->AddPage (currentPage, pIndex++);
        writingMode = false;
        
    }
    
    currentPage->EmptyItOut ();
    pIndex = 0;
    file->GetPage (currentPage, pIndex);
    
}

int HeapHandler :: Close () {
    
    if (writingMode && currentPage->GetNumRecs () > 0) {
        
        file->AddPage (currentPage, pIndex++);
        currentPage->EmptyItOut ();
        writingMode = false;
        
    }
    
    file->Close ();
    
    return 1;
    
}

void HeapHandler :: Add (Record &rec) {
    
    if (! (currentPage->Append (&rec))) {
        
        file->AddPage (currentPage, pIndex++);
        currentPage->EmptyItOut ();
        currentPage->Append (&rec);
        
    }
    
}

int HeapHandler :: GetNext (Record &fetchme) {
    
    if (currentPage->GetFirst (&fetchme)) {
        return 1;
        
    } else {

        pIndex++;
        if (pIndex < file->GetLength () - 1) {
            
            file->GetPage (currentPage, pIndex);
            currentPage->GetFirst (&fetchme);
            
            return 1;

        } else {
            return 0;
            
        }
        
    }
    
}

int HeapHandler :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    
    ComparisonEngine comp;
    
    while (GetNext (fetchme)) {
        
        if (comp.Compare (&fetchme, &literal, &cnf)){
            
            return 1;
            
        }
        
    }
    
    return 0;
    
}
