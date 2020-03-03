#include <string>
#include <cstring>
#include <fstream>
#include <iostream>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "SortedFileHandler.h"

//FileHandler :: ~FileHandler () {}

DBFile :: DBFile () {}

DBFile :: ~DBFile () {
	
	delete fileHandler;
	
}

int DBFile :: Create (const char *fpath, fType f_type, void *startup) {
	
	ofstream metadata;
	
	char metadataPath[100];
	
	sprintf (metadataPath, "%s.md", fpath);
	metadata.open (metadataPath);
    
    switch (f_type) {
        case heap:
        {
            metadata << "heap" << endl;
            fileHandler = new HeapHandler;
            break;
        }
          
            
        case sorted:
        {
            metadata << "sorted" << endl;
            metadata << ((SortBus *) startup)->runLength << endl;
            metadata << ((SortBus *) startup)->myOrder->numAtts << endl;
            ((SortBus *) startup)->myOrder->PrintInOfstream (metadata);
            fileHandler = new SortedFileHandler (((SortBus *) startup)->myOrder, ((SortBus *) startup)->runLength);
            break;
            
        }
        default:
            metadata.close();
            return 0;
    }
    
    
    

	
	fileHandler->Create (fpath);
	metadata.close ();
	
	return 1;
	
}

int DBFile :: Open (char *filepath) {
	
	ifstream metadata;
	string inputString;
	
	int attNum;
	char *mdpath = new char[100];
	
	sprintf (mdpath, "%s.md", filepath);
	metadata.open (mdpath);
	
	if (metadata.is_open ()) {
		
		metadata >> inputString;
		
		if (inputString.compare ("heap") == 0) {
			
			fileHandler = new HeapHandler;
			
		} else if (inputString.compare ("sorted") == 0){
			
			int runLength;
			
			OrderMaker *order = new OrderMaker;
			
			metadata >> runLength;
			metadata >> order->numAtts;
			
			for (int i = 0; i < order->numAtts; i++) {
				
				metadata >> attNum;
				metadata >> inputString;
				
				order->whichAtts[i] = attNum;
				
				if (!inputString.compare ("Int")) {
					
					order->whichTypes[i] = Int;
					
				} else if (!inputString.compare ("Double")) {
					
					order->whichTypes[i] = Double;
					
				} else if (!inputString.compare ("String")) {
					
					order->whichTypes[i] = String;
					
				} else {
					
					delete order;
					
					metadata.close ();
					
					cout << "Bad Data! Some error occurred (" << filepath << ")" << endl;
					
					return 0;
					
				}
				
			}
			
			fileHandler = new SortedFileHandler (order, runLength);
			
			
		} else {
			
			metadata.close ();
			
			cout << "Bad file type, it should be sorted or heap only(" << filepath << ")" << endl;
			
			return 0;
			
		}
		
	} else {
		
		metadata.close ();
		
		cout << "Check the path, there is some error (" << filepath << ")!" << endl;
		return 0;
		
	}
	
	fileHandler->Open (filepath);
	metadata.close ();
	
	return 1;
	
}

int DBFile :: Close () {
	
	return fileHandler->Close ();
	
}

void DBFile :: Load (Schema &myschema, const char *loadpath) {
	
	fileHandler->Load (myschema, loadpath);
	
}

void DBFile :: MoveFirst () {
	
	fileHandler->MoveFirst ();
	
}

void DBFile :: Add (Record &addme) {
	
	fileHandler->Add (addme);
	
}

int DBFile :: GetNext (Record &fetchme) {
	
	return fileHandler->GetNext (fetchme);
	
}

int DBFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	
	return fileHandler->GetNext (fetchme, cnf, literal);
	
}

//HeapHandler :: HeapHandler () {
//
//	file = new File ();
//	currentPage = new Page ();
//
//}
//
//HeapHandler :: ~HeapHandler () {
//
//	delete currentPage;
//	delete file;
//
//}
//
//int HeapHandler :: Create (const char *fpath) {
//
//	writingMode = true;
//
//	this->fpath = new char[100];
//	strcpy (this->fpath, fpath);
//
//	pIndex = 0;
//	currentPage->EmptyItOut ();
//	file->Open (0, this->fpath);
//
//	return 1;
//
//}
//
//void HeapHandler :: Load (Schema &f_schema, const char *loadpath) {
//
//	FILE *tblfile = fopen(loadpath, "r");
//	Record temp;
//
//	if (tblfile == NULL) {
//
//		cout << "Can not open file " << loadpath << "!" << endl;
//		exit (0);
//
//	}
//
//	pIndex = 0;
//	currentPage->EmptyItOut ();
//
//	while (temp.SuckNextRecord (&f_schema, tblfile) == 1)
//		Add (temp);
//
//	file->AddPage (currentPage, pIndex);
//	currentPage->EmptyItOut ();
//
//	fclose (tblfile);
//
//}
//
//int HeapHandler :: Open (char *fpath) {
//
//	this->fpath = new char[100];
//
//	strcpy (this->fpath, fpath);
//	pIndex = 0;
//
//	currentPage->EmptyItOut ();
//	file->Open (1, this->fpath);
//
//	return 1;
//
//}
//
//void HeapHandler :: MoveFirst () {
//
//	if (writingMode && currentPage->GetNumRecs () > 0) {
//
//		file->AddPage (currentPage, pIndex++);
//		writingMode = false;
//
//	}
//
//	currentPage->EmptyItOut ();
//	pIndex = 0;
//	file->GetPage (currentPage, pIndex);
//
//}
//
//int HeapHandler :: Close () {
//
//	if (writingMode && currentPage->GetNumRecs () > 0) {
//
//		file->AddPage (currentPage, pIndex++);
//		currentPage->EmptyItOut ();
//		writingMode = false;
//
//	}
//
//	file->Close ();
//
//	return 1;
//
//}
//
//void HeapHandler :: Add (Record &rec) {
//
//	if (! (currentPage->Append (&rec))) {
//
//		file->AddPage (currentPage, pIndex++);
//		currentPage->EmptyItOut ();
//		currentPage->Append (&rec);
//
//	}
//
//}
//
//int HeapHandler :: GetNext (Record &fetchme) {
//
//	if (currentPage->GetFirst (&fetchme)) {
//		return 1;
//
//	} else {
//
//		pIndex++;
//		if (pIndex < file->GetLength () - 1) {
//
//			file->GetPage (currentPage, pIndex);
//			currentPage->GetFirst (&fetchme);
//
//			return 1;
//
//		} else {
//			return 0;
//
//		}
//
//	}
//
//}
//
//int HeapHandler :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
//
//	ComparisonEngine comp;
//
//	while (GetNext (fetchme)) {
//
//		if (comp.Compare (&fetchme, &literal, &cnf)){
//
//			return 1;
//
//		}
//
//	}
//
//	return 0;
//
//}
//
//SortedFileHandler :: SortedFileHandler (OrderMaker *order, int runLength) {
//
//	this->query = NULL;
//	this->bigq = NULL;
//	this->file = new File ();
//	this->currentPage = new Page ();
//	this->order = order;
//	this->runLength = runLength;
//	this->buffsize = 100;
//
//}
//
//SortedFileHandler :: ~SortedFileHandler () {
//
//	delete query;
//	delete file;
//	delete currentPage;
//
//}
//
//int SortedFileHandler :: Create (const char *fpath) {
//
//	writingMode = false;
//	pIndex = 0;
//
//	this->fpath = new char[100];
//	strcpy (this->fpath, fpath);
//
//	currentPage->EmptyItOut ();
//	file->Open (0, this->fpath);
//
//	return 1;
//
//}
//
//int SortedFileHandler :: Open (char *fpath) {
//
//	writingMode = false;
//	pIndex = 0;
//
//	this->fpath = new char[100];
//	strcpy (this->fpath, fpath);
//
//	currentPage->EmptyItOut ();
//	file->Open (1, this->fpath);
//
//	if (file->GetLength () > 0) {
//
//		file->GetPage (currentPage, pIndex);
//
//	}
//
//
//	return 1;
//
//}
//
//int SortedFileHandler :: Close () {
//
//	if (writingMode) {
//
//		FlushPipeToFile ();
//
//	}
//
//	file->Close ();
//    return 1;
//
//}
//
//void SortedFileHandler :: Add (Record &addme) {
//
//	if (!writingMode) {
//
//		startSortingThread();
//
//	}
//
//	inPipe->Insert (&addme);
//
//}
//
//void SortedFileHandler :: Load (Schema &myschema, const char *loadpath) {
//
//	FILE *tableFile = fopen(loadpath, "r");
//	Record temp;
//
//	if (tableFile == NULL) {
//
//		cout << "Can not open file " << loadpath << "!" << endl;
//		exit (0);
//
//	}
//
//	pIndex = 0;
//	currentPage->EmptyItOut ();
//
//	while (temp.SuckNextRecord (&myschema, tableFile)) {
//
//		Add (temp);
//
//	}
//
//	fclose (tableFile);
//
//}
//
//void SortedFileHandler :: MoveFirst () {
//
//	if (writingMode) {
//
//		FlushPipeToFile ();
//
//	} else {
//
//		currentPage->EmptyItOut ();
//		pIndex = 0;
//
//		if (file->GetLength () > 0) {
//
//			file->GetPage (currentPage, pIndex);
//
//		}
//
//		if (query) {
//
//			delete query;
//
//		}
//
//	}
//
//}
//
//int SortedFileHandler :: GetNext (Record &fetchme) {
//
//	if (writingMode) {
//
//		FlushPipeToFile ();
//
//	}
//
//	if (currentPage->GetFirst (&fetchme)) {
//		return 1;
//
//	} else {
//		pIndex++;
//		if (pIndex < file->GetLength () - 1) {
//			file->GetPage (currentPage, pIndex);
//			currentPage->GetFirst (&fetchme);
//
//			return 1;
//
//		} else {
//			// if already reach EOF
//			return 0;
//
//		}
//
//	}
//
//}
//
//int SortedFileHandler :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
//
//	if (writingMode) {
//
//		FlushPipeToFile ();
//
//	}
//
//
//
//	if (!query) {
//
//		query = new OrderMaker;
//
//		if (NewOrderGenerator (*query, *order, cnf) > 0) {
//
//			query->Print ();
//			if (BinarySearchInSorted (fetchme, cnf, literal)) {
//
//				return 1;
//
//			} else {
//
//				return 0;
//
//			}
//
//		} else {
//
//			return GetNextInSequence(fetchme, cnf, literal);
//
//		}
//
//	} else {
//
//		if (query->numAtts == 0) {
//			// invalid query
//			return GetNextInSequence (fetchme, cnf, literal);
//
//		} else {
//			// valid query
//			return GetNextWithCNF (fetchme, cnf, literal);
//
//		}
//
//	}
//
//}
//
//int SortedFileHandler :: GetNextWithCNF(Record &fetchme, CNF &cnf, Record &literal) {
//
//	ComparisonEngine engine;
//
//	while (GetNext (fetchme)) {
//
//		if (!engine.Compare (&literal, query, &fetchme, order)){
//
//			if (engine.Compare (&fetchme, &literal, &cnf)){
//
//				return 1;
//
//			}
//
//		} else {
//
//			break;
//
//		}
//
//	}
//
//	return 0;
//
//}
//
//int SortedFileHandler :: GetNextInSequence(Record &fetchme, CNF &cnf, Record &literal) {
//
//	ComparisonEngine engine;
//
//	while (GetNext (fetchme)) {
//
//		if (engine.Compare (&fetchme, &literal, &cnf)){
//
//			return 1;
//
//		}
//
//	}
//
//	return 0;
//
//}
//
//int SortedFileHandler :: BinarySearchInSorted(Record &fetchme, CNF &cnf, Record &literal) {
//
//	off_t start = pIndex;
//	off_t end = file->GetLength () - 1;
//	off_t mid = pIndex;
//
//	Page *page = new Page;
//
//	ComparisonEngine engine;
//
//	while (true) {
//
//		mid = (start + end) / 2;
//
//		file->GetPage (page, mid);
//
//		if (page->GetFirst (&fetchme)) {
//
//			if (engine.Compare (&literal, query, &fetchme, order) <= 0) {
//
//				end = mid - 1;
//				if (end <= start) break;
//
//			} else {
//
//				start = mid + 1;
//				if (end <= start) break;
//
//			}
//
//		} else {
//
//			break;
//
//		}
//
//	}
//
//	if (engine.Compare (&fetchme, &literal, &cnf)) {
//
//		delete currentPage;
//
//		pIndex = mid;
//		currentPage = page;
//
//		return 1;
//
//	} else {
//
//		delete page;
//
//		return 0;
//
//	}
//
//}
//
//void SortedFileHandler :: startSortingThread() {
//
//	writingMode = true;
//
//	inPipe = new Pipe (buffsize);
//	outPipe = new Pipe (buffsize);
//
//	bigq = new BigQ(*inPipe, *outPipe, *order, runLength);
//
//}
//
//void SortedFileHandler :: FlushPipeToFile () {
//
//	inPipe->ShutDown ();
//
//	writingMode = false;
//
//	if (file->GetLength () > 0) {
//
//		MoveFirst ();
//
//	}
//
//	Record *recFromPipe = new Record;
//	Record *recFromFile = new Record;
//
//	HeapHandler *newFile = new HeapHandler;
//	newFile->Create ("bin/temp.bin");
//
//	int flagPipe = outPipe->Remove (recFromPipe);
//	int flagFile = GetNext (*recFromFile);
//
//	ComparisonEngine comp;
//
//	while (flagFile && flagPipe) {
//
//		if (comp.Compare (recFromPipe, recFromFile, order) > 0) {
//
//			newFile->Add (*recFromFile);
//			flagFile = GetNext (*recFromFile);
//
//		} else {
//
//			newFile->Add (*recFromPipe);
//			flagPipe = outPipe->Remove (recFromPipe);
//
//		}
//
//	}
//
//	while (flagFile) {
//
//		newFile->Add (*recFromFile);
//		flagFile = GetNext (*recFromFile);
//
//	}
//
//	while (flagPipe) {
//
//		newFile->Add (*recFromPipe);
//		flagPipe = outPipe->Remove (recFromPipe);
//
//	}
//
//	outPipe->ShutDown ();
//	newFile->Close ();
//	delete newFile;
//
//	file->Close ();
//
//	remove (fpath);
//	rename ("bin/temp.bin", fpath);
//
//	file->Open (1, fpath);
//
//	MoveFirst ();
//
//}
//
//int SortedFileHandler :: NewOrderGenerator(OrderMaker &query, OrderMaker &order, CNF &cnf) {
//    //Query was failing order generation with default method provided
//    //After a lot of discussion with classmates, we have made some changes and pasted here
//
//	query.numAtts = 0;
//	bool found = false;
//
//	for (int i = 0; i < order.numAtts; ++i) {
//
//
//		for (int j = 0; j < cnf.numAnds; ++j) {
//
//			if (cnf.orLens[j] != 1) {
//
//				continue;
//
//			}
//
//			if (cnf.orList[j][0].op != Equals) {
//
//				continue;
//
//			}
//
//			if ((cnf.orList[i][0].operand1 == Left && cnf.orList[i][0].operand2 == Left) ||
//               (cnf.orList[i][0].operand2 == Right && cnf.orList[i][0].operand1 == Right) ||
//               (cnf.orList[i][0].operand1==Left && cnf.orList[i][0].operand2 == Right) ||
//               (cnf.orList[i][0].operand1==Right && cnf.orList[i][0].operand2 == Left)) {
//
//                continue;
//
//			}
//
//
//			if (cnf.orList[j][0].operand1 == Left &&
//				cnf.orList[j][0].whichAtt1 == order.whichAtts[i]) {
//
//				query.whichAtts[query.numAtts] = cnf.orList[i][0].whichAtt2;
//				query.whichTypes[query.numAtts] = cnf.orList[i][0].attType;
//
//				query.numAtts++;
//
//				found = true;
//
//				break;
//
//			}
//
//			if (cnf.orList[j][0].operand2 == Left &&
//				cnf.orList[j][0].whichAtt2 == order.whichAtts[i]) {
//
//				query.whichAtts[query.numAtts] = cnf.orList[i][0].whichAtt1;
//				query.whichTypes[query.numAtts] = cnf.orList[i][0].attType;
//
//				query.numAtts++;
//
//				found = true;
//
//				break;
//
//			}
//
//		}
//
//		if (!found) {
//
//			break;
//
//		}
//
//	}
//
//	return query.numAtts;
//
//}
