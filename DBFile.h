#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"

typedef enum {
	heap,
	sorted,
	tree
} fType;

class SortBus {
public:
    
	OrderMaker *myOrder;
	int runLength;
	
};

class FileHandler {

protected:
	
	File *file;
	Page *currentPage;
	
	char *fpath;
	bool writingMode;
	
	off_t pIndex;
	
public:
	
	virtual int Create (const char *fpath) = 0;
	virtual int Open (char *fpath) = 0;
	virtual int Close () = 0;
	
	virtual void Load (Schema &myschema, const char *loadpath) = 0;
	
	virtual void MoveFirst () = 0;
	virtual void Add (Record &addme) = 0;
	virtual int GetNext (Record &fetchme) = 0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
	
	virtual ~FileHandler ();
	
};

class HeapHandler : public FileHandler {

public:
	
	HeapHandler ();
	~HeapHandler ();
	
	int Create (const char *fpath);
	int Open (char *fpath);
	int Close ();
	
	void Load (Schema &myschema, const char *loadpath);
	
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};

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

class DBFile {
	
private:
	
	FileHandler *fileHandler;         

public:
	
	DBFile ();
	~DBFile ();
	
	int Create (const char *fpath, fType ftype, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
