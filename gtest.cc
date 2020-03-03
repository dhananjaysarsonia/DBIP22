#include "gtest.h"
#include "test.h"
#include "Util.h"
#include <fstream>
#include "BigQ.h"
#include <pthread.h>


int runlen = 0;

int add_data (FILE *src, int numrecs, int &res) {
	DBFile dbfile;
	dbfile.Open (rel->path ());
	Record temp;

	int proc = 0;
	int xx = 20000;
	while ((res = temp.SuckNextRecord (rel->schema (), src)) && ++proc < numrecs) {
		dbfile.Add (temp);
		if (proc == xx) cerr << "\t ";
		if (proc % xx == 0) cerr << ".";
	}

	dbfile.Close ();
	return proc;
}



TEST (SortCheck, CreateFile) {


    OrderMaker o;

    int runlen = 8;

    cout <<"Please enter:    (c_phone)";
    rel->get_sort_order (o);
    
    struct {OrderMaker *o; int l;} startup = {&o, runlen};

    DBFile dbfile;
    cout << "\n output to dbfile : " << rel->path () << endl;
    dbfile.Create (rel->path(), sorted, &startup);
    dbfile.Close ();

    char tbl_path[100];
    sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
    cout << " input from file : " << tbl_path << endl;

        FILE *tblfile = fopen (tbl_path, "r");

    srand48 (time (NULL));

    int proc = 1, res = 1, tot = 0;
    while (proc && res) {
        int x = 2;
//        while (x < 1 || x > 3) {
//
//            cout << "\n select option for : " << rel->path () << endl;
//            cout << " \t 1. add a few (1 to 1k recs)\n";
//            cout << " \t 2. add a lot (1k to 1e+06 recs) \n";
//            cout << " \t 3. run some query \n \t ";
//            cin >> x;
//            x=2;
//        }
        if (x < 3) {
            proc = add_data (tblfile,lrand48()%(int)pow(1e3,x)+(x-1)*1000, res);
            tot += proc;
            if (proc)
                cout << "\n\t added " << proc << " recs..so far " << tot << endl;
        }
        else {
            //test3 ();
        }
    }
    cout << "\n create finished.. " << tot << " recs inserted\n";
    fclose (tblfile);
}



TEST (SortCheck, CheckIfTempFileIsDeleted) {
    //temp file is created for storing runs. It checks if the temp file is deleted or not to save memory.
    char *filepath = "temp.bin";
    ifstream file(filepath);
    
    bool status;
    if(!file)            // If the file was not found, then file is 0, i.e. !file=1 or true.
        status = false;    // The file was not found.
    else                 // If the file was found, then file is non-0.
        status =  true;     // The file was found.

EXPECT_EQ(false, status);

}


TEST (SortCheck, LoadTest) {
	int count = 0;
	
	OrderMaker o (rel->schema ());
	
	struct {OrderMaker *o; int l;} startup = {&o, runlen};
	
	DBFile dbfile;
	cout << "\t\n Start loading " << rel->path () << endl;
	
	dbfile.Create (rel->path(), sorted, &startup);
	dbfile.Close ();
	
	srand48 (time (NULL));
	
	char tbl_path[100];
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
	
	FILE* tblfile = fopen (tbl_path, "r");
	
	int proc = 1, res = 1, tot = 0;
	
	while (proc && res) {
		
		proc = add_data (tblfile,lrand48()%(int)pow(1e3,2)+1000, res);
		tot += proc;
		count++;
		if (proc) 
			cout << "\n\t Run " << count << " : added " << proc << " recs..so far " << tot << endl;
		
	}
	
	fclose (tblfile);
	cout << "\n" << rel->path () << " created!" << endl;
	cout << tot << " recs inserted" << endl;
	
}




TEST (SortCheck, CheckOrderedTest) {
	
	OrderMaker o(rel->schema ());
	ComparisonEngine comp;
	
	DBFile dbfile;
	dbfile.Open (rel->path ());
	cout << "\t\n start to check the order of " << rel->path () << " . \t\n";		
	
	Record *rec = new Record;
	Record *prev = new Record;
    int error = 0;
    
	if (!dbfile.GetNext (*prev)) {
		
		while (dbfile.GetNext (*rec)) {
			
            if(comp.Compare(prev, rec, &o) == 1)
            {
                error ++;
            }
			//ASSERT_NE (comp.Compare (prev, rec, &o), 1);
			prev->Copy (rec);
		
		}
		
	}
    
	
	dbfile.Close ();
	cout << "\t\n done!" << endl;
	
	cleanup ();
    ASSERT_EQ(0, error);
	
}


int main (int argc, char **argv) {
	
	testing::InitGoogleTest(&argc, argv);
	
	setup ();
	
	relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};
    runlen = 8;
// 	while (runlen < 1) {
//		cout << "\t\n specify runlength:\n\t ";
//		cin >> runlen;
//	}
	
	int findx = 3;
    cout << "GTEST WILL RUN FOR CUSTOMER TABLE, PLEASE MAKE SURE CUSTOMER.TBL FILE IS COPIED :)";
//	while (findx < 1 || findx > 8) {
//		cout << "\n select table: \n";
//		cout << "\t 1. nation \n";
//		cout << "\t 2. region \n";
//		cout << "\t 3. customer \n";
//		cout << "\t 4. part \n";
//		cout << "\t 5. partsupp \n";
//		cout << "\t 6. supplier \n";
//		cout << "\t 7. orders \n";
//		cout << "\t 8. lineitem \n \t ";
//		cin >> findx;
//	}
	rel = rel_ptr [findx - 1];
	
	return RUN_ALL_TESTS ();
	
}
