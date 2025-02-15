
CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

gtest: Record.o Comparison.o ComparisonEngine.o Schema.o File.o Run.o RecordComparator.o RunComparator.o BigQ.o FileHandler.o HeapHandler.o SortedFileHandler.o DBFile.o Pipe.o y.tab.o lex.yy.o gtest-all.o gtest.o
	$(CC) -o gtest Record.o Comparison.o ComparisonEngine.o Schema.o File.o Run.o RecordComparator.o RunComparator.o BigQ.o FileHandler.o HeapHandler.o SortedFileHandler.o  DBFile.o Pipe.o y.tab.o lex.yy.o gtest-all.o gtest.o -ll -lpthread
gtest-all.o: gtest-all.cc
	$(CC)  -g -DGTEST_HAS_PTHREAD=0 -c gtest-all.cc
#-DGTEST_HAS_PTHREAD=0
test: Record.o Comparison.o ComparisonEngine.o Schema.o File.o Run.o RecordComparator.o RunComparator.o BigQ.o FileHandler.o HeapHandler.o SortedFileHandler.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o
	$(CC) -o test Record.o Comparison.o ComparisonEngine.o Schema.o File.o Run.o RecordComparator.o RunComparator.o BigQ.o FileHandler.o HeapHandler.o SortedFileHandler.o DBFile.o Pipe.o y.tab.o lex.yy.o test.o -ll -lpthread

a21test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-1-test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o BigQ.o DBFile.o File.o Pipe.o y.tab.o lex.yy.o a2-1-test.o -ll -lpthread

a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o File.o DBFile.o y.tab.o lex.yy.o a1-test.o Schema.o -ll

#gtest.o: gtest.cc
#	$(CC) -g -c gtest.cc

test.o: test.cc
	$(CC) -g -c test.cc
	
a2-1-test.o: a2-1-test.cc
	$(CC) -g -c a2-1-test.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	gsed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
