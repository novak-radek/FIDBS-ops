// DbOperations.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include "cTable.h"
#include "cRowTable.h"
#include "cJoin.h"

int main()
{
    const int preallocateRows = 1000;

    cTable* r0 = cTable::Load((char *) "../tables/r5");
    cTable* r1 = cTable::Load((char *) "../tables/r0");

    cRowTable * r_join = cJoin::NestedLoop(r0, r1, 2, 0, preallocateRows);

    printf("Table r5 - %d rows and %d columns.\n", r0->GetRowCount(), r0->GetColumnCount());
    printf("Table r0 - %d rows and %d columns.\n", r1->GetRowCount(), r1->GetColumnCount());
    printf("Joined table - %d rows and %d columns.\n", r_join->GetRowCount(), r_join->GetColumnCount());

    int attrCount = 3;
    int attrList[] = { 1, 2, 4 };
    cRowTable *r_sum = r_join->Projection_Sum(attrList, attrCount);
    printf("\nSums for columns 2,3 and 5\n");
    r_sum->Print();

    printf("\nData in joined table...\n\n");
    r_join->Print();

    delete r0;
    delete r1;
    delete r_join;
    delete r_sum;
}

// Run program: Ctrl + F5 or Debug > Start Without Debugging menu
// Debug program: F5 or Debug > Start Debugging menu

// Tips for Getting Started: 
//   1. Use the Solution Explorer window to add/manage files
//   2. Use the Team Explorer window to connect to source control
//   3. Use the Output window to see build output and other messages
//   4. Use the Error List window to view errors
//   5. Go to Project > Add New Item to create new code files, or Project > Add Existing Item to add existing code files to the project
//   6. In the future, to open this project again, go to File > Open > Project and select the .sln file
