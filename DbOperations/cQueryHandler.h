#pragma once
#include <string>
#include <iostream>
#include <sstream>
#include <regex>
#include "cTable.h"
#include "cRowTable.h"
#include "cJoin.h"
#include "IndexHandler.h"


class cQueryHandler
{
private:
    int mPreallocateRows;
    const char* mTablesPath;

	std::string mInputString;
    std::string mHelpString;

    int mTablesCount;
    int* mTablesColumnCountArr;
    int* mTablesArr;
	cTable** mColumnTables;

    int mProjectionAttrCount;
    int* mProjectionAttrs;

    int mJoinT1;
    int mJoinT2;
    int mJoinC1;
    int mJoinC2;

    int mSelectionTable;
    int mSelectionColumn;
    int mSelectionValue;
    char mOperation;

    cRowTable* r_join;
    cRowTable* r_sel;
    cRowTable* r_sum;

    bool mSelectFirst;


    void LoadTables();
    void GetProjectionAttrs();
    void GetJoinAttrs();
    void GetSelectionAttrs();

public:

	cQueryHandler(std::string inputString, int preallocateRows, const char* tablesPath);
	~cQueryHandler();

    void Join();
    void HashJoin(cMemory* memory);
    void Select();
    void SelectWithIndex(cMemory* memory, IndexHandler* indexHandler);
    void Sum();

    void PrintData();
    void ShortPrintData();
    void PrintTest();
    void PrintQuery();

};

cQueryHandler::cQueryHandler(std::string inputString, int preallocateRows, const char* tablesPath) {
    mTablesPath = tablesPath;
    mPreallocateRows = preallocateRows;
	mInputString = inputString;
    mHelpString = inputString.substr(inputString.find("|") + 1, sizeof(inputString));


	LoadTables();
    GetProjectionAttrs();
    GetJoinAttrs();
    GetSelectionAttrs();

    mSelectFirst = false;
    r_sel = NULL;
}

cQueryHandler::~cQueryHandler() {
    if (mTablesColumnCountArr != NULL) {
        delete mTablesColumnCountArr;
    }

    if (mTablesArr != NULL) {
        delete mTablesArr;
    }

    if (mProjectionAttrs != NULL) {
        delete mProjectionAttrs;
    }

    if (mColumnTables != NULL) {
        for (int i = 0; i < mTablesCount; i++) {
            if (mColumnTables[i] != NULL) {
                delete mColumnTables[i];
            }
        }
        delete mColumnTables;
    }

    if (r_join != NULL) {
        delete r_join;
    }

    if (r_sel != NULL) {
        delete r_sel;
    }

    if (r_sum != NULL) {
        delete r_sum;
    }

}


void cQueryHandler::LoadTables() {
    std::string tablesString = mInputString.substr(0, mInputString.find("|"));
    mTablesCount = (int)std::count(tablesString.begin(), tablesString.end(), ' ') + 1;

    mTablesArr = new int[mTablesCount];
    for (int i = 0; i < mTablesCount; i++) {
        mTablesArr[i] = 0;
    }

    int i = 0;
    std::stringstream sstables(tablesString);
    while (sstables.good() && i < mTablesCount) {

        std::string tmp;
        sstables >> tmp;

        mTablesArr[i++] = std::stoi(tmp);
    }

    mColumnTables = new cTable * [mTablesCount];

    for (int i = 0; i < mTablesCount; i++) {
        char path[50];
        sprintf_s(path, "%sr%d", mTablesPath, mTablesArr[i]);
        mColumnTables[i] = cTable::Load(path);
    }

    mTablesColumnCountArr = new int[mTablesCount];
    for (int i = 0; i < mTablesCount; i++) {
        mTablesColumnCountArr[i] = mColumnTables[i]->GetColumnCount();
    }
}

void cQueryHandler::GetProjectionAttrs() {
    std::string projectionString = mHelpString.substr(mHelpString.find("|") + 1, sizeof(mHelpString));

    mProjectionAttrCount = (int)std::count(projectionString.begin(), projectionString.end(), ' ') + 1;
    mProjectionAttrs = new int[mProjectionAttrCount];
    for (int i = 0; i < mProjectionAttrCount; i++) {
        mProjectionAttrs[i] = 0;
    }

    int i = 0;
    std::stringstream ssProject(projectionString);
    while (ssProject.good() && i < mProjectionAttrCount) {

        std::string tmp;
        ssProject >> tmp;

        int projectionTable = std::stoi(tmp.substr(0, tmp.find(".")));
        int projectionColumn = std::stoi(tmp.substr(tmp.find(".") + 1, sizeof(tmp)));

        for (int j = 0; j < projectionTable; j++) {
            projectionColumn += (int)mTablesColumnCountArr[j];
        }

        mProjectionAttrs[i++] = projectionColumn;
    }
}

void cQueryHandler::GetJoinAttrs() {

    std::string joinString = mHelpString.substr(0, mHelpString.find("&"));
    std::replace(joinString.begin(), joinString.end(), '.', ' ');
    std::replace(joinString.begin(), joinString.end(), '=', ' ');


    std::stringstream str_strm;
    str_strm << joinString;
    std::string temp_str;

    str_strm >> temp_str;
    std::stringstream(temp_str) >> mJoinT1;
    temp_str = "";

    str_strm >> temp_str;
    std::stringstream(temp_str) >> mJoinC1;
    temp_str = ""; 

    str_strm >> temp_str;
    std::stringstream(temp_str) >> mJoinT2;
    temp_str = "";

    str_strm >> temp_str;
    std::stringstream(temp_str) >> mJoinC2;
}

void cQueryHandler::GetSelectionAttrs() {

    std::string selectionString = mHelpString.substr(mHelpString.find("&") + 1, mHelpString.find("|") - mHelpString.find("&") - 1);
    std::replace(selectionString.begin(), selectionString.end(), '.', ' ');


    if (selectionString.find('<') != std::string::npos) {
        mOperation = '<';
        std::replace(selectionString.begin(), selectionString.end(), '<', ' ');
    }
    else if (selectionString.find('>') != std::string::npos) {
        mOperation = '>';
        std::replace(selectionString.begin(), selectionString.end(), '>', ' ');
    }
    else {
        mOperation = '=';
        std::replace(selectionString.begin(), selectionString.end(), '=', ' ');
    }

    std::stringstream str_strm;
    str_strm << selectionString;
    std::string temp_str;

    str_strm >> temp_str;
    std::stringstream(temp_str) >> mSelectionTable;
    temp_str = "";

    str_strm >> temp_str;
    std::stringstream(temp_str) >> mSelectionColumn;
    temp_str = "";

    str_strm >> temp_str;
    std::stringstream(temp_str) >> mSelectionValue;
    temp_str = "";
}



void cQueryHandler::Join() {
    if (r_sel == NULL) {
        r_join = cJoin::NestedLoop(mColumnTables[0], mColumnTables[1], mJoinC1, mJoinC2, mPreallocateRows);
    }
    else {
        if (mSelectionTable == 0) {
            r_join = cJoin::NestedLoop(r_sel, mColumnTables[1], mJoinC1, mJoinC2, mPreallocateRows);
        }
        else {
            r_join = cJoin::NestedLoop(mColumnTables[0], r_sel, mJoinC1, mJoinC2, mPreallocateRows);
        }
    }
}

void cQueryHandler::HashJoin(cMemory* memory) {
    if (r_sel == NULL) {
        r_join = cJoin::Hash(mColumnTables[0], mColumnTables[1], mJoinC1, mJoinC2, mPreallocateRows, memory);
    }
    else {
        if (mSelectionTable == 0) {
            r_join = cJoin::Hash(r_sel, mColumnTables[1], mJoinC1, mJoinC2, mPreallocateRows, memory);
        }
        else {
            r_join = cJoin::Hash(mColumnTables[0], r_sel, mJoinC1, mJoinC2, mPreallocateRows, memory);
        }
    }
}

void cQueryHandler::Select() {
    if (r_join != NULL) {
        int column = 0;
        int i = 0;
        while (i < mSelectionTable) {
            column += mTablesColumnCountArr[i++];
        }
        column += mSelectionColumn;

        r_sel = r_join->Selection(mOperation, column, mSelectionValue);
    }
    else {
        mSelectFirst = true;
        r_sel = mColumnTables[mSelectionTable]->Selection(mOperation, mSelectionColumn, mSelectionValue);
    }
}

void cQueryHandler::SelectWithIndex(cMemory* memory, IndexHandler* indexHandler) {
    if (r_join != NULL) {
        int column = 0;
        int i = 0;
        while (i < mSelectionTable) {
            column += mTablesColumnCountArr[i++];
        }
        column += mSelectionColumn;

        int indexNumber = mTablesArr[0] * 10000 + mTablesArr[1] * 100 + mSelectionColumn;

        if (!indexHandler->IndexExistsJoin(indexNumber)) {
            indexHandler->Add(r_join, (uint16_t)column, memory, indexNumber);
        }

        auto index = indexHandler->GetIndexJoin(indexNumber);
        int retCount = 0;
        uint64_t*** values = index->FindWithoutRecursion((uint64_t)mSelectionValue, retCount);

        if (retCount == 0) {
            r_sel = new cRowTable(r_join->GetColumnCount());;
            return;
        }

        r_sel = new cRowTable(mPreallocateRows, r_join->GetColumnCount());

        for (int i = 0; i < retCount; i++) {
            r_sel->Add(*values[i]);
        }

    }
    else {
        mSelectFirst = true;

        int indexNumber = mTablesArr[mSelectionTable] * 100 + mSelectionColumn;

        if (!indexHandler->IndexExistsSelect(indexNumber)) {
            indexHandler->Add(mColumnTables[mSelectionTable], (uint16_t)mSelectionColumn, memory, indexNumber);
        }

        auto index = indexHandler->GetIndexSelect(indexNumber);
        int retCount = 0;
        int* values = index->FindWithoutRecursion((uint64_t)mSelectionValue, retCount);

        if (retCount == 0) {
            r_sel = new cRowTable(mColumnTables[mSelectionTable]->GetColumnCount());;
            return;
        }

        r_sel = new cRowTable(mPreallocateRows, mColumnTables[mSelectionTable]->GetColumnCount());

        for (int i = 0; i < retCount; i++) {
            r_sel->Add(mColumnTables[mSelectionTable]->GetRow(values[i]), true);
        }
    }
}

void cQueryHandler::Sum() {
    if (!mSelectFirst) {
        r_sum = r_sel->Projection_Sum(mProjectionAttrs, mProjectionAttrCount);
    }
    else {
        r_sum = r_join->Projection_Sum(mProjectionAttrs, mProjectionAttrCount);
    }
}

void cQueryHandler::PrintData() {
    std::cout << "Input query: " << mInputString << "\n\n";

    for (int i = 0; i < mTablesCount; i++) {
        printf("Table r%d - %d rows and %d columns.\n", mTablesArr[i], mColumnTables[i]->GetRowCount(), mTablesColumnCountArr[i]);
    }
    
    printf("Joined table - %d rows and %d columns.\n", r_join->GetRowCount(), r_join->GetColumnCount());

    printf("\nSums for columns ");
    for (int i = 0; i < mProjectionAttrCount; i++) {
        if (i == mProjectionAttrCount - 2) {
            printf("%d and ", mProjectionAttrs[i] + 1);
        }
        else if (i == mProjectionAttrCount - 1) {
            printf("%d:\n", mProjectionAttrs[i] + 1);
        }
        else {
            printf("%d, ", mProjectionAttrs[i] + 1);
        }
    }
    r_sum->Print();

    if (!mSelectFirst) {
        printf("\nData in joined table after selection (%d rows):\n", r_sel->GetRowCount());
        r_sel->PrintSample();
    }
    else {
        printf("\nData in joined table after selection (%d rows):\n", r_join->GetRowCount());
        r_join->PrintSample();
    }
}

void cQueryHandler::ShortPrintData() {
    printf("Joined table - %d columns and %d rows.\n", r_join->GetColumnCount(), r_join->GetRowCount());
}

void cQueryHandler::PrintQuery() {
    std::cout << mInputString << "\n\n";
    //r_sum->Print();
}

void cQueryHandler::PrintTest() {
    mColumnTables[0]->Print();
}