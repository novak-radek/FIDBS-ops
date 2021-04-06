#pragma once
#include <inttypes.h>


class cRowTable
{
private:
	int mPreallocatedCount;
	int mRowCount;
	int mColumnCount;

	bool mProjection;

	uint64_t** mData;
	cRowTable* mNextData;

	uint64_t GetColumnCount(int column);

public:

	cRowTable(int preallocateRows, int columnCount);
	cRowTable(int columnCount);
	~cRowTable();

	void Add(uint64_t* row, bool del);
	void Add(uint64_t* row0, uint64_t* row1, uint32_t columnCount0, uint32_t columnCount1, int del);
	void Print();
	void PrintSample();
	
	int GetRowCount();
	int GetColumnCount();

	uint64_t* GetColumn(uint32_t column);
	void GetColumn(uint64_t** out, uint32_t column, uint32_t shift);
	uint64_t* GetRow(uint32_t row);

	cRowTable* Projection_Sum(int* attrList, int attrCount);
	cRowTable* Selection(char operation, int column, uint64_t value);
	
};

cRowTable::cRowTable(int preallocateRows, int columnCount) {
	mPreallocatedCount = preallocateRows;
	mRowCount = 0;
	mColumnCount = columnCount;

	mProjection = false;

	mData = new uint64_t * [preallocateRows];
	for (int i = 0; i < preallocateRows; i++)
	{
		mData[i] = new uint64_t[columnCount];
	}

	mNextData = NULL;
}

cRowTable::cRowTable(int columnCount) {
	mPreallocatedCount = 1;
	mRowCount = 0;
	mColumnCount = columnCount;

	mProjection = true;

	mData = new uint64_t * [1];
	mData[0] = new uint64_t[columnCount];

	mNextData = NULL;
}

cRowTable::~cRowTable() {
	if (mNextData != NULL) {
		delete mNextData;
	}
	
	for (int i = 0; i < mRowCount; i++) {
		if (mData[i] != NULL) {
			delete mData[i];
		}
	}
	delete mData;
}

void cRowTable::Add(uint64_t* row, bool del = false) {
	if (mNextData != NULL) {
		mNextData->Add(row, del);
		return;
	}

	for (int i = 0; i < mColumnCount; i++) {
		mData[mRowCount][i] = row[i];
	}

	mRowCount++;

	if (del) {
		delete[] row;
	}

	if (mRowCount == mPreallocatedCount && !mProjection) {
		mNextData = new cRowTable(mPreallocatedCount, mColumnCount);
	}
}

void cRowTable::Add(uint64_t* row0, uint64_t* row1, uint32_t columnCount0, uint32_t columnCount1, int del = 999) {
	if (mNextData != NULL) {
		mNextData->Add(row0, row1, columnCount0, columnCount1, del);
		return;
	}

	int index = 0;
	for (int i = 0; i < columnCount0; i++) {
		mData[mRowCount][i] = row0[i];
		index++;
	}

	for (int i = 0; i < columnCount1; i++) {
		mData[mRowCount][index] = row1[i];
		index++;
	}

	mRowCount++;

	if (del == 0) {
		delete[] row0;
	}
	else if (del == 1) {
		delete[] row1;
	}
	else {
		delete[] row0;
		delete[] row1;
	}

	if (mRowCount == mPreallocatedCount && !mProjection) {
		mNextData = new cRowTable(mPreallocatedCount, mColumnCount);
	}
}

void cRowTable::Print() {
	for (int i = 0; i < mRowCount; i++) {
		for (int j = 0; j < mColumnCount; j++) {
			printf("%" PRIu64 "\t", mData[i][j]);
		}
		printf("\n");
	}
	if (mNextData != NULL) {
		mNextData->Print();
	}
}

void cRowTable::PrintSample() {
	int counter = 0;
	for (int i = 0; i < mRowCount; i++) {
		if (counter == 3) {
			break;
		}

		for (int j = 0; j < mColumnCount; j++) {
			printf("%" PRIu64 "\t", mData[i][j]);
		}
		printf("\n");
		counter++;
	}
	if (counter < mRowCount) {
		printf("...\n");
	}
}

int cRowTable::GetRowCount() {
	if (mNextData != NULL) {
		return mPreallocatedCount + mNextData->GetRowCount();
	}
	return mRowCount;
}

int cRowTable::GetColumnCount() {
	return mColumnCount;
}

cRowTable* cRowTable::Projection_Sum(int* attrList, int attrCount) {
	cRowTable* table = new cRowTable(attrCount);
	uint64_t* sums = new uint64_t[attrCount];

	for (int i = 0; i < attrCount; i++) {
		sums[i] = GetColumnCount(attrList[i]);
	}

	table->Add(sums);

	delete[] sums;

	return table;
}

uint64_t cRowTable::GetColumnCount(int column) {

	uint64_t counter = 0;

	for (int i = 0; i < mRowCount; i++) {
		counter += mData[i][column];
	}

	if (mNextData != NULL) {
		counter += mNextData->GetColumnCount(column);
	}

	return counter;
}

cRowTable* cRowTable::Selection(char operation, int column, uint64_t value) {
	cRowTable* table = NULL;
	if (mNextData != NULL) {
		table = mNextData->Selection(operation, column, value);
	}
	else {
		table = new cRowTable(mPreallocatedCount, mColumnCount);
	}

	for (int i = 0; i < mRowCount; i++) {
		if (operation == '=') {
			if (mData[i][column] == value) {
				table->Add(mData[i]);
			}
		}
		else if (operation == '>') {
			if (mData[i][column] > value) {
				table->Add(mData[i]);
			}
		}
		else if (operation == '<') {
			if (mData[i][column] < value) {
				table->Add(mData[i]);
			}
		}
	}


	return table;
}

uint64_t* cRowTable::GetColumn(uint32_t column) {
	uint64_t* outp = new uint64_t[GetRowCount()];
	for (uint32_t i = 0; i < mRowCount; i++) {
		outp[i] = mData[i][column];
	}
	if (mNextData != NULL) {
		uint64_t* nextColumn = mNextData->GetColumn(column);
		for (int i = mRowCount; i < GetRowCount(); i++) {
			outp[i] = nextColumn[i- mRowCount];
		}

		delete[] nextColumn;
	}
	return outp;
}


uint64_t* cRowTable::GetRow(uint32_t row) {
	if (row < mRowCount) {
		return mData[row];
	}
	else {
		return mNextData->GetRow(row - mRowCount);
	}
}


