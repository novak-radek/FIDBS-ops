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

	void Add(uint64_t* row);
	void Print();
	
	int GetRowCount();
	int GetColumnCount();

	cRowTable* Projection_Sum(int* attrList, int attrCount);
	
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

void cRowTable::Add(uint64_t* row) {
	if (mNextData != NULL) {
		mNextData->Add(row);
		return;
	}

	for (int i = 0; i < mColumnCount; i++) {
		mData[mRowCount][i] = row[i];
	}

	mRowCount++;

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


