#pragma once
#include <fstream>
#include <inttypes.h>
#include "cRowTable.h"


class cTable
{
private:
	uint32_t mColumnCount;
	uint32_t mRowCount;

	uint64_t** mData;


public:
	cTable(uint32_t columnCount, uint32_t rowCount);
	~cTable();

	static cTable* Load(char* filename)
	{
		std::ifstream is(filename, std::ifstream::binary);

		cTable* table = NULL;
		if (is)
		{
			uint64_t rowCount = 0, columnCount = 0;
			is.read((char*)&rowCount, sizeof(uint64_t));
			is.read((char*)&columnCount, sizeof(uint64_t));
			table = new cTable((uint32_t)columnCount, (uint32_t)rowCount);
			table->Read(is);
			is.close();
		}
		return table;
	}

	void Read(std::ifstream& is);

	uint64_t** GetData();
	uint32_t GetColumnCount();
	uint32_t GetRowCount();

	uint64_t* GetColumn(uint32_t column);
	uint64_t* GetRow(uint32_t row);

	cRowTable* Selection(char operation, int column, uint64_t value);

	void Print();

};

cTable::cTable(uint32_t columnCount, uint32_t rowCount) {
	mColumnCount = columnCount;
	mRowCount = rowCount;

	mData = new uint64_t * [mColumnCount];
	for (uint32_t i = 0; i < mColumnCount; i++)
	{
		mData[i] = new uint64_t[mRowCount];
	}
}

cTable::~cTable() {
	for (uint32_t i = 0; i < mColumnCount; i++) {
		if (mData[i] != NULL) {
			delete mData[i];
		}
	}
	delete mData;
}



void cTable::Read(std::ifstream &is)
{
	//for(uint32_t i = 0; i < mColumnCount; i++)
	//{
	//	for (uint32_t j = 0; j < mRowCount; j++) {
	//		is.read((char*)&mData[i][j], sizeof(uint64_t));
	//	}
	//}
	for (uint32_t i = 0; i < mColumnCount; i++)
	{
		is.read((char*)&mData[i][0], sizeof(uint64_t) * mRowCount);
	}
}

uint64_t** cTable::GetData() {
	return mData;
}

uint32_t cTable::GetColumnCount() {
	return mColumnCount;
}

uint32_t cTable::GetRowCount() {
	return mRowCount;
}


uint64_t* cTable::GetColumn(uint32_t column) {
	return mData[column];
}

uint64_t* cTable::GetRow(uint32_t row) {
	uint64_t* outp = new uint64_t[mColumnCount];
	for (uint32_t i = 0; i < mColumnCount; i++) {
		outp[i] = mData[i][row];
	}
	return outp;
}

void cTable::Print() {
	for (int i = 0; i < mColumnCount; i++) {
		for (int j = 0; j < mRowCount; j++) {
			printf("%" PRIu64 "\t", mData[i][j]);
		}
		printf("\n");
	}
}

cRowTable* cTable::Selection(char operation, int column, uint64_t value) {
	cRowTable* table = new cRowTable(1000, mColumnCount);

	for (int i = 0; i < mRowCount; i++) {
		if (operation == '=') {
			if (mData[column][i] == value) {
				table->Add(GetRow(i), true);
			}
		}
		else if (operation == '>') {
			if (mData[column][i] > value) {
				table->Add(GetRow(i), true);
			}
		}
		else if (operation == '<') {
			if (mData[column][i] < value) {
				table->Add(GetRow(i), true);
			}
		}
	}

	return table;
}


