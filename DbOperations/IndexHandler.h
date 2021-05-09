#pragma once
#include <list>
#include "cHashTable.h"
#include "cRowTable.h"

class IndexHandler 
{
private:
	std::list<int> mIndexColumnsSelect;
	std::list<int> mIndexColumnsJoin;
	std::list<cHashTable<uint64_t, int>*> mIndexesSelect;
	std::list<cHashTable<uint64_t, uint64_t**>*> mIndexesJoin;


public:
	IndexHandler() = default;
	~IndexHandler();

	bool IndexExistsSelect(int column);
	bool IndexExistsJoin(int column);
	cHashTable< uint64_t, int >* GetIndexSelect(int column);
	cHashTable< uint64_t, uint64_t** >* GetIndexJoin(int column);
	void Add(cRowTable* table, uint16_t column, cMemory* memory, int indexNumber);
	void Add(cTable* table, uint16_t column, cMemory* memory, int indexNumber);

private:
	int GetIndexOfIndexSelect(int column);
	int GetIndexOfIndexJoin(int column);
};

IndexHandler::~IndexHandler() {
	mIndexColumnsSelect.clear();
	mIndexColumnsJoin.clear();

	for (auto& item : mIndexesSelect)
		delete item;
	mIndexesSelect.clear();

	for (auto& item : mIndexesJoin)
		delete item;
	mIndexesJoin.clear();
}

bool IndexHandler::IndexExistsSelect(int column) {
	return (std::find(mIndexColumnsSelect.begin(), mIndexColumnsSelect.end(), column) != mIndexColumnsSelect.end());
}

bool IndexHandler::IndexExistsJoin(int column) {
	return (std::find(mIndexColumnsJoin.begin(), mIndexColumnsJoin.end(), column) != mIndexColumnsJoin.end());
}

int IndexHandler::GetIndexOfIndexSelect(int column) {
    auto it = std::find(mIndexColumnsSelect.begin(), mIndexColumnsSelect.end(), column);
	int index = 0;

    if (it != mIndexColumnsSelect.end())
    {
		index = std::distance(mIndexColumnsSelect.begin(), it);
    }
    else {
		index = -1;
    }

	return index;
}

int IndexHandler::GetIndexOfIndexJoin(int column) {
	auto it = std::find(mIndexColumnsJoin.begin(), mIndexColumnsJoin.end(), column);
	int index = 0;

	if (it != mIndexColumnsJoin.end())
	{
		index = std::distance(mIndexColumnsJoin.begin(), it);
	}
	else {
		index = -1;
	}

	return index;
}

void IndexHandler::Add(cRowTable* table, uint16_t columnNumber, cMemory* memory, int indexNumber) {

	uint32_t rowCount = table->GetRowCount();

	uint64_t* column = table->GetColumn(columnNumber);

	cHashTable< uint64_t, uint64_t** >* index = new cHashTable< uint64_t, uint64_t** >(rowCount * 3, memory);

	for (int i = 0; i < rowCount; i++) {
		index->AddWithoutRecursion(column[i], table->GetRowPointer(i));
	}

	delete column;
	mIndexesJoin.push_back(index);
	mIndexColumnsJoin.push_back(indexNumber);
}

void IndexHandler::Add(cTable* table, uint16_t columnNumber, cMemory* memory, int indexNumber) {

	uint32_t rowCount = table->GetRowCount();

	uint64_t* column = table->GetColumn(columnNumber);

	cHashTable< uint64_t, int >* index = new cHashTable< uint64_t, int >(rowCount * 3, memory);

	for (int i = 0; i < rowCount; i++) {
		index->AddWithoutRecursion(column[i], i);
	}

	mIndexesSelect.push_back(index);
	mIndexColumnsSelect.push_back(indexNumber);
}



cHashTable< uint64_t, int >* IndexHandler::GetIndexSelect(int column) {
	int index = GetIndexOfIndexSelect(column);

	if (index == -1) {
		return NULL;
	}

	auto it = mIndexesSelect.begin();
	for (int i = 0; i < index; i++) {
		++it;
	}

	return *it;

}

cHashTable< uint64_t, uint64_t** >* IndexHandler::GetIndexJoin(int column) {
	int index = GetIndexOfIndexJoin(column);

	if (index == -1) {
		return NULL;
	}

	auto it = mIndexesJoin.begin();
	for (int i = 0; i < index; i++) {
		++it;
	}

	return *it;

}


