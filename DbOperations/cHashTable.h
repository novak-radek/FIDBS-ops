#pragma once

#include "cHashTableNode.h"
#include "cMemory.h"

template<class TKey, class TData>
class cHashTable
{
private:
	int mSize;
	cHashTableNode<TKey, TData>** mHashTable;
	int mItemCount;
	int mNodeCount;
	cMemory* mMemory;

private:
	inline int HashValue(const TKey& key) const;

public:
	cHashTable(int size, cMemory* memory);
	~cHashTable();

	bool Add(const TKey& key, const TData& data);
	bool AddWithoutRecursion(const TKey& key, const TData& data);
	bool FindWithoutRecursion(const TKey& key, TData& data) const;
	bool Find(const TKey& key, TData& data) const;
	void PrintStat() const;
};

template<class TKey, class TData>
cHashTable<TKey, TData>::cHashTable(int size, cMemory* memory)
{
	mSize = size;
	mHashTable = new cHashTableNode<TKey, TData>*[size];
	for (int i = 0; i < mSize; i++)
	{
		mHashTable[i] = NULL;
	}
	mMemory = memory;
}

template<class TKey, class TData>
cHashTable<TKey, TData>::~cHashTable()
{
	if (mMemory == NULL) {
		for (int i = 0; i < mSize; i++) {
			if (mHashTable[i] != NULL) {
				delete mHashTable[i];
			}
		}
	}

	delete mHashTable;
}

template<class TKey, class TData>
bool cHashTable<TKey, TData>::Add(const TKey& key, const TData& data)
{
	int hv = HashValue(key);
	if (mHashTable[hv] == NULL)
	{
		if (mMemory == NULL) {
			mHashTable[hv] = new cHashTableNode<TKey, TData>();
		}
		else {
			char* mem = mMemory->New(sizeof(cHashTableNode<TKey, TData>));
			mHashTable[hv] = new (mem) cHashTableNode<TKey, TData>();
		}
		mNodeCount++;
	}
	return mHashTable[hv]->Add(key, data, mMemory, mItemCount, mNodeCount);
}

template<class TKey, class TData>
bool cHashTable<TKey, TData>::AddWithoutRecursion(const TKey& key, const TData& data)
{
	int hv = HashValue(key);
	if (mHashTable[hv] == NULL)
	{
		if (mMemory == NULL) {
			mHashTable[hv] = new cHashTableNode<TKey, TData>(key, data);
		}
		else {
			char* mem = mMemory->New(sizeof(cHashTableNode<TKey, TData>));
			mHashTable[hv] = new (mem) cHashTableNode<TKey, TData>(key, data);
		}
		mNodeCount++;
	}
	else {
		cHashTableNode<TKey, TData>* invNode = mHashTable[hv];
		while (invNode->HasNextNode()) {
			if (invNode->GetKey() == key) {
				return false;
			}
			invNode = invNode->GetNextNode();
		}
		invNode->SetNextNode(mMemory, key, data);
		mNodeCount++;
	}

	mItemCount++;

	return true;
}


template<class TKey, class TData>
bool cHashTable<TKey, TData>::Find(const TKey& key, TData& data) const
{
	int hv = HashValue(key);

	if (mHashTable[hv] == NULL) {
		return false;
	}

	return mHashTable[hv]->Find(key, data);
}

template<class TKey, class TData>
bool cHashTable<TKey, TData>::FindWithoutRecursion(const TKey& key, TData& data) const
{
	int hv = HashValue(key);

	cHashTableNode<TKey, TData>* invNode = mHashTable[hv];

	if (invNode == NULL) {
		return false;
	}

	if (invNode->GetKey() == key) {
		data = invNode->GetData();
		return true;
	}

	while (invNode->HasNextNode()) {
		invNode = invNode->GetNextNode();
		if (invNode->GetKey() == key) {
			data = invNode->GetData();
			return true;
		}
	}

	return false;
}

template<class TKey, class TData>
inline int cHashTable<TKey, TData>::HashValue(const TKey& key) const
{
	return key % mSize;
}

template<class TKey, class TData>
void cHashTable<TKey, TData>::PrintStat() const
{
	printf("HashTable Statistics: Size: %d, ItemCount: %d, NodeCount: %d, Avg. Items/Slot: %.2f.\n",
		mSize, mItemCount, mNodeCount, (float)mItemCount / mSize);
}
