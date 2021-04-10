#pragma once

#include "cMemory.h"

template<class TKey, class TData>
class cHashTableNode
{
private:
	bool mEmptyNode;
	TKey mKey;
	TData mData;
	cHashTableNode* mNextNode;

public:
	cHashTableNode();
	cHashTableNode(const TKey& key, const TData& data);
	~cHashTableNode();

	bool Add(const TKey& key, const TData& data, cMemory* memory, int& itemCount, int& nodeCount);
	bool Find(const TKey& key, TData& data) const;

	TKey GetKey();
	TData GetData();
	cHashTableNode* GetNextNode();

	void SetNextNode(cMemory* memory, const TKey& key, const TData& data);

	bool HasNextNode();

};

template<class TKey, class TData>
cHashTableNode<TKey, TData>::cHashTableNode()
{
	mNextNode = NULL;
	mEmptyNode = true;
}

template<class TKey, class TData>
cHashTableNode<TKey, TData>::cHashTableNode(const TKey& key, const TData& data)
{
	mNextNode = NULL;
	mEmptyNode = false;
	mKey = key;
	mData = data;
}

template<class TKey, class TData>
cHashTableNode<TKey, TData>::~cHashTableNode()
{
	if (mNextNode != NULL)
	{
		delete mNextNode;
		mNextNode = NULL;
	}
}

template<class TKey, class TData>
bool cHashTableNode<TKey, TData>::Add(const TKey& key, const TData& data, cMemory* memory, int& itemCount, int& nodeCount)
{
	bool ret = true;

	if (!mEmptyNode) {
		if (mKey == key) {
			ret = false;
		}
		else {
			if (mNextNode == NULL) {
				if (memory == NULL) {
					mNextNode = new cHashTableNode<TKey, TData>();
				}
				else {
					char* mem = memory->New(sizeof(cHashTableNode<TKey, TData>));
					mNextNode = new (mem)cHashTableNode<TKey, TData>;
				}
				nodeCount++;
			}
			ret = mNextNode->Add(key, data, memory, itemCount, nodeCount);
		}
	}
	else {
		mKey = key;
		mData = data;
		mEmptyNode = false;
		itemCount++;

		ret = true;
	}
	return ret;
}

template<class TKey, class TData>
bool cHashTableNode<TKey, TData>::Find(const TKey& key, TData& data) const
{
	if (!mEmptyNode) {
		if (mKey == key) {
			data = mData;
			return true;
		}
		else if (mNextNode != NULL) {
			return mNextNode->Find(key, data);
		}
	}
	return false;
}


template<class TKey, class TData>
inline TKey cHashTableNode<TKey, TData>::GetKey()
{
	return mKey;
}

template<class TKey, class TData>
inline TData cHashTableNode<TKey, TData>::GetData()
{
	return mData;
}

template<class TKey, class TData>
inline cHashTableNode<TKey, TData>* cHashTableNode<TKey, TData>::GetNextNode()
{
	return mNextNode;
}


template<class TKey, class TData>
inline void cHashTableNode<TKey, TData>::SetNextNode(cMemory* memory, const TKey& key, const TData& data)
{
	if (memory == NULL) {
		mNextNode = new cHashTableNode<TKey, TData>(key, data);
	}
	else {
		char* mem = memory->New(sizeof(cHashTableNode<TKey, TData>));
		mNextNode = new (mem)cHashTableNode<TKey, TData>(key, data);
	}
}

template<class TKey, class TData>
inline bool cHashTableNode<TKey, TData>::HasNextNode()
{
	return mNextNode != NULL;
}
