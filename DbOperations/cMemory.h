#pragma once

#include <stdio.h>
class cMemory
{
private:
	int mCapacity;
	int mSize;
	char* mData;
	cMemory* mNext;
public:
	cMemory(int capacity);
	~cMemory();
	inline char* New(int size);
	void Reset();
	void PrintStat() const;
};

char* cMemory::New(int size)
{
	if (mNext != NULL) {
		return mNext->New(size);
	}
	char* mem = NULL;
	if (mSize + size >= mCapacity)
	{
		mNext = new cMemory(mCapacity);
		return mNext->New(size);
	}
	else {
		mem = mData + mSize;
		mSize += size;
	}
	return mem;
}

