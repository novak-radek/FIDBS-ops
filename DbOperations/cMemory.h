#pragma once

#include <stdio.h>
class cMemory
{
private:
	int mCapacity;
	int mSize;
	char* mData;
public:
	cMemory(int capacity);
	~cMemory();
	inline char* New(int size);
	void PrintStat() const;
};

char* cMemory::New(int size)
{
	char* mem = NULL;
	if (mSize + size >= mCapacity)
	{
		printf("Critical Error: cMemory::New(): There is no memory. \n");
		mem = NULL;
	}
	else {
		mem = mData + mSize;
		mSize += size;
	}
	return mem;
}

