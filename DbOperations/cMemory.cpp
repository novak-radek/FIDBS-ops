#include "cMemory.h "

cMemory::cMemory(int capacity) {
	mData = new char[capacity];
	mCapacity = capacity;
	mSize = 0;
}
cMemory::~cMemory() {
	if (mData != NULL) {
		delete mData;
		mCapacity = 0;
		mSize = 0;
	}
}
void cMemory::PrintStat() const {
	printf("cMemory::PrintStat(): Capacity: %d, Size: %d, Utilization: %.2f.\n", mCapacity, mSize, (float)mSize / mCapacity);
}
