#pragma once
#include "cTable.h"
#include "cRowTable.h"
#include "cMemory.h"
#include "cHashTable.h"

#define TKey uint64_t
#define TData uint64_t*

class cJoin
{
private:


public:
	static cRowTable* NestedLoop(cTable* r0, cTable* r1, uint16_t r0AttrOrder, uint16_t r1AttrOrder, int preallocateRowsCount) {


		uint64_t* column0 = r0->GetColumn(r0AttrOrder);
		uint64_t* column1 = r1->GetColumn(r1AttrOrder);

		uint32_t rowCount0 = r0->GetRowCount();
		uint32_t rowCount1 = r1->GetRowCount();

		uint32_t columnCount0 = r0->GetColumnCount();
		uint32_t columnCount1 = r1->GetColumnCount();


		cRowTable* table = new cRowTable(preallocateRowsCount, columnCount0 + columnCount1);

		for (uint32_t i = 0; i < rowCount0; i++) {
			for (uint32_t j = 0; j < rowCount1; j++) {
				if (column0[i] == column1[j]) {
					table->Add(r0->GetRow(i), r1->GetRow(j), columnCount0, columnCount1);
				}
			}
		}


		return table;
	}

	static cRowTable* NestedLoop(cRowTable* r0, cTable* r1, uint16_t r0AttrOrder, uint16_t r1AttrOrder, int preallocateRowsCount) {


		uint64_t* column0 = r0->GetColumn(r0AttrOrder);
		uint64_t* column1 = r1->GetColumn(r1AttrOrder);

		uint32_t rowCount0 = r0->GetRowCount();
		uint32_t rowCount1 = r1->GetRowCount();

		uint32_t columnCount0 = r0->GetColumnCount();
		uint32_t columnCount1 = r1->GetColumnCount();


		cRowTable* table = new cRowTable(preallocateRowsCount, columnCount0 + columnCount1);

		for (uint32_t i = 0; i < rowCount0; i++) {
			for (uint32_t j = 0; j < rowCount1; j++) {
				if (column0[i] == column1[j]) {
					table->Add(r0->GetRow(i), r1->GetRow(j), columnCount0, columnCount1, 1);
				}
			}
		}

		delete[] column0;
		return table;
	}

	static cRowTable* NestedLoop(cTable* r0, cRowTable* r1, uint16_t r0AttrOrder, uint16_t r1AttrOrder, int preallocateRowsCount) {


		uint64_t* column0 = r0->GetColumn(r0AttrOrder);
		uint64_t* column1 = r1->GetColumn(r1AttrOrder);

		uint32_t rowCount0 = r0->GetRowCount();
		uint32_t rowCount1 = r1->GetRowCount();

		uint32_t columnCount0 = r0->GetColumnCount();
		uint32_t columnCount1 = r1->GetColumnCount();


		cRowTable* table = new cRowTable(preallocateRowsCount, columnCount0 + columnCount1);

		for (uint32_t i = 0; i < rowCount0; i++) {
			for (uint32_t j = 0; j < rowCount1; j++) {
				if (column0[i] == column1[j]) {
					table->Add(r0->GetRow(i), r1->GetRow(j), columnCount0, columnCount1, 0);
				}
			}
		}

		delete[] column1;
		return table;
	}


	static cRowTable* Hash(cTable* r0, cTable* r1, uint16_t r0AttrOrder, uint16_t r1AttrOrder, int preallocateRowsCount, cMemory* memory) {

		uint32_t rowCount0 = r0->GetRowCount();
		uint32_t rowCount1 = r1->GetRowCount();

		uint32_t columnCount0 = r0->GetColumnCount();
		uint32_t columnCount1 = r1->GetColumnCount();


		cRowTable* table = new cRowTable(preallocateRowsCount, columnCount0 + columnCount1);

		bool firstTableIsBigger = rowCount0 > rowCount1;
		uint32_t rowCountSmaller = firstTableIsBigger ? rowCount1 : rowCount0;
		uint32_t rowCountBigger = firstTableIsBigger ? rowCount0 : rowCount1;

		uint64_t* columnSmallerTable = firstTableIsBigger ? r1->GetColumn(r1AttrOrder) : r0->GetColumn(r0AttrOrder);
		uint64_t* columnBiggerTable = firstTableIsBigger ? r0->GetColumn(r0AttrOrder) : r1->GetColumn(r1AttrOrder);

		cHashTable<TKey, TData>* hashTable = new cHashTable<TKey, TData>(rowCountSmaller * 3, memory);

		TKey key;
		TData data;


		for (int i = 0; i < rowCountSmaller; i++) {
			key = columnSmallerTable[i];
			data = firstTableIsBigger ? r1->GetRow(i) : r0->GetRow(i);

			if (!hashTable->AddWithoutRecursion(key, data)) {
				printf("Critical Error: Record %d insertion failed!\n", i);
			}
		}


		for (int i = 0; i < rowCountBigger; i++)
		{
			key = columnBiggerTable[i];
			int retCount = 0;
			TData* ret = hashTable->FindWithoutRecursion(key, retCount);

			if (ret == NULL) {
				continue;
			}


			for (int j = 0; j < retCount; j++) {
				if (firstTableIsBigger) {
					table->Add(r0->GetRow(i), ret[j], columnCount0, columnCount1, 0);
				}
				else {
					table->Add(ret[j], r1->GetRow(i), columnCount0, columnCount1, 1);
				}
			}

			delete ret;

		}

		delete hashTable;
		return table;
	}

	static cRowTable* Hash(cRowTable* r0, cTable* r1, uint16_t r0AttrOrder, uint16_t r1AttrOrder, int preallocateRowsCount, cMemory* memory) {

		uint32_t rowCount0 = r0->GetRowCount();
		uint32_t rowCount1 = r1->GetRowCount();

		uint32_t columnCount0 = r0->GetColumnCount();
		uint32_t columnCount1 = r1->GetColumnCount();


		cRowTable* table = new cRowTable(preallocateRowsCount, columnCount0 + columnCount1);

		bool firstTableIsBigger = rowCount0 > rowCount1;
		uint32_t rowCountSmaller = firstTableIsBigger ? rowCount1 : rowCount0;
		uint32_t rowCountBigger = firstTableIsBigger ? rowCount0 : rowCount1;

		if (rowCountSmaller == 0) {
			return table;
		}

		uint64_t* columnSmallerTable = firstTableIsBigger ? r1->GetColumn(r1AttrOrder) : r0->GetColumn(r0AttrOrder);
		uint64_t* columnBiggerTable = firstTableIsBigger ? r0->GetColumn(r0AttrOrder) : r1->GetColumn(r1AttrOrder);

		cHashTable<TKey, TData>* hashTable = new cHashTable<TKey, TData>(rowCountSmaller * 3, memory);

		TKey key;
		TData data;


		for (int i = 0; i < rowCountSmaller; i++) {
			key = columnSmallerTable[i];
			data = firstTableIsBigger ? r1->GetRow(i) : r0->GetRow(i);

			if (!hashTable->AddWithoutRecursion(key, data)) {
				printf("Critical Error: Record %d insertion failed!\n", i);
			}
		}


		for (int i = 0; i < rowCountBigger; i++)
		{
			key = columnBiggerTable[i];
			int retCount = 0;
			TData* ret = hashTable->FindWithoutRecursion(key, retCount);

			if (ret == NULL) {
				continue;
			}


			for (int j = 0; j < retCount; j++) {
				if (firstTableIsBigger) {
					table->Add(r0->GetRow(i), ret[j], columnCount0, columnCount1, 3);
				}
				else {
					table->Add(ret[j], r1->GetRow(i), columnCount0, columnCount1, 1);
				}
			}

			delete ret;

		}

		if (firstTableIsBigger) {
			delete[] columnBiggerTable;
		}
		else{
			delete[] columnSmallerTable;
		}
		delete hashTable;
		return table;
	}

	static cRowTable* Hash(cTable* r0, cRowTable* r1, uint16_t r0AttrOrder, uint16_t r1AttrOrder, int preallocateRowsCount, cMemory* memory) {

		uint32_t rowCount0 = r0->GetRowCount();
		uint32_t rowCount1 = r1->GetRowCount();

		uint32_t columnCount0 = r0->GetColumnCount();
		uint32_t columnCount1 = r1->GetColumnCount();


		cRowTable* table = new cRowTable(preallocateRowsCount, columnCount0 + columnCount1);

		bool firstTableIsBigger = rowCount0 > rowCount1;
		uint32_t rowCountSmaller = firstTableIsBigger ? rowCount1 : rowCount0;
		uint32_t rowCountBigger = firstTableIsBigger ? rowCount0 : rowCount1;

		if (rowCountSmaller == 0) {
			return table;
		}

		uint64_t* columnSmallerTable = firstTableIsBigger ? r1->GetColumn(r1AttrOrder) : r0->GetColumn(r0AttrOrder);
		uint64_t* columnBiggerTable = firstTableIsBigger ? r0->GetColumn(r0AttrOrder) : r1->GetColumn(r1AttrOrder);

		cHashTable<TKey, TData>* hashTable = new cHashTable<TKey, TData>(rowCountSmaller * 3, memory);

		TKey key;
		TData data;


		for (int i = 0; i < rowCountSmaller; i++) {
			key = columnSmallerTable[i];
			data = firstTableIsBigger ? r1->GetRow(i) : r0->GetRow(i);

			if (!hashTable->AddWithoutRecursion(key, data)) {
				printf("Critical Error: Record %d insertion failed!\n", i);
			}
		}


		for (int i = 0; i < rowCountBigger; i++)
		{
			key = columnBiggerTable[i];
			int retCount = 0;
			TData* ret = hashTable->FindWithoutRecursion(key, retCount);

			if (ret == NULL) {
				continue;
			}


			for (int j = 0; j < retCount; j++) {
				if (firstTableIsBigger) {
					table->Add(r0->GetRow(i), ret[j], columnCount0, columnCount1, 0);
				}
				else {
					table->Add(ret[j], r1->GetRow(i), columnCount0, columnCount1, 3);
				}
			}

			delete ret;

		}

		if (firstTableIsBigger) {
			delete[] columnSmallerTable;
		}
		else {
			delete[] columnBiggerTable;
		}
		delete hashTable;
		return table;
	}

};