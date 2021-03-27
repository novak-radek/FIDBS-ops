#pragma once
#include "cTable.h"
#include "cRowTable.h"

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
					table->Add(r0->GetRow(i), r1->GetRow(j), columnCount0, columnCount1);
				}
			}
		}


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
					table->Add(r0->GetRow(i), r1->GetRow(j), columnCount0, columnCount1);
				}
			}
		}


		return table;
	}

};