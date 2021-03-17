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
					uint64_t* result = new uint64_t[columnCount0 + columnCount1];

					uint64_t* row0 = r0->GetRow(i);
					uint64_t* row1 = r1->GetRow(j);

					std::copy(row0, row0 + columnCount0, result);
					std::copy(row1, row1 + columnCount1, result + columnCount0);

					table->Add(result);
				}
			}
		}


		return table;
	}

};