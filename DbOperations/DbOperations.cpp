// DbOperations.cpp : This file contains the 'main' function. Program execution begins and ends there.
//
#include <stdio.h>
#include <chrono>

#include <iostream>
#include <sstream>
#include "cQueryHandler.h"


int main()
{
    const int preallocateRows = 1000;
    //const char* tablesPath = "../tables/";
    const char* tablesPath = "";

    std::ifstream inFile("queries.txt");
    int numOfRows = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n') + 1;

    cQueryHandler** handlers0 = new cQueryHandler * [numOfRows];
    cQueryHandler** handlers1 = new cQueryHandler * [numOfRows];
    cQueryHandler** handlers2 = new cQueryHandler * [numOfRows];
    cQueryHandler** handlers3 = new cQueryHandler * [numOfRows];

    int i = 0;
    std::ifstream file("queries.txt");
    std::string inputQuery;
    while (std::getline(file, inputQuery)) {
        handlers0[i] = new cQueryHandler(inputQuery, preallocateRows, tablesPath);
        handlers1[i] = new cQueryHandler(inputQuery, preallocateRows, tablesPath);
        handlers2[i] = new cQueryHandler(inputQuery, preallocateRows, tablesPath);
        handlers3[i++] = new cQueryHandler(inputQuery, preallocateRows, tablesPath);
    }
    auto tmp1 = std::chrono::high_resolution_clock::now();
    auto tmp2 = std::chrono::high_resolution_clock::now();
    auto hash = std::chrono::duration_cast<std::chrono::duration<double>>(tmp1 - tmp2);
    auto hashSelect = std::chrono::duration_cast<std::chrono::duration<double>>(tmp1 - tmp2);
    auto nestedloop = std::chrono::duration_cast<std::chrono::duration<double>>(tmp1 - tmp2);
    auto nestedloopSelect = std::chrono::duration_cast<std::chrono::duration<double>>(tmp1 - tmp2);

    cMemory* memory = new cMemory(10*1024*1024);

    for (int i = 0; i < numOfRows; i++) {

        handlers0[i]->PrintQuery();
        printf("Hash join:\n");
        auto t1 = std::chrono::high_resolution_clock::now();

        handlers0[i]->HashJoin(memory);
        handlers0[i]->Select();
        handlers0[i]->Sum();
        handlers0[i]->ShortPrintData();
        
        delete handlers0[i];

        auto t2 = std::chrono::high_resolution_clock::now();
        auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        hash += time_span;
        printf("Duration of operations join, select and sum: %.5fs\n\n", time_span);

        memory->Reset();

        printf("Hash join, select first:\n");
        t1 = std::chrono::high_resolution_clock::now();

        handlers1[i]->Select();
        handlers1[i]->HashJoin(memory);
        handlers1[i]->Sum();
        handlers1[i]->ShortPrintData();
        
        delete handlers1[i];

        t2 = std::chrono::high_resolution_clock::now();
        time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        hashSelect += time_span;
        printf("Duration of operations join, select and sum: %.5fs\n\n", time_span);

        memory->Reset();

        printf("Nested-loop join:\n");
        t1 = std::chrono::high_resolution_clock::now();

        handlers2[i]->Join();
        handlers2[i]->Select();
        handlers2[i]->Sum();
        handlers2[i]->ShortPrintData();

        delete handlers2[i];

        t2 = std::chrono::high_resolution_clock::now();
        time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        nestedloop += time_span;
        printf("Duration of operations join, select and sum: %.5fs\n\n", time_span);

        printf("Nested-loop join, select first:\n");
        t1 = std::chrono::high_resolution_clock::now();

        handlers3[i]->Select();
        handlers3[i]->Join();
        handlers3[i]->Sum();
        handlers3[i]->ShortPrintData();

        delete handlers3[i];

        t2 = std::chrono::high_resolution_clock::now();
        time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
        nestedloopSelect += time_span;
        printf("Duration of operations join, select and sum: %.5fs\n\n", time_span);

        printf("-----------------------------------------------------------------------\n");
        
    }


    printf("\nHash join total duration: %.5fs\n", hash);
    printf("Hash join, select first total duration: %.5fs\n", hashSelect);
    printf("Nested-loop join total duration: %.5fs\n", nestedloop);
    printf("Nested-loop join, select firs total duration: %.5fs\n", nestedloopSelect);


    delete[] handlers0;
    delete[] handlers1;
    delete[] handlers2;
    delete[] handlers3;
    delete memory;
}

