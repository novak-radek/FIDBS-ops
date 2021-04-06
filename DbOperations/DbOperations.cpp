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

    cQueryHandler** handlers = new cQueryHandler * [numOfRows];
    cQueryHandler** handlersV2 = new cQueryHandler * [numOfRows];

    int i = 0;
    std::ifstream file("queries.txt");
    std::string inputQuery;
    while (std::getline(file, inputQuery)) {
        handlers[i] = new cQueryHandler(inputQuery, preallocateRows, tablesPath);
        handlersV2[i++] = new cQueryHandler(inputQuery, preallocateRows, tablesPath);
    }

    auto tStart = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numOfRows; i++) {

        auto t1 = std::chrono::high_resolution_clock::now();

        handlers[i]->Join();
        handlers[i]->Select();
        handlers[i]->Sum();


        auto t2 = std::chrono::high_resolution_clock::now();
        auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);

        //handlers[i]->PrintData();
        handlers[i]->ShortPrintData();

        printf("Duration of operations join, select and sum: %.5fs\n", time_span);

        printf("-----------------------------------------------------------------------\n");
    }


    auto tEnd = std::chrono::high_resolution_clock::now();
    auto time_span_full = std::chrono::duration_cast<std::chrono::duration<double>>(tEnd - tStart);
    printf("\nTotal duration: %.5fs\n\n", time_span_full);





    auto tStartV2 = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numOfRows; i++) {

        auto t1 = std::chrono::high_resolution_clock::now();

        handlersV2[i]->Select();
        handlersV2[i]->Join();
        handlersV2[i]->Sum();


        auto t2 = std::chrono::high_resolution_clock::now();
        auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);

        //handlers[i]->PrintData();
        handlersV2[i]->ShortPrintData();

        printf("Duration of operations join, select and sum: %.5fs\n", time_span);

        printf("-----------------------------------------------------------------------\n");
    }


    auto tEndV2 = std::chrono::high_resolution_clock::now();
    auto time_span_fullV2 = std::chrono::duration_cast<std::chrono::duration<double>>(tEndV2 - tStartV2);
    printf("\nTotal duration: %.5fs\n\n", time_span_fullV2);




    for (int i = 0; i < numOfRows; i++) {
        delete handlers[i];
    }
    delete[] handlers;

    for (int i = 0; i < numOfRows; i++) {
        delete handlersV2[i];
    }
    delete[] handlersV2;
}

