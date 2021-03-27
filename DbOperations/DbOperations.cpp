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

    std::ifstream inFile("queries.txt");
    int numOfRows = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n') + 1;

    cQueryHandler** handlers = new cQueryHandler * [numOfRows];

    int i = 0;
    std::ifstream file("queries.txt");
    std::string inputQuery;
    while (std::getline(file, inputQuery)) {
        handlers[i++] = new cQueryHandler(inputQuery, preallocateRows);
    }

    for (int i = 0; i < numOfRows; i++) {

        auto t1 = std::chrono::high_resolution_clock::now();

        handlers[i]->Join();
        handlers[i]->Select();
        handlers[i]->Sum();


        auto t2 = std::chrono::high_resolution_clock::now();
        auto time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);

        handlers[i]->PrintData();

        printf("Duration of operations join, select and sum: %.5fs\n\n\n", time_span);
    }


    for (int i = 0; i < numOfRows; i++) {
        delete handlers[i];
    }
    delete []handlers;
}

