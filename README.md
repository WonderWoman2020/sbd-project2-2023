# B-Tree index implementation

## Table of contents

1. [Description](#description)
2. [Who may find it useful](#who-may-find-it-useful)
3. [How B-Tree and data is presented](#how-b---tree-and-data-is-presented)
4. [How to run](#how-to-run)
5. [Input commands and parameters](#input-commands)
6. [Index and data files structure](#index-and-data-file-structure)
7. [Memory management](#memory-management)


## Description

Second project of Database Structures subject - B-Tree index file to control data file.

This is a Java CLI application that runs a database-like structure - B-Tree index with associated data file. The data file stores simple records consisting of an 8-byte key and 2 integer values (each 4-bytes long). The index file contains B-Tree structure, which stores entries consisting of an aforementioned data record key and a page number in the data file, on which the record is written.

**Briefly on how B-Tree indexing works**

The index file controls the data file. All CRUD operations on the data file must go through the B-Tree, so it would contain information about locations of the records in the large data file and store these entries sorted. Index file helps in reducing IO operations on the big file, which, without it, would have to be read sequentially to find the desired record that we want to read. B-Tree ensures low number of disk reads to find an entry (with record key and its page number) thanks to its broad structure, consisting of nodes with multiple child-nodes pointers, and a requirement to visit only max. 'h' number of nodes to find an entry, where 'h' is the tree height.

## Who may find it useful

It is an educational project that aims to show how B-Tree indexing works and how it optimizes IO operations and memory management. It can be useful to students that are just starting to learn about that structure - it can help you visualize and experiment with it, which can ease a way through and make the learning process more enjoyable :) You can also study the implementation details if you want, to clarify some ideas on how to implement the structure yourselves.

## How B-Tree and data is presented

Work in progress, will be updated soon!

