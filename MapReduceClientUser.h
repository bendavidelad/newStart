
// Created by shaked571 on 5/6/17.
//
#include <iostream>
#include "MapReduceClient.h"

class SubStringKey :public k1Base{
public:
    SubStringKey(std::string name);

    ~SubStringKey();

    std::string getSubString() const;

    bool operator<(const k1Base &other) const;

private:
    std::string subString;
};


class FolderNameKey: public v1Base{
public:
    FolderNameKey(std::string name);

    std::string getFolderName()const;

private:
    std::string folderName;

};

class FileName:public k2Base{
public:
    FileName(std::string name);

    ~FileName();

    std::string getFileName()const;

    bool operator<(const k2Base &other) const;


private:
    std::string fileName;

};

class OneClass: public v2Base{
public:
    OneClass();

    ~OneClass();

    int getOne()const;

private:
    int val = 1;
};

class FileNameReduce: public k3Base{
public:
    FileNameReduce(std::string name);

    ~FileNameReduce();

    std::string getFileName()const;

    bool operator<(const k3Base &other) const;


private:
    std::string fileName;
};

class NumOfFiles: public  v3Base{
public:
    NumOfFiles(int num);

    ~NumOfFiles();

    int getNumOfFiles() const;

private:
    int numFiles;



};