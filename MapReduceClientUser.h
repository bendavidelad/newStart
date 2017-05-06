
// Created by shaked571 on 5/6/17.
//
#include <iostream>
#include "MapReduceClient.h"

class SubStringKey :public k1Base{
public:
    SubStringKey(std::string name);

    ~SubStringKey();

    std::string getSubString();

    bool operator<(const k1Base &other) const;

private:
    std::string subString;
};


class FolderNameKey: public v1Base{
public:
    FolderNameKey(std::string name);

    std::string getFolderName();

private:
    std::string folderName;

};

class FileName:public k2Base{
public:
    FileName(std::string name);

    ~FileName();

    std::string getFileName();

    bool operator<(const k2Base &other) const;


private:
    std::string fileName;

};

class oneClass: public v2Base{
public:
    oneClass();

    ~oneClass();

    int getOne();

private:
    int val = 1;
};

class FileNameReduce: public k3Base{




};