
#include <iostream>
#include "MapReduceClientUser.h"


/**
 *
 * SubStringKey class
 * #####################################################################################
 */

SubStringKey::SubStringKey(std::string name):subString(name){}

std::string SubStringKey::getSubString()const{
    return subString;
}
bool SubStringKey::operator<(const k1Base &other) const {
    SubStringKey otherIns = (const SubStringKey&) other;

    return this->subString < otherIns.getSubString();
    }

SubStringKey::~SubStringKey() {

}






/**
 *
 * FolderNameKey class
 * #####################################################################################
 */
//constructor
FolderNameKey::FolderNameKey(std::string name): folderName(name) {}

std::string FolderNameKey::getFolderName()const{
    return folderName;
}


/**
 *
 * oneClass class
 * #####################################################################################
 */
int OneClass::getOne()const {
    return val;
}

OneClass::~OneClass() {
}

OneClass::OneClass() {
}



/**
 *
 * FileName class
 * #####################################################################################
 */
FileName::FileName(std::string name) {

}
FileName::~FileName(){

}
std::string FileName::getFileName() const{
    return fileName;
}

bool FileName::operator<(const k2Base &other) const {
    FileName otherIns = (const FileName&) other;

    return this->fileName < otherIns.getFileName();
}

/**
 *
 * FileNameReduce class
 * #####################################################################################
 */

bool FileNameReduce::operator<(const k3Base &other) const {

    FileNameReduce otherIns = (const FileNameReduce&) other;
    return this->fileName < otherIns.getFileName();

}

std::string FileNameReduce::getFileName() const{
    return this->fileName;
}

FileNameReduce::~FileNameReduce() {
}

FileNameReduce::FileNameReduce(std::string name) :fileName(name){
    std::cout<< name;
}


/**
 *
 * NumOfFiles class
 * #####################################################################################
 */

NumOfFiles::NumOfFiles(int num): numFiles(num)
{
}

NumOfFiles::~NumOfFiles()
{
}

int NumOfFiles::getNumOfFiles() const
{
    return numFiles;
}


