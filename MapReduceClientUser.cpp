
#include <iostream>
#include "MapReduceClient.h"
#include "MapReduceClientUser.h"


/**
 *
 * SubStringKey class
 * #####################################################################################
 */

SubStringKey::SubStringKey(std::string name):subString(name){}

std::string SubStringKey::getSubString(){
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

std::string FolderNameKey::getFolderName(){
    return folderName;
}


/**
 *
 * oneClass class
 * #####################################################################################
 */
int oneClass::getOne() {
    return val;
}

oneClass::~oneClass() {
}

oneClass::oneClass() {
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
std::string FileName::getFileName() {
    return fileName;
}

bool FileName::operator<(const k2Base &other) const {
    FileName otherIns = (const FileName&) other;

    return this->fileName < otherIns.getFileName();
}

