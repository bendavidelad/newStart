#include <iostream>
#include <dirent.h>
#include <libltdl/lt_system.h>
#include <list>

#include "MapReduceSearch.h"
#define ERROR_MSG "MapReduceFramework Failure:"
#define ERROR_MSG_END " failed"
#define FUNC_NAME_MAP " Map"
#define FUNC_NAME_REDUCE " Reduce"

using namespace std;


MapReduceSearch::MapReduceSearch(){
}


void MapReduceSearch::Map(const k1Base *const key, const v1Base *const val) const{



    const SubStringKey* pattern = (const SubStringKey*)(key);
    const FolderNameKey* folderName =(const FolderNameKey*)(val);
    std::string patternString = pattern->getSubString();
    std::string folderNameString = folderName->getFolderName();

    list<std::string> filesInCurrentFolder;

    DIR *dir;
    struct dirent *ent;
    if ((dir = opendir (folderNameString.c_str())) != NULL) {
        /* print all the files and directories within directory */
        while ((ent = readdir (dir)) != NULL) {
            filesInCurrentFolder.push_back(ent->d_name);
        }
        closedir (dir);
    } else {
        /* could not open directory */
        perror ("");
        return;
    }


    OneClass* one;
    try{
        one = new OneClass();
    }catch(const std::bad_alloc&) {
        cerr << ERROR_MSG <<FUNC_NAME_MAP<< ERROR_MSG_END << endl;
        exit(EXIT_FAILURE);
    }
    for (std::list<std::string>::const_iterator iterator = filesInCurrentFolder.begin(), end
            = filesInCurrentFolder.end(); iterator != end; ++iterator)
    {
        if ((*iterator).find(patternString) != std::string::npos) {
            FileName* currFile;
            try{

                currFile = new FileName(*iterator);
            }catch(const std::bad_alloc&) {
                cerr << ERROR_MSG <<FUNC_NAME_MAP<< ERROR_MSG_END << endl;
                exit(EXIT_FAILURE);
            }
            Emit2(currFile, one);
        }
    }
    delete(one);
}

void MapReduceSearch::Reduce(const k2Base *const key, const V2_VEC &vals) const
{

    const FileName* fileName = dynamic_cast<const FileName*>(key);
    std::string fileNameString = fileName->getFileName();
    int counter = 0;
    for (std::vector<v2Base *>::const_iterator iterator = vals.begin(), end
            = vals.end(); iterator != end; ++iterator)
    {
        counter++;
    }
    NumOfFiles *numOfFiles;
    FileNameReduce *fileNameReduce;
    try {
        numOfFiles = new NumOfFiles(counter);
        fileNameReduce = new FileNameReduce(fileNameString);
    }catch(const std::bad_alloc&) {
        cerr << ERROR_MSG <<FUNC_NAME_REDUCE<< ERROR_MSG_END << endl;
        exit(EXIT_FAILURE);
    }
    Emit3(fileNameReduce , numOfFiles);
}