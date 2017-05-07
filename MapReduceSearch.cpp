#include <iostream>
#include <dirent.h>
#include <stdlib.h>
#include <libltdl/lt_system.h>
#include <list>

#include "MapReduceSearch.h"

using namespace std;


MapReduceSearch::MapReduceSearch(){
}


void MapReduceSearch::Map(const k1Base *const key, const v1Base *const val){


        const SubStringKey* pattern = dynamic_cast<const SubStringKey*>(key);

        const FolderNameKey* folderName = dynamic_cast<const FolderNameKey*>(val);

        std::string patternString = pattern->getSubString();
        std::string folderNameString = folderName->getFolderName();

        list<std::string> filesInCurrentFolder;

        DIR *dir;
        struct dirent *ent;
        if ((dir = opendir (folderNameString.c_str())) != NULL) {
            /* print all the files and directories within directory */
            while ((ent = readdir (dir)) != NULL) {
                filesInCurrentFolder.push_back(ent->d_name);
                printf ("%s\n", ent->d_name);
            }
            closedir (dir);
        } else {
            /* could not open directory */
            perror ("");
            return;
        }

        std::list<std::string> filesThatMatches;
        OneClass* one = new OneClass();

        for (std::list<std::string>::const_iterator iterator = filesInCurrentFolder.begin(), end
                = filesInCurrentFolder.end(); iterator != end; ++iterator) {
            if ((*iterator).find(patternString) != std::string::npos) {
                FileName* currFile = new FileName(*iterator);
                Emit2(currFile, one);
                delete(currFile);
            }
        }
        delete(one);
    }

void MapReduceSearch::Reduce(const k2Base *const key, const V2_VEC &vals)
    {
        const FileName* fileName = dynamic_cast<const FileName*>(key);
        std::string fileNameString = fileName->getFileName();

        int counter = 0;

        for (std::vector<v2Base *>::const_iterator iterator = vals.begin(), end
                = vals.end(); iterator != end; ++iterator)
        {
            counter++;
        }
        NumOfFiles* numOfFiles = new NumOfFiles(counter);
        FileNameReduce* fileNameReduce = new FileNameReduce(fileNameString);

        Emit3(fileNameReduce , numOfFiles);
        delete(numOfFiles);
        delete(fileNameReduce);
    }


