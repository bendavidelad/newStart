#include <iostream>
#include <dirent.h>
#include <stdlib.h>
#include <libltdl/lt_system.h>
#include <list>

#include "MapReduceClientUser.h"

using namespace std;

class MapReduceSearch : MapReduceBase{

    void Map(const k1Base *const key, const v1Base *const val){


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

        for (std::list<std::string>::const_iterator iterator = filesInCurrentFolder.begin(), end
                = filesInCurrentFolder.end(); iterator != end; ++iterator) {
            if ((*iterator).find(patternString) != std::string::npos) {
//                Emit2(*iterator , 1);
            }
        }
    }

    virtual void Reduce(const k2Base *const key, const V2_VEC &vals)
    {
//        std::string folderName = key->name();
        int counter = 0;

        for (std::vector<v2Base *>::const_iterator iterator = vals.begin(), end
                = vals.end(); iterator != end; ++iterator)
        {
            counter++;
        }
    }

    // check


};