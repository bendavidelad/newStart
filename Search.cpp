#include <iostream>
#include <algorithm>
#include <stdlib.h>
#include "MapReduceSearch.h"
using namespace std;
//TODO do sorting with a
#define NUM_OF_THREAD 10
#define YES_AUTO_DELETE true
#define BAD_INPUT_MSG "Usage: <substring to search> <folders, separated by space>"
#define ERROR_MSG "MapReduceFramework Failure: main() failed"

int main(int argc, char* argv[])
{
    if (argc <= 2){
        cout << BAD_INPUT_MSG <<endl;
        return -1;
    }
    string key = argv[1];
    int numOfFiles = argc - 2;
    IN_ITEMS_VEC *in_items_vec;
    MapReduceSearch *mapReduceSearch;
    try {
        mapReduceSearch = new MapReduceSearch();
        in_items_vec = new IN_ITEMS_VEC();
    }catch(const std::bad_alloc&){
        cerr<<ERROR_MSG<<endl;
        exit(EXIT_FAILURE);
    }
    for (int i = 2; i < argc; ++i)
    {
        SubStringKey * subStringKey = new SubStringKey(key);
        FolderNameKey * folderNameKey = new FolderNameKey(argv[i]);
        IN_ITEM pair  = std::make_pair(subStringKey, folderNameKey);
        in_items_vec->push_back(pair);
    }
    OUT_ITEMS_VEC res = RunMapReduceFramework(*mapReduceSearch,*in_items_vec, NUM_OF_THREAD, YES_AUTO_DELETE);
//    printing the sorted result vector
    for (int j = 0; j < res.size(); ++j) {
        FileNameReduce& h = static_cast<FileNameReduce&>(*res[j].first);
        NumOfFiles& g = static_cast<NumOfFiles&>(*res[j].second);

        for (int k = 0 ; k < g.getNumOfFiles() ; k++){
            std::cout << h.getFileName() << " ";
        }
    }
    for(auto it = in_items_vec->begin(); it != in_items_vec->end(); ++it)
    {
        delete ((*it).first);
        delete ((*it).second);
        ((*it).first) = nullptr;
        ((*it).second)= nullptr;
    }
    for(auto it = res.begin(); it !=res.end(); ++it)
    {
        delete ((*it).first);
        delete ((*it).second);
        ((*it).first) = nullptr;
        ((*it).second)= nullptr;

    }
    delete(mapReduceSearch);
    delete(in_items_vec);
    mapReduceSearch = nullptr;
    in_items_vec = nullptr;
    return 0;
}