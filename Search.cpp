#include <iostream>
#include <assert.h>

#include "MapReduceSearch.h"

using namespace std;
//TODO do sorting with a
#define NUM_OF_THREAD 10
#define YES_AUTO_DELETE true
int main(int argc, char* argv[])
{
    std::cout <<argc<<endl;
    if (argc <= 2){
        //print error
        return -1;
    }
    string key = argv[1];
    cout<<"this is the key:  "<< key<<endl;
    int numOfFiles = argc - 2;
    vector<string> sources;
    MapReduceSearch *mapReduceSearch= new MapReduceSearch();//TODO is it supposed to be like that??empty..
    IN_ITEMS_VEC * in_items_vec = new IN_ITEMS_VEC();
    for (int i = 2; i < argc; ++i){
        sources.push_back(argv[i]);
        SubStringKey * subStringKey = new SubStringKey(key);
        FolderNameKey * folderNameKey = new FolderNameKey(argv[i]);
        IN_ITEM * pair  = new IN_ITEM (subStringKey, folderNameKey);
        in_items_vec->push_back(*pair);
    }

    OUT_ITEMS_VEC res = RunMapReduceFramework(mapReduceSearch,in_items_vec, NUM_OF_THREAD, YES_AUTO_DELETE);
    assert(sources.size() == numOfFiles);
    for (auto it = sources.begin() ; it != sources.end(); ++it ){
        cout <<*it <<endl;
    }

//    string a = "aaaa";
//    string n = "nnn";
//    string c = "caa";
//    cout<<"(a> c): "<<(a< c)<<endl;
//    cout<<"(a> c): "<<(a>c)<<endl;

//dsfds
    return 0;
}