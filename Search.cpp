#include <iostream>
#include <assert.h>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "MapReduceClientUser.h"
#include "MapReduceSearch.cpp" //todo make it

using namespace std;
//TODO dp sorting with a
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
    for (int i = 2; i < argc; ++i){
        sources.push_back(argv[i]);
        SubStringKey * subStringKey = new SubStringKey(key);
        FolderNameKey * folderNameKey = new FolderNameKey(argv[i]);
        IN_ITEM * pair  = new IN_ITEM (subStringKey, folderNameKey);
        MapReduceBase mapReduceBase  = new MapReduceBase(const subStringKey,const folderNameKey);
        OUT_ITEMS_VEC res = RunMapReduceFramework()


    }
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