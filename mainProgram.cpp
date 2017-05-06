#include <iostream>
#include <assert.h>
//#include "MapReduceFramework.a"
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
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
    }
    assert(sources.size() == numOfFiles);
    for (auto it = sources.begin() ; it != sources.end(); ++it ){
        cout <<*it <<endl;
    }
    string a = "aaaa";
    string n = "nnn";
    string c = "caa";
    cout<<"(a> c): "<<(a< c)<<endl;
    cout<<"(a> c): "<<(a>c)<<endl;

//dsfds
    return 0;
}