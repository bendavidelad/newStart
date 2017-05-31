#include <iostream>
#include <assert.h>
#include <algorithm>
#include <stdlib.h>
#include "MapReduceSearch.h"
using namespace std;
//TODO do sorting with a
#define NUM_OF_THREAD 1
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
        //TODO is it supposed to be like that??empty..
        in_items_vec = new IN_ITEMS_VEC();
    }catch(const std::bad_alloc&){
        cerr<<ERROR_MSG<<endl;
        exit(EXIT_FAILURE);
    }
    for (int i = 2; i < argc; ++i){
        SubStringKey * subStringKey = new SubStringKey(key);
//        std::cerr << " key: "<<subStringKey<<"\n"<<std::fflush;
        FolderNameKey * folderNameKey = new FolderNameKey(argv[i]);
        IN_ITEM pair  = std::make_pair(subStringKey, folderNameKey);
        in_items_vec->push_back(pair);
    }
    OUT_ITEMS_VEC res = RunMapReduceFramework(*mapReduceSearch,*in_items_vec, NUM_OF_THREAD, YES_AUTO_DELETE);
    //printing the sorted result vector
    for (int j = 0; j < res.size(); ++j) {
        FileNameReduce& h = static_cast<FileNameReduce&>(*res[j].first);//TODO work on a int
        std::cout<<h.getFileName()<<endl;
    }


















    /**
    typedef std::pair<k3Base*, v3Base*> OUT_ITEM;
    typedef std::vector<OUT_ITEM> OUT_ITEMS_VEC;
    k3Base * n1 = new FileNameReduce("a");
    v3Base* a1 = new NumOfFiles(3873);
    OUT_ITEM *p1 = new OUT_ITEM (n1, a1);

    k3Base * n2 = new FileNameReduce("ab");
    v3Base* a2 = new NumOfFiles(3873);
    OUT_ITEM *p2 = new OUT_ITEM (n2, a2);

    k3Base * n3 = new FileNameReduce("ela");
    v3Base* a3 = new NumOfFiles(3873);
    OUT_ITEM *p3 = new OUT_ITEM (n3, a3);

    k3Base * n4 = new FileNameReduce("dddvg");
    v3Base* a4 = new NumOfFiles(3873);
    OUT_ITEM *p4 = new OUT_ITEM (n4, a4);


    k3Base * n5 = new FileNameReduce("ab");
    v3Base* a5 = new NumOfFiles(3873);
    OUT_ITEM *p5 = new OUT_ITEM (n5, a5);

    OUT_ITEMS_VEC out_items_vecT;
    out_items_vecT.push_back(*p1);
    out_items_vecT.push_back(*p2);
    out_items_vecT.push_back(*p3);
    out_items_vecT.push_back(*p4);
    out_items_vecT.push_back(*p5);

//    for (auto it = out_items_vecT->begin() ; it != sources.end(); ++it ){
//        FileNameReduce otherIns = (const FileNameReduce&) it->first;
//        cout << otherIns.getFileName() <<endl;
//    }
    for (int j = 0; j < out_items_vecT.size(); ++j) {
        FileNameReduce& h = static_cast<FileNameReduce&>(*out_items_vecT[j].first);

        std::cout<<h.getFileName()<<endl;
    }
//    cout<<"SSSS\n"<<endl;
//    for (int j = 0; j < out_items_vecT->size(); ++j) {
//        FileNameReduce& h = static_cast<FileNameReduce&>(*out_items_vecT[0][j].first);
//        std::cout<<h.getFileName()<<endl;
//    }

//    s->FileNameReduce(thhhh);
//    for (auto it = out_items_vecT->begin() ; it != sources.end(); ++it ){
////        FileNameReduce otherIns = (const FileNameReduce&) it->first;
//        (FileNameReduce&)it->first;
//        cout <<( (FileNameReduce&)it->first).getFileName() <<endl;
//    }



//    string a = "aaaa";
//    string n = "nnn";
//    string c = "caa";
//    cout<<"(a> c): "<<(a< c)<<endl;
//    cout<<"(a> c): "<<(a>c)<<endl;

//dsfds
*/
    return 0;
}