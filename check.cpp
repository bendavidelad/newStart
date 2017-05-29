#include <iostream>
#include "MapReduceClient.h"
#include "MapReduceSearch.h"

using namespace std;

int main(){

    MapReduceBase* map = new MapReduceSearch();
    const k1Base* k1 = new SubStringKey("25");
    const v1Base* v1 = new FolderNameKey("/cs/usr/bendavidelad/testEx3/grids");
//    map->Map(k1 ,v1);

    const k2Base* k2 = new FileName("/cs/usr/bendavidelad/newStart/grids/grid25");
    v2Base* v2 = new OneClass();
    V2_VEC v2_vec;
//    pair<k2Base* , v2Base*> newPair(k2,v2);
    v2_vec.push_back(v2);
    v2_vec.push_back(v2);

//    map->Reduce(k2,v2_vec);

}