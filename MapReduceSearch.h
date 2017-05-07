#ifndef OSTARGIL3_MAPREDUCESEARCH_H
#define OSTARGIL3_MAPREDUCESEARCH_H

#include <iostream>
#include <dirent.h>
#include <stdlib.h>
#include <libltdl/lt_system.h>
#include <list>

#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include "MapReduceClientUser.h"

class MapReduceSearch : MapReduceBase{
public:

    MapReduceSearch();

    void Map(const k1Base *const key, const v1Base *const val);


    virtual void Reduce(const k2Base *const key, const V2_VEC &vals);

};

#endif //OSTARGIL3_MAPREDUCESEARCH_H
