
#include <thread>
#include <iostream>
#include <stdlib.h>
#include <map>
#include <list>
#include <libltdl/lt_system.h>
#include "MapReduceFramework.h"

#define KEYS_PER_THREAD 10

using namespace std;
unsigned long itemsVecPlace;
typedef std::list<pair<k2Base*, v2Base*>>  listOfPairsK2BaseV2Base;
static const std::string BAD_ALLOC_MSG = "ERROR- Bad Allocation";
MapReduceBase* mapReduceGlobal;
IN_ITEMS_VEC itemsVecGlobal;
std::map<pthread_t , listOfPairsK2BaseV2Base*> globalContainerMap ;


pthread_mutex_t mutexItemsVec = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexThreadCreation = PTHREAD_MUTEX_INITIALIZER;




/**
 *  A function that takes a chunk from the vector
 * @return the chunk as a vector
 */
IN_ITEMS_VEC* getChunkOfPairs(){
    unsigned long chunkSize = KEYS_PER_THREAD;
    if (itemsVecPlace > 0){
        // Critical Section!!!
        pthread_mutex_lock(&mutexItemsVec);
        unsigned long start = itemsVecPlace;
        itemsVecPlace -= KEYS_PER_THREAD;
        pthread_mutex_unlock(&mutexItemsVec);
        // End of Critical Section!!!
        // check if there's less threads than KEYS_PER_THREAD
        if (start < KEYS_PER_THREAD){
            chunkSize = start;
        }
        IN_ITEMS_VEC::const_iterator first = itemsVecGlobal.begin() + itemsVecPlace - chunkSize ;
        IN_ITEMS_VEC::const_iterator last =  itemsVecGlobal.end()+ itemsVecPlace;
        vector<IN_ITEM>* newVec;
        try{
            newVec = new vector<IN_ITEM>(first, last);
        }catch(const std::bad_alloc&){
            cout<<BAD_ALLOC_MSG<<endl;
            exit(EXIT_FAILURE);
        }
        return newVec;
    } else {

        return nullptr;
    }
}

void* execMap(void*)
{
    pthread_mutex_lock(&mutexThreadCreation);
    //lock(x) ->here all the threads wait for the main thread to finish making all the threads
    //unlock(x)-> and now after we past this phase ot mean no one locked x
    pthread_mutex_unlock(&mutexThreadCreation);
    while (true)
    {
        IN_ITEMS_VEC *currVec = getChunkOfPairs();
        if (currVec == nullptr)
        {
            break;
        }
        for (int i = 0; i < currVec->size(); i++)
        {
            mapReduceGlobal->Map(((*currVec)[i]).first, ((*currVec)[i]).second); // might not
            // work TODO
        }
    }
    pthread_exit(NULL); //TODO might gonna need to check

}



/**
 *
 * @param mapReduce
 * @param itemsVec
 * @param multiThreadLevel
 * @param autoDeleteV2K2
 * @return OUT_ITEMS_VEC if succecd ,Null otherwise
 */
OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase &mapReduce, IN_ITEMS_VEC &
    itemsVec, int multiThreadLevel, bool autoDeleteV2K2){
    itemsVecPlace = itemsVec.size();
    mapReduceGlobal = &mapReduce;
    itemsVecGlobal = itemsVec;
    pthread_t threads[multiThreadLevel];
    int threadCreation;
    int i;

    //mutex lock(x)-> so we connent between thread id and the container
    pthread_mutex_lock(&mutexThreadCreation);
    for(i = 0 ; i < multiThreadLevel ; i++){
        threadCreation = pthread_create(&threads[i] , NULL , execMap , NULL);
        listOfPairsK2BaseV2Base *currContainer;
        try {
            currContainer = new listOfPairsK2BaseV2Base();
        }catch (const std::bad_alloc&){
            exit(EXIT_FAILURE);
        }
        std::pair<pthread_t , listOfPairsK2BaseV2Base*> currPair(threads[i] , currContainer);
        globalContainerMap.insert(currPair);
        if (threadCreation){
            cout << "Error:unable to create thread," << threadCreation << endl;
            exit(EXIT_FAILURE);
        }
    }
    pthread_mutex_unlock(&mutexThreadCreation);
    //unlock mutex (x);


}



void Emit2 (k2Base*, v2Base*){

    pthread_t currThreadID  = pthread_self();
    listOfPairsK2BaseV2Base *currContainer =  globalContainerMap.at(currThreadID);
    std::pair<k2Base* , v2Base*> currPair(k2Base , v2Base);
    currContainer->insert(k2Base , v2Base);



}

void Emit3 (k3Base*, v3Base*){

}
