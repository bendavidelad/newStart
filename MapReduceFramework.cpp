#include <thread>
#include <iostream>
#include <stdlib.h>
#include <map>
#include <list>
#include <libltdl/lt_system.h>
#include <unordered_map>
#include <algorithm>
#include "MapReduceFramework.h"
#include "semaphore.h"
#include "GridKeysValues.hpp"

#include "MapReduceClientUser.h"

#define KEYS_PER_THREAD 10


//########################################################################
// Typedefs
using namespace std;

int itemsVecPlace;
typedef std::vector<pair<k2Base*, v2Base*>>  vectorOfPairsK2BaseV2Base;
typedef std::pair<k2Base*, V2_VEC> MID_ITEM;
typedef std::vector<MID_ITEM> MID_ITEMS_VEC;
static const std::string BAD_ALLOC_MSG = "ERROR- Bad Allocation";

//########################################################################
// Globals
MapReduceBase* mapReduceGlobal;
IN_ITEMS_VEC givenVectorK1V1Global;
int multiThreadLevelGlobal;
std::vector<pthread_t> threadsGlobal(0);
unordered_map<pthread_t , vectorOfPairsK2BaseV2Base*> preShuffleThreadsContainerK2V2Global;

std::map<k2Base*, V2_VEC , bool (*)(k2Base* , k2Base*)> postShuffleContainerK2V2VECGlobal;

unordered_map<pthread_t , OUT_ITEMS_VEC*> containerReduceK3V3Global;

bool isJoin = false;



//########################################################################
// Semaphores
const int semaphoreShuffleInt = 0;
sem_t semaphoreShuffle;
const int WORK_BETWEEN_THE_PROCESSES = 0;

//########################################################################
//  Mutex
pthread_mutex_t mutexItemsVec = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexThreadCreation = PTHREAD_MUTEX_INITIALIZER;
unordered_map<pthread_t, pthread_mutex_t> mutexMapGlobal;





/**
 *  A function that takes a chunk from the vector
 * @return the chunk as a vector
 */
IN_ITEMS_VEC* getChunkOfPairs(){
    int chunkSize = KEYS_PER_THREAD;
    cout<<"tis is itemsVecPlace: "<<itemsVecPlace<<endl;

    if (itemsVecPlace > 0)
    {
        cout<<"this is itemsVecPlace: "<<itemsVecPlace<<endl;
        // Critical Section!!!
        if(itemsVecPlace < KEYS_PER_THREAD)
        {
            chunkSize = itemsVecPlace;
        }
        pthread_mutex_lock(&mutexItemsVec);
        int start = itemsVecPlace;
        itemsVecPlace -= chunkSize;
        pthread_mutex_unlock(&mutexItemsVec);
        // End of Critical Section!!!
        // check if there's less threads than KEYS_PER_THREAD
        if (start == 0){
            return nullptr;
        }
        IN_ITEMS_VEC::const_iterator first = givenVectorK1V1Global.begin() + start - chunkSize;
        IN_ITEMS_VEC::const_iterator last =  givenVectorK1V1Global.begin() + start;
        vector<IN_ITEM>* newVec;
        try
        {
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




MID_ITEMS_VEC* getChunkOfPairsReduce(){
    int chunkSize = KEYS_PER_THREAD;
    cout<<"tis is itemsVecPlace in Reduce: "<<itemsVecPlace<<endl;

    if (itemsVecPlace > 0){
        if(itemsVecPlace < KEYS_PER_THREAD)
        {
            chunkSize = itemsVecPlace;
        }
        // Critical Section!!!
        pthread_mutex_lock(&mutexItemsVec);
        int start = itemsVecPlace;
        itemsVecPlace -= KEYS_PER_THREAD;
        pthread_mutex_unlock(&mutexItemsVec);
        // End of Critical Section!!!
        // check if there's less threads than KEYS_PER_THREAD
        auto first = postShuffleContainerK2V2VECGlobal.begin();
        std::advance(first , start - chunkSize);
        auto last =  postShuffleContainerK2V2VECGlobal.begin();
        std::advance(last,start);
        vector<MID_ITEM>* newVec;
        try{
            newVec = new vector<MID_ITEM>(first, last);
        }catch(const std::bad_alloc&){
            cout<<BAD_ALLOC_MSG<<endl;
            exit(EXIT_FAILURE);
        }
        return newVec;
    } else {

        return nullptr;
    }
}




//

/**
 *
 * @return
 */
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
            sem_post(&semaphoreShuffle);
            break;
        }
        for (int i = 0; i < currVec->size(); i++)
        {
            mapReduceGlobal->Map(((*currVec)[i]).first, ((*currVec)[i]).second); // might not
        }
    }
    pthread_exit(NULL); //TODO might gonna need to check
}


void* execReduce(void*)//TODO update
{
    pthread_mutex_lock(&mutexThreadCreation);
    //lock(x) ->here all the threads wait for the main thread to finish making all the threads
    //unlock(x)-> and now after we past this phase ot mean no one locked x
    pthread_mutex_unlock(&mutexThreadCreation);

    while (true)
    {
        MID_ITEMS_VEC *currVec = getChunkOfPairsReduce();

        if (currVec == nullptr)
        {
            break;
        }
        for (int i = 0; i < currVec->size(); i++)
        {
            mapReduceGlobal->Reduce(((*currVec)[i]).first, ((*currVec)[i]).second);
        }
    }
    pthread_exit(NULL); //TODO might gonna need to check
}



/**
 *.
 * @return
 */
void* shuffle(void*)
{
    while(!isJoin)
    {
        for (unsigned long i = 0; i < threadsGlobal.size(); i++)
        {
            if ((preShuffleThreadsContainerK2V2Global[threadsGlobal[i]]->size() > 0))
            {
                pthread_t currThreadID  = threadsGlobal[i];
                //locking the critical code section-> the mutual resource

                pthread_mutex_lock(&mutexMapGlobal[currThreadID]);
                pair<k2Base*, v2Base*> currPair = preShuffleThreadsContainerK2V2Global.at(threadsGlobal[i])->back();
                preShuffleThreadsContainerK2V2Global.at(threadsGlobal[i])->pop_back();
                //unlock the mutex
                pthread_mutex_unlock(&mutexMapGlobal[currThreadID]);
                postShuffleContainerK2V2VECGlobal[currPair.first].push_back(currPair.second);
                sem_wait(&semaphoreShuffle);
                break;
            }
        }
    }
    //after the join of the threads
    for (unsigned long k = 0; k < threadsGlobal.size(); k++)
    {
        if ((preShuffleThreadsContainerK2V2Global[threadsGlobal[k]]->size() > 0))
        {
            while (preShuffleThreadsContainerK2V2Global[threadsGlobal[k]]->size() != 0)
            {
                pair<k2Base*, v2Base*> currPair = preShuffleThreadsContainerK2V2Global.at(threadsGlobal[k])->back();
                preShuffleThreadsContainerK2V2Global.at(threadsGlobal[k])->pop_back();
                postShuffleContainerK2V2VECGlobal[currPair.first].push_back(currPair.second);
                sem_wait(&semaphoreShuffle);
            }
        }
    }

    pthread_exit(NULL); //TODO might gonna need to check
}
/**
 * creating all the threds,mutex and their containers
 */
void creatingThreadsMap()
{
    int threadCreation;
    int i;
    for(i = 0 ; i < multiThreadLevelGlobal ; i++)
    {
        threadCreation = pthread_create(&threadsGlobal[i] , NULL , execMap , NULL);
        vectorOfPairsK2BaseV2Base *currContainer;
        try {
            currContainer = new vectorOfPairsK2BaseV2Base();
        }catch (const std::bad_alloc&){
            exit(EXIT_FAILURE);
        }
        std::pair<pthread_t, vectorOfPairsK2BaseV2Base*> pairToThreadContainer = make_pair(threadsGlobal[i],
                                                                                           currContainer);
        preShuffleThreadsContainerK2V2Global.insert(pairToThreadContainer);
        //init the mutex pf the thread
        pthread_mutex_t thread_mutex;
        pthread_mutex_init(&thread_mutex, NULL);
        //make the pair for the mutex map that
        std::pair<pthread_t, pthread_mutex_t> pairToMutexMap = make_pair(threadsGlobal[i], thread_mutex);
        mutexMapGlobal.insert(pairToMutexMap);
        if (threadCreation){
            cout << "Error:unable to create thread," << threadCreation << endl;
            exit(EXIT_FAILURE);
        }
    }
}
/**
 * joinning the threads
 */
void joinThreads()
{

    for(int j = 0; j< multiThreadLevelGlobal; ++j)
    {
        int rc;
        void* status;
        rc = pthread_join(threadsGlobal[j], &status);
        if(rc)
        {
            cerr<<"error"<<rc<<endl;//TODO make a standard error
        }
    }
}
/**
 * join the shuffle
 * @param shuffleID  the shuffle id
 */
void joinShuffle(pthread_t shuffleID)
{
    int rc;
    void* status;
    //join to the shuffle
    rc = pthread_join(shuffleID, &status);
    if(rc)
    {
        cerr<<"error"<<rc<<endl;//TODO make a standard error
    }
}

void creatingThreadsReduce()
{
    threadsGlobal.clear();
    int threadCreation, i;
    for(i = 0 ; i < multiThreadLevelGlobal ; i++)
    {
        threadCreation = pthread_create(&threadsGlobal[i] , NULL , execReduce , NULL);
        OUT_ITEMS_VEC *currContainer;
        try {
            currContainer = new OUT_ITEMS_VEC();
        }catch (const std::bad_alloc&){
            exit(EXIT_FAILURE);//TODO format system call
        }
        std::pair<pthread_t, OUT_ITEMS_VEC*> pairToThreadContainer = make_pair
                (threadsGlobal[i], currContainer);
        containerReduceK3V3Global.insert(pairToThreadContainer);
        if (threadCreation)
        {
            cout << "Error:unable to create thread," << threadCreation << endl;
            exit(EXIT_FAILURE);
        }
    }
}

void deletePreShuffleThreadsContainerK2V2Global()
{
    for (int j = 0; j < multiThreadLevelGlobal; ++j)
    {
        auto it = (preShuffleThreadsContainerK2V2Global[threadsGlobal[j]])->begin();
        for(it; it!= (preShuffleThreadsContainerK2V2Global[threadsGlobal[j]])->end(); ++it)
        {
            delete ((*it).first);
            delete((*it).second);
        }
        delete preShuffleThreadsContainerK2V2Global[threadsGlobal[j]];
    }
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
    postShuffleContainerK2V2VECGlobal = map<k2Base*,V2_VEC,bool(*)(k2Base*,k2Base*)>(
            [](k2Base* k2Base1, k2Base* k2Base2)->bool {
                return *k2Base1 < *k2Base2;
            }
    );
    itemsVecPlace = (int)itemsVec.size();
    mapReduceGlobal = &mapReduce;
    multiThreadLevelGlobal = multiThreadLevel;
    //a var that holds the vector of k1,v1
    givenVectorK1V1Global  = itemsVec;
    threadsGlobal.resize((unsigned long)multiThreadLevel);
    //Initial the shuffle semaphore
    sem_init(&semaphoreShuffle,WORK_BETWEEN_THE_PROCESSES,semaphoreShuffleInt);
    //mutex lock(x)-> so we connect between thread id and the container
    pthread_mutex_lock(&mutexThreadCreation);
    creatingThreadsMap();
    //unlock mutex (x);
    pthread_mutex_unlock(&mutexThreadCreation);
    //the shuffle will activate only after the first emit (which will post the semaphore)
    sem_wait(&semaphoreShuffle);
    pthread_t shuffleID;
    int threadCreation = pthread_create(&shuffleID , NULL , shuffle , NULL);
    if (threadCreation)
    {
        cout << "Error:unable to create thread," << threadCreation << endl;
        exit(EXIT_FAILURE);
    }
    //waiting until all the threads will finish
    joinThreads();
    //in this point all the treads definitely have finished
    isJoin = true;
    joinShuffle(shuffleID);

    itemsVecPlace = (int)postShuffleContainerK2V2VECGlobal.size();

    auto it = preShuffleThreadsContainerK2V2Global.begin();
//    for (it; it != preShuffleThreadsContainerK2V2Global.end(); ++it)
//    {
//        vectorOfPairsK2BaseV2Base * vec = ((*it).second);
//        auto it2 = (*vec).begin();
//        for(it2; it2 !=((*it).second)->end(); ++it2)
//        {
//            RowMaxVal =
////            int i = dynamic_cast<int>((((*it2).first))->getRowNum());
//            cout<< "Row num " << ((RowMaxVal*)(*it2).first)->getRowNum()<<endl;
//            cout<<  "Value " <<  ((RowMaxVal*)(*it2).first)->getValue()<<endl;
//            cout<<((Index*)(*it2).second)->getIndex()<<endl;
//
//        }
//        cout  <<  ((RowMaxVal*)((*it).first))->getRowNum()<< endl;
//    }
    deletePreShuffleThreadsContainerK2V2Global();

    pthread_mutex_lock(&mutexThreadCreation);
    creatingThreadsReduce();
    auto first = postShuffleContainerK2V2VECGlobal.begin();
    pthread_mutex_unlock(&mutexThreadCreation);
    joinThreads();

    OUT_ITEMS_VEC outContainer;

//    for ( auto iter = containerReduceK3V3Global.begin(); iter != containerReduceK3V3Global.end();
//          ++iter )
//        std::cout << iter->second->size() << endl;


    for (int i = 0 ; i < containerReduceK3V3Global.size() ; i++){
        outContainer.insert(outContainer.end(), (*containerReduceK3V3Global[threadsGlobal[i]]).begin(),
                            (*containerReduceK3V3Global[threadsGlobal[i]]).end());
    }
    std::sort(outContainer.begin(), outContainer.end(), [](const OUT_ITEM &left, const OUT_ITEM &right) {
        return (*left.first) < (*right.first);
    });

    return outContainer;
}



void Emit2 (k2Base* k2, v2Base* v2){

    pthread_t currThreadID  = pthread_self();
    vectorOfPairsK2BaseV2Base *currContainer =  preShuffleThreadsContainerK2V2Global.at(currThreadID);
    std::pair<k2Base* , v2Base*> currPair = make_pair(k2 , v2);
    //locking the critical code section-> the mutual resource
    pthread_mutex_lock(&mutexMapGlobal[currThreadID]);
    currContainer->push_back(currPair);
    //unlock the mutex
    pthread_mutex_unlock(&mutexMapGlobal[currThreadID]);
    sem_post(&semaphoreShuffle);
}

void Emit3 (k3Base* k3, v3Base* v3){

    pthread_t currThreadID  = pthread_self();
    OUT_ITEMS_VEC *currContainer =  containerReduceK3V3Global.at(currThreadID);
    std::pair<k3Base* , v3Base*> currPair = make_pair(k3 , v3);
    FileName* fileName = (FileName*)(k3);
//    cout << fileName->getFileName() << endl;

    currContainer->push_back(currPair);
}