
#include <thread>
#include <iostream>
#include <stdlib.h>
#include <map>
#include <list>
#include <libltdl/lt_system.h>
#include <unordered_map>
#include "MapReduceFramework.h"
#include "semaphore.h"
#include "MapReduceClientUser.h"

#define KEYS_PER_THREAD 10


//########################################################################
// Typedefs
using namespace std;
unsigned long itemsVecPlace;
typedef std::list<pair<k2Base*, v2Base*>>  listOfPairsK2BaseV2Base;
typedef std::list<pair<k3Base*, v3Base*>>  listOfPairsK3BaseV3Base;
typedef std::pair<k2Base*, V2_VEC> MID_ITEM;
typedef std::vector<MID_ITEM> MID_ITEMS_VEC;

static const std::string BAD_ALLOC_MSG = "ERROR- Bad Allocation";

//########################################################################
// Globals
MapReduceBase* mapReduceGlobal;
IN_ITEMS_VEC itemsVecGlobal;
int multiThreadLevelGlobal;
std::vector<pthread_t> threadsGlobal(0);
unordered_map<pthread_t , listOfPairsK2BaseV2Base*> containerMapGlobal;
std::map<k2Base,std::list<v2Base>> shuffleMapGlobal;
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
        IN_ITEMS_VEC::const_iterator first = itemsVecGlobal.begin() + itemsVecPlace - chunkSize;
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
/**
 *
 * @return
 */
MID_ITEMS_VEC* getChunkOfPairsReduce()
{
    itemsVecPlace = shuffleMapGlobal.size();
    unsigned long chunkSize = KEYS_PER_THREAD;
    if (itemsVecPlace > 0)
    {
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

        std::map<k2Base,std::list<v2Base>>::const_iterator first = shuffleMapGlobal.begin() + itemsVecPlace - chunkSize ;
        std::map<k2Base,std::list<v2Base>>::const_iterator last =  shuffleMapGlobal.end()+ itemsVecPlace;
        vector<IN_ITEM>* newVec;
        try{
            newVec = new vector<IN_ITEM>(first, last);
        }catch(const std::bad_alloc&){
            cout<<BAD_ALLOC_MSG<<endl;
            exit(EXIT_FAILURE);
        }
        return newVec;
    } else
    {
        return nullptr;
    }
}


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
            // work TODO
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
            if ((containerMapGlobal[threadsGlobal[i]]->size() > 0))
            {
                pair<k2Base*, v2Base*> currPair = containerMapGlobal.at(threadsGlobal[i])->back();
                pthread_t currThreadID  = threadsGlobal[i];
                //locking the critical code section-> the mutual resource
                pthread_mutex_lock(&mutexMapGlobal[currThreadID]);
                containerMapGlobal.at(threadsGlobal[i])->pop_back();
                if (shuffleMapGlobal.count(*currPair.first))
                {
                    shuffleMapGlobal.at(*currPair.first).push_back(*currPair.second);
                }
                else
                {
                    std::list<v2Base> *listV2Base = new list<v2Base>();
                    listV2Base->push_back(*currPair.second);
                    std::pair<k2Base, std::list<v2Base>> newPair = make_pair(currPair.first, listV2Base);
                    shuffleMapGlobal.insert(newPair);
                }
                //unlock the mutex
                pthread_mutex_unlock(&mutexMapGlobal[currThreadID]);
                sem_wait(&semaphoreShuffle);
                break;
            }
        }
    }
    //after the join of the threads
    for (unsigned long k = 0; k < threadsGlobal.size(); k++)
    {
        if ((containerMapGlobal[threadsGlobal[k]]->size() > 0))
        {
            while (containerMapGlobal[threadsGlobal[k]]->size() != 0)
            {
                pair<k2Base*, v2Base*> currPair = containerMapGlobal.at(threadsGlobal[k])->back();
                containerMapGlobal.at(threadsGlobal[k])->pop_back();
                if (shuffleMapGlobal.count(*currPair.first))
                {
                    shuffleMapGlobal.at(*currPair.first).push_back(*currPair.second);
                }
                else
                {
                    std::list<v2Base> *listV2Base = new list<v2Base>();
                    listV2Base->push_back(*currPair.second);
                    std::pair<k2Base, std::list<v2Base>> newPair = make_pair(currPair.first, listV2Base);
                    shuffleMapGlobal.insert(newPair);
                }
                sem_wait(&semaphoreShuffle);
            }
        }
    }

    pthread_exit(NULL); //TODO might gonna need to check
}
/**
 * creating all the threds,mutex and their containers
 */
void creatingThreadsShuffle()
{
    int threadCreation;
    int i;
    for(i = 0 ; i < multiThreadLevelGlobal ; i++)
    {
        threadCreation = pthread_create(&threadsGlobal[i] , NULL , execMap , NULL);
        listOfPairsK2BaseV2Base *currContainer;
        try {
            currContainer = new listOfPairsK2BaseV2Base();
        }catch (const std::bad_alloc&){
            exit(EXIT_FAILURE);
        }
        std::pair<pthread_t, listOfPairsK2BaseV2Base*> pairToThreadContainer = make_pair(threadsGlobal[i],
                                                                                         currContainer);
        containerMapGlobal.insert(pairToThreadContainer);
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
    int threadCreation, i;
    for(i = 0 ; i < multiThreadLevelGlobal ; i++)
    {
        threadCreation = pthread_create(&threadsGlobal[i] , NULL , execReduce , NULL);
        listOfPairsK3BaseV3Base *currContainer;
        try {
            currContainer = new listOfPairsK3BaseV3Base();
        }catch (const std::bad_alloc&){
            exit(EXIT_FAILURE);//TODO format system call
        }
        std::pair<pthread_t, listOfPairsK3BaseV3Base*> pairToThreadContainer = make_pair
                (threadsGlobal[i], currContainer);
        containerMapGlobal.insert(pairToThreadContainer);
        if (threadCreation)
        {
            cout << "Error:unable to create thread," << threadCreation << endl;
            exit(EXIT_FAILURE);
        }
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
    itemsVecPlace = itemsVec.size();
    mapReduceGlobal = &mapReduce;
    multiThreadLevelGlobal = multiThreadLevel;
    //a var that holds the list of k1,v1
    itemsVecGlobal = itemsVec;
    threadsGlobal.resize((unsigned long)multiThreadLevel);
    //Initial the shuffle semaphore
    sem_init(&semaphoreShuffle,WORK_BETWEEN_THE_PROCESSES,semaphoreShuffleInt);
    //mutex lock(x)-> so we connect between thread id and the container
    pthread_mutex_lock(&mutexThreadCreation);
    creatingThreadsShuffle();
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

    for (int j = 0; j < multiThreadLevelGlobal; ++j)
    {
        delete containerMapGlobal[threadsGlobal[j]];
    }

    pthread_mutex_lock(&mutexThreadCreation);
    creatingThreadsReduce();
    pthread_mutex_unlock(&mutexThreadCreation);





}



void Emit2 (k2Base* k2, v2Base* v2){

    pthread_t currThreadID  = pthread_self();
    listOfPairsK2BaseV2Base *currContainer =  containerMapGlobal.at(currThreadID);
    std::pair<k2Base* , v2Base*> currPair = make_pair(k2 , v2);
    //locking the critical code section-> the mutual resource
    pthread_mutex_lock(&mutexMapGlobal[currThreadID]);
    currContainer->push_back(currPair);
    //unlock the mutex
    pthread_mutex_unlock(&mutexMapGlobal[currThreadID]);
    sem_post(&semaphoreShuffle);
}

void Emit3 (k3Base*, v3Base*){


}
