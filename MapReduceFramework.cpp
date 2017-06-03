#include <thread>
#include <iostream>
#include <stdlib.h>
#include <map>
#include <list>
#include <unordered_map>
#include <algorithm>
#include <ios>
#include <bits/ios_base.h>
#include <fstream>
#include <iostream>
#include <zconf.h>
#include "MapReduceFramework.h"
#include "semaphore.h"
#include <unistd.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <math.h>
#include <sys/time.h>

#define GetCurrentDir getcwd


#include "MapReduceClientUser.h"

#define KEYS_PER_THREAD 10
#define EXIT_FAILURE 1


//########################################################################
// Typedefs
using namespace std;

int itemsVecPlace;
typedef std::vector<pair<k2Base*, v2Base*>>  vectorOfPairsK2BaseV2Base;
typedef std::pair<k2Base*, V2_VEC> MID_ITEM;
typedef std::vector<MID_ITEM> MID_ITEMS_VEC;
static const std::string ERROR_MSG = "MapReduceFramework Failure:";
static const std::string ERROR_MSG_END = " failed.";
static const std::string FUNC_NAME_GET_CHUNKE_OF_PAIRS= "getChunkOfPairs";
static const std::string FUNC_NAME_GET_CHUNKE_OF_PAIRS_REDUCE= "getChunkOfPairsReduce";
static const std::string FUNC_NAME_CREATING_THREADS_MAP = "creatingThreadsMap";
static const std::string FUNC_NAME_CREATING_THREADS_REDUCE= "creatingThreadsReduce";
static const std::string FUNC_NAME_RUN_MAP_REDUCE_FRAMEWORK = "RunMapReduceFramework";
static const std::string FUNC_NAME_JOIN_THREADS= "jointhreads";


//########################################################################
// Globals
MapReduceBase* mapReduceGlobal;
IN_ITEMS_VEC givenVectorK1V1Global;
int multiThreadLevelGlobal;
std::vector<pthread_t> threadsGlobal(0);
unordered_map<pthread_t , vectorOfPairsK2BaseV2Base*> preShuffleThreadsContainerK2V2Global;

std::map<k2Base*, V2_VEC , bool (*)(k2Base* , k2Base*)> postShuffleContainerK2V2VECGlobal;

unordered_map<pthread_t , OUT_ITEMS_VEC*> containerReduceK3V3Global;

std::ofstream ofs("MapReduceFramework.log", std::ofstream::out);

char buffer[26];
struct tm* tm_info;
struct timeval tv;

struct timeval startMap , endMap , startReduce , endReduce;

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
pthread_mutex_t mutexLogFile = PTHREAD_MUTEX_INITIALIZER;

unordered_map<pthread_t, pthread_mutex_t> mutexMapGlobal;



void log( const std::string &text)
{
    pthread_mutex_lock(&mutexLogFile);
    ofs << text;
    pthread_mutex_unlock(&mutexLogFile);
}


string getTime(){
    gettimeofday(&tv, NULL);
    tm_info = localtime(&tv.tv_sec);
    strftime(buffer, 26, "%d.%m.%Y %H:%M:%S", tm_info);
    return buffer;
}

/**
 *  A function that takes a chunk from the vector
 * @return the chunk as a vector
 */
IN_ITEMS_VEC* getChunkOfPairs(){
    int chunkSize = KEYS_PER_THREAD;

    if (itemsVecPlace > 0)
    {
        if(itemsVecPlace < KEYS_PER_THREAD)
        {
            chunkSize = itemsVecPlace;
        }
        // Critical Section!!!
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
        }catch(const std::bad_alloc&)
        {
            cerr<<ERROR_MSG<<FUNC_NAME_GET_CHUNKE_OF_PAIRS<<ERROR_MSG_END<<endl;
            exit(EXIT_FAILURE);
        }
        return newVec;
    } else {

        return nullptr;
    }
}




MID_ITEMS_VEC* getChunkOfPairsReduce(){
    int chunkSize = KEYS_PER_THREAD;

    if (itemsVecPlace > 0){
        if(itemsVecPlace < KEYS_PER_THREAD)
        {
            chunkSize = itemsVecPlace;
        }
        // Critical Section!!!
        pthread_mutex_lock(&mutexItemsVec);
        int start = itemsVecPlace;
        itemsVecPlace -= chunkSize;
        pthread_mutex_unlock(&mutexItemsVec);
        // End of Critical Section!!!
        // check if there's less threads than KEYS_PER_THREAD
        if (start == 0){
            return nullptr;
        }
        auto first = postShuffleContainerK2V2VECGlobal.begin();
        std::advance(first , start - chunkSize);
        auto last =  postShuffleContainerK2V2VECGlobal.begin();
        std::advance(last,start);
        vector<MID_ITEM>* newVec;
        try{
            newVec = new vector<MID_ITEM>(first, last);
        }catch(const std::bad_alloc&)
        {
            cerr<<ERROR_MSG<<FUNC_NAME_GET_CHUNKE_OF_PAIRS_REDUCE<<ERROR_MSG_END<<endl;
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
    string startMessage = "Thread ExecMap created [";
    string endMessage = "]\n";
    log(startMessage + getTime() + endMessage);

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
        delete(currVec);
    }


    startMessage = "Thread ExecMap terminated [";
    endMessage = "]\n";
    log(startMessage + getTime() + endMessage);

    pthread_exit(NULL);
}


void* execReduce(void*)
{
    string startMessage = "Thread ExecReduce created [";
    string endMessage = "]\n";
    log(startMessage + getTime() + endMessage);

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
        delete(currVec);
    }

    startMessage = "Thread ExecReduce terminated [";
    endMessage = "]\n";
    log(startMessage + getTime() + endMessage);

    pthread_exit(NULL);
}



/**
 *.
 * @return
 */
void* shuffle(void*)
{
    string startMessage = "Thread Shuffle created [";
    string endMessage = "]\n";
    log(startMessage + getTime() + endMessage);

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

    startMessage = "Thread Shuffle terminated [";
    endMessage = "]\n";
    log(startMessage + getTime() + endMessage);

    pthread_exit(NULL);
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
        }catch (const std::bad_alloc&)
        {
            cerr<<ERROR_MSG<<FUNC_NAME_CREATING_THREADS_MAP<<ERROR_MSG_END<<endl;

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
        if (threadCreation)
        {
            cerr<<ERROR_MSG<<FUNC_NAME_CREATING_THREADS_MAP<<ERROR_MSG_END<<endl;
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
            cerr<<ERROR_MSG<<FUNC_NAME_JOIN_THREADS<<ERROR_MSG_END<<endl;
            exit(EXIT_FAILURE);
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
        cerr<<ERROR_MSG<<FUNC_NAME_CREATING_THREADS_MAP<<ERROR_MSG_END<<endl;
        exit(EXIT_FAILURE);
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
            cerr<<ERROR_MSG<<FUNC_NAME_CREATING_THREADS_REDUCE<<ERROR_MSG_END<<endl;
            exit(EXIT_FAILURE);
        }
        std::pair<pthread_t, OUT_ITEMS_VEC*> pairToThreadContainer = make_pair
                (threadsGlobal[i], currContainer);
        containerReduceK3V3Global.insert(pairToThreadContainer);
        if (threadCreation)
        {
            cerr<<ERROR_MSG<<FUNC_NAME_CREATING_THREADS_REDUCE<<ERROR_MSG_END<<endl;
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
    gettimeofday(&startMap, NULL);
    log("RunMapReduceFramework started with " + to_string(multiThreadLevel) + " threads\n");
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
        cerr<<ERROR_MSG<<FUNC_NAME_RUN_MAP_REDUCE_FRAMEWORK<<ERROR_MSG_END<<endl;
        exit(EXIT_FAILURE);
    }
    //waiting until all the threads will finish
    joinThreads();
    //in this point all the treads definitely have finished
    isJoin = true;
    joinShuffle(shuffleID);
    itemsVecPlace = (int)postShuffleContainerK2V2VECGlobal.size();
    auto it = preShuffleThreadsContainerK2V2Global.begin();
    if(autoDeleteV2K2)
    {
        deletePreShuffleThreadsContainerK2V2Global();

    }
    gettimeofday(&endMap, NULL);
    double ret = ((endMap.tv_usec) - (startMap.tv_usec));
    ret *= 1000;
    log("Map and Shuffle took " + to_string(ret) + "ns\n");
    gettimeofday(&startReduce, NULL);
    // Reduce threads are starting
    pthread_mutex_lock(&mutexThreadCreation);
    creatingThreadsReduce();
    auto first = postShuffleContainerK2V2VECGlobal.begin();
    pthread_mutex_unlock(&mutexThreadCreation);
    joinThreads();
    OUT_ITEMS_VEC outContainer;
    for (int i = 0 ; i < containerReduceK3V3Global.size() ; i++)
    {
        outContainer.insert(outContainer.end(), (*containerReduceK3V3Global[threadsGlobal[i]]).begin(),
                            (*containerReduceK3V3Global[threadsGlobal[i]]).end());
    }

    std::sort(outContainer.begin(), outContainer.end(), [](const OUT_ITEM &left, const OUT_ITEM &right) {
        return (*left.first) < (*right.first);
    });
    gettimeofday(&endReduce, NULL);
    ret = ((endReduce.tv_usec) - (startReduce.tv_usec));
    ret *= 1000;
    log("Reduce took " + to_string(ret) + "ns\n");
    log("RunMapReduceFramework finished\n" );
    for(auto iter = containerReduceK3V3Global.begin() ; iter != containerReduceK3V3Global.end(); ++iter)
    {
        delete((*iter).second);
    }

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
    currContainer->push_back(currPair);
}