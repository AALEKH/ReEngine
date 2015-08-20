// For Vector Array 
#include <vector>
#include <iostream>
#include <string>

#include <stdio.h>
#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fstream>    
#include <typeinfo>

// For JSON Parsing stuff, see this: https://github.com/nlohmann/json, quite the best for C++
#include "json/src/json.hpp"
#include <string>
#include <sstream>

#include "redisoperations.hpp"

#include <typeinfo>
// For Redis Cluster Functioning
#include <queue>
#include <thread>
#include <assert.h>
#include "createcluster.h"
#include "cpp-hiredis-cluster/include/hirediscommand.h"
#include <boost/functional/hash.hpp>
 
void *connection_handler(void *);

using namespace nlohmann;
using namespace RedisCluster;

void InsertColumnNameCommand(const char *address, int port, char *table_database,const char *data)
{
    Cluster<redisContext>::ptr_t cluster_p;
    redisReply * reply;
    redisReply * reply2;
    cluster_p = HiredisCommand<>::createCluster( address, port );
    reply = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "SADD %s %s", table_database, data));
    if( reply->type == REDIS_REPLY_STATUS  || reply->type == REDIS_REPLY_ERROR )
    {
      my_printf("Either REDIS_REPLY_ERROR or REDIS_REPLY_STATUS");
    }
    freeReplyObject( reply );
    delete cluster_p;
}

std::vector<const char *> ReadAllColumns(const char *address, int port, char *table_database) {
  std::vector<const char *> vect2;
  // Cluster<redisContext>::ptr_t cluster_p;
  // redisReply * reply;
  Cluster<redisContext>::ptr_t cluster_p = HiredisCommand<>::createCluster( address, port );
  struct redisReply **reply2 = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "SMEMBERS %s", table_database))->element;
  size_t length = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "SMEMBERS %s", table_database))->elements;
  for (int j = 0; j <= length-1; j++ ){
    vect2.push_back(reply2[j]->str);
  }
  delete cluster_p;
  return vect2;
}

long long countColumns(const char *address, int port, char *table_database) {
  // redisReply * reply;
  int returnCount;
  Cluster<redisContext>::ptr_t cluster_p = HiredisCommand<>::createCluster( address, port );
  long long count = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "SCARD %s", table_database))->integer;
  delete cluster_p;
  return count;
}

// HSET myhash field1 "Hello"
void InsertColumnData(const char *address, int port, const char *column_table,const char *data, int primary_key) {
    Cluster<redisContext>::ptr_t cluster_p;
    redisReply * reply;
    cluster_p = HiredisCommand<>::createCluster( address, port );

    my_printf("Finished Insert Column Function: %s", data);
    reply = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "HSET %s %i %s", column_table, primary_key, data));
    if( reply->type == REDIS_REPLY_STATUS  || reply->type == REDIS_REPLY_ERROR )
    {
      my_printf("Either REDIS_REPLY_ERROR or REDIS_REPLY_STATUS");
    }
    freeReplyObject( reply );
    delete cluster_p;
}

void updatePrimaryKey(const char *address, int port, char *table_database) {
   redisReply * reply2;
   int returnCount;
   Cluster<redisContext>::ptr_t cluster_p = HiredisCommand<>::createCluster( address, port );
   long long count = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "HLEN %s", table_database))->integer; 
   count += 1;
   reply2 = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "HSET %s %i %i", table_database, count, count));
   if( reply2->type == REDIS_REPLY_ERROR )
   {
   	my_printf("Some Error Occured");
   }
   freeReplyObject(reply2);
   delete cluster_p;
}

void deleteCommand( const char *address, int port, const char *table_database, int primary_key ) {
   redisReply * reply2;
   int returnCount;
   Cluster<redisContext>::ptr_t cluster_p = HiredisCommand<>::createCluster( address, port );
   reply2 = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "HDEL %s %i", table_database, primary_key ));
   if( reply2->type == REDIS_REPLY_ERROR )
   {
   	my_printf("Some Error Occured");
   }
   freeReplyObject(reply2);
   delete cluster_p;
}

long long getLength(const char *address, int port, char *table_database) {
   redisReply * reply2;
   int returnCount;
   Cluster<redisContext>::ptr_t cluster_p = HiredisCommand<>::createCluster( address, port );
   long long count = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "HLEN %s", table_database))->integer;
   delete cluster_p;
   return count;
}

json readCommand(const char *address, int port, const char *table_database, int primary_key) {
   redisReply * reply2;
   json j;
   int returnCount;
   int j = primary_key;
   Cluster<redisContext>::ptr_t cluster_p = HiredisCommand<>::createCluster( address, port );
   char *ste = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "HGET %s %i", table_database, primary_key))->str;
   // j[table_database] = ste; //Don't remove this bring this comment back when client for it is developed.
   delete cluster_p;
   // return j; /// Return's JSON Value for a key-value
}

