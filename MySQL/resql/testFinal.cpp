// For Vector Array 
#include <vector>
#include <iostream>
#include <string>

#include <stdio.h>
#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h> 
#include <unistd.h>
#include <fstream>    
#include <typeinfo>

// For JSON Parsing stuff, see this: https://github.com/nlohmann/json
#include "json/src/json.hpp"
#include <string>
#include <sstream>

#include <typeinfo>
// For Redis Cluster Functioning
#include <queue>
#include <thread>
#include <assert.h>
#include "createcluster.h"
#include "cpp-hiredis-cluster/include/hirediscommand.h"
#include <boost/functional/hash.hpp>


#include<pthread.h> //for threading , link with lpthread
 
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
      std::cout << "Reply to SADD FOO BAR" << std::endl;
      std::cout << reply->str << std::endl;
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
void InsertColumnData(const char *address, int port, const char *column_table,const char *data, int primary_key)
{
    Cluster<redisContext>::ptr_t cluster_p;
    redisReply * reply;
    cluster_p = HiredisCommand<>::createCluster( address, port );
    std::cout << "Inside Insert Column Function" << std::endl;
    std::cout << column_table << std::endl;
    std::cout << data << std::endl;
    std::cout << "Finished Insert Column Function" << std::endl;
    reply = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "HSET %s %i %s", column_table, primary_key, data));
    if( reply->type == REDIS_REPLY_STATUS  || reply->type == REDIS_REPLY_ERROR )
    {
      std::cout << "Reply to SADD FOO BAR" << std::endl;
      std::cout << reply->str << std::endl;
    }
    std::cout << reply->type << std::endl;
    freeReplyObject( reply );
    delete cluster_p;
}

int main(int argc , char *argv[])
{
    int socket_desc , new_socket , c , *new_sock;
    struct sockaddr_in server , client;
    char *message;

    socket_desc = socket(AF_INET , SOCK_STREAM , 0);
    if (socket_desc == -1)
    {
        printf("Could not create socket");
    }
     
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons( 8885 );
     
    if( bind(socket_desc,(struct sockaddr *)&server , sizeof(server)) < 0)
    {
        puts("bind failed");
        return 1;
    }
    puts("bind done");
     
    listen(socket_desc , 3);
     
    puts("Waiting for incoming connections...");
    c = sizeof(struct sockaddr_in);
    while( (new_socket = accept(socket_desc, (struct sockaddr *)&client, (socklen_t*)&c)) )
    {
        puts("Connection accepted");
         
        message = "Hello Client , I have received your connection. And now I will assign a handler for you\n";
        write(new_socket , message , strlen(message));
         
        pthread_t sniffer_thread;
        new_sock = (int *)malloc(1);
        *new_sock = new_socket;
         
        if( pthread_create( &sniffer_thread , NULL ,  connection_handler , (void*) new_sock) < 0)
        {
            perror("could not create thread");
            return 1;
        }

        puts("Handler assigned");
    }
     
    if (new_socket<0)
    {
        perror("accept failed");
        return 1;
    }
     
    return 0;
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
     std::cout << "Some Error Occured" << std::endl;
   }
   freeReplyObject(reply2);
   delete cluster_p;
}

void deleteCommand( const char *address, int port, const char *table_database, int primary_key ){
   redisReply * reply2;
   int returnCount;
   Cluster<redisContext>::ptr_t cluster_p = HiredisCommand<>::createCluster( address, port );
   reply2 = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "HDEL %s %i", table_database, primary_key ));
   if( reply2->type == REDIS_REPLY_ERROR )
   {
     std::cout << "Some Error Occured" << std::endl;
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

void readCommand(const char *address, int port, const char *table_database, int primary_key) {
   redisReply * reply2;
   int returnCount;
   std::cout << primary_key << std::endl;
   int j = primary_key;
   std::cout << j << std::endl;
   Cluster<redisContext>::ptr_t cluster_p = HiredisCommand<>::createCluster( address, port );
   char *ste = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "HGET %s %i", table_database, primary_key))->str;
   delete cluster_p;
}

void *connection_handler(void *socket_desc)
{

    int sock = *(int*)socket_desc;
    int read_size;
    json object;
    long long countIter;
    const char *s1;
    int passing;
    std::vector<const char *> vect3;
    long long count;
    const char *interString;
    // std::string arr;
    int port = 11002;
    std::vector<const char *> arr;
    std::string strr;
    json::string_t value;
    char *table_database = "personal_finance";
    std::string  st = "hey"; 
    char *message , client_message[2000];
    char *bufferr;
    std::ifstream file;
     
    file.open("program.txt");
     
    while( (read_size = recv(sock , client_message , 2000 , 0)) > 0 )
    {
        value = client_message;
        std::cout << "Wored till here 0" << std::endl;
        json j(value);
        std::cout << "Wored till here 1/2" << std::endl;
        auto j3 = json::parse(j);
        std::cout << "Wored till here 1" << std::endl;
        s1 = j3["type"].get<std::string>().c_str();
        if(strcmp(s1, "create") == 0)
        {
            std::cout << "Create" << std::endl;
            vect3 = ReadAllColumns("127.0.0.1", 8085, table_database);
            std::cout << "Read All Columns" << std::endl;
            count = countColumns("127.0.0.1", 8085, table_database); // Add here user Id
            std::cout << "Counted All Columns" << std::endl;
            // count +=1;
            countIter = getLength("127.0.0.1", port, "primary");
            countIter += 1;
            std::cout << count << std::endl;
            std::cout << countIter << std::endl;
            if (count > 0) {
                for(int k = 0; k <= count-1; k++){
                    std::string str(vect3[k]);
                    std::cout << str << std::endl;
                    InsertColumnData("127.0.0.1", port, vect3[k], j3[str].get<std::string>().c_str(), countIter);
                }
            } else {
                for(int k = 0; k <= count; k++){
                    std::string str(vect3[k]);
                    std::cout << str << std::endl;
                    InsertColumnData("127.0.0.1", port, vect3[k], j3[str].get<std::string>().c_str(), countIter);
                }
            }
            updatePrimaryKey("127.0.0.1", port, "primary");
        }
        else if(strcmp(s1, "read") == 0)
        {
            std::cout << "Read" << std::endl;
           // Executes when the boolean expression 2 is true
            std::cout << j3["array"].size() << std::endl;
            for(int i = 0; i <= j3["array"].size()-1; i++){
                std::cout << j3["array"][i] << std::endl;
                readCommand("127.0.0.1", port, j3["array"][i].get<std::string>().c_str(), atoi(j3["primaryKey"].get<std::string>().c_str()));
            }
     
        }
        else if(strcmp(s1, "update") == 0)
        {
            std::cout << "Update" << std::endl;
            vect3 = ReadAllColumns("127.0.0.1", 8085, table_database);
            count = countColumns("127.0.0.1", 8085, table_database); // Add here user Id
            if (count > 0) {
                for(int k = 0; k <= count-1; k++){
                    deleteCommand("127.0.0.1", port, vect3[k], atoi(j3["primaryKey"].get<std::string>().c_str()));
                    deleteCommand( "127.0.0.1", port, table_database, atoi(j3["primaryKey"].get<std::string>().c_str()));
                }
            } else {
                for(int k = 0; k <= count; k++){
                    std::string str(vect3[k]);
                    std::cout << str << std::endl;
                    deleteCommand("127.0.0.1", port, vect3[k], atoi(j3["primaryKey"].get<std::string>().c_str()));
                    deleteCommand( "127.0.0.1", port, table_database, atoi(j3["primaryKey"].get<std::string>().c_str()));
                }
            }

            if (count > 0) {
                for(int k = 0; k <= count-1; k++){
                    std::string str(vect3[k]);
                    std::cout << str << std::endl;
                    InsertColumnData("127.0.0.1", port, vect3[k], j3[str].get<std::string>().c_str(), atoi(j3["primaryKey"].get<std::string>().c_str()));
                }
            } else {
                for(int k = 0; k <= count; k++){
                    std::string str(vect3[k]);
                    std::cout << str << std::endl;
                    deleteCommand("127.0.0.1", port, j3[str].get<std::string>().c_str(), atoi(j3["primaryKey"].get<std::string>().c_str()));
                }
            }

        }
        else if( strcmp(s1, "delete") == 0)
        {
            vect3 = ReadAllColumns("127.0.0.1", 8085, table_database);
            count = countColumns("127.0.0.1", 8085, table_database); // Add here user Id
            if (count > 0) {
                for(int k = 0; k <= count-1; k++){
                    deleteCommand("127.0.0.1", port, vect3[k], atoi(j3["primaryKey"].get<std::string>().c_str()));
                }
            } else {
                for(int k = 0; k <= count; k++){
                    std::string str(vect3[k]);
                    std::cout << str << std::endl;
                    deleteCommand("127.0.0.1", port, j3[str].get<std::string>().c_str(), atoi(j3["primaryKey"].get<std::string>().c_str()));
                }
            }
        }
        else if( strcmp(s1, "crow") == 0)
        {
            for(int i = 0; i <= j3["array"].size()-1; i++){
                std::cout << j3["array"][i] << std::endl;
                try {
                    InsertColumnNameCommand("127.0.0.1", port, table_database, j3["array"][i].get<std::string>().c_str());
                } catch ( const RedisCluster::ClusterException &e ) {
                    std::cout << "Cluster exception: " << e.what() << std::endl;
                    exit (EXIT_FAILURE);
                }
            }
        }
        else
        {
            std::cout << "Not in Option" << std::endl; 
        }    
    }
    
    if(read_size == 0)
    {
        puts("Client disconnected");
        fflush(stdout);
    }
    else if(read_size == -1)
    {
        perror("recv failed");
    }
         
    free(socket_desc);
    file.close(); 
    return 0;
}

