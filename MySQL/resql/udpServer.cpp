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

// For JSON Parsing stuff, see this: https://github.com/nlohmann/json, quite the best for C++
#include "json/src/json.hpp"
#include <string>
#include <sstream>
#include "udpServer.hpp"

// For all Redis Operations
#include "redisoperations.hpp"

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

int runReSQL() {
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
    char *address = "127.0.0.1";
    std::vector<const char *> arr;
    std::string strr;
    json::string_t value;
    char *table_database = "personal_finance"; //Remeber to remove this add the function back from gist
    char *message , client_message[8000];
    char *bufferr;
    std::ifstream file;
     

     
    while( (read_size = recv(sock , client_message , 2000 , 0)) > 0 )
    {
        value = client_message;
        json j(value);
        auto j3 = json::parse(j);

        s1 = j3["type"].get<std::string>().c_str();
        if(strcmp(s1, "create") == 0)
        {   
            TABLE *open = openTable(table_database);
            my_printf("Inside Create Statement");
            vect3 = ReadAllColumns( address, port, table_database );
            count = countColumns( address, port, table_database ); // Add here user Id
            // count +=1;
            countIter = getLength( address, port, "primary");
            countIter += 1;
            if (count > 0) {
                for(int k = 0; k <= count-1; k++){
                    std::string str(vect3[k]);
                    InsertColumnData( address, port, vect3[k], j3[str].get<std::string>().c_str(), countIter);
                    insertIntoSQL( open, table_database, address, port );
                }
            } else {
                for(int k = 0; k <= count; k++){
                    std::string str(vect3[k]);
                    InsertColumnData( address, port, vect3[k], j3[str].get<std::string>().c_str(), countIter);
                    insertIntoSQL( open, table_database, address, port );
                }
            }
            updatePrimaryKey(address, port, "primary");
        }
        else if(strcmp(s1, "read") == 0)
        {
            TABLE *open = openTable(table_database);
            my_printf("Inside Read Statement");          
            for(int i = 0; i <= j3["array"].size()-1; i++){
                readCommand( address, port, j3["array"][i].get<std::string>().c_str(), atoi(j3["primaryKey"].get<std::string>().c_str()) );
                insertIntoSQL( open, table_database, address, port );
            }
     
        }
        else if(strcmp(s1, "update") == 0)
        {
            TABLE *open = openTable(table_database);
            my_printf("Inside Update Statement");
            vect3 = ReadAllColumns( address, 8085, table_database);
            count = countColumns( address, 8085, table_database); // Add here user Id
            if (count > 0) {
                for(int k = 0; k <= count-1; k++){
                    deleteCommand( address, port, vect3[k], atoi(j3["primaryKey"].get<std::string>().c_str()));
                    deleteCommand( address, port, table_database, atoi(j3["primaryKey"].get<std::string>().c_str()));
                    insertIntoSQL( open, table_database, address, port );
                }
            } else {
                for(int k = 0; k <= count; k++){
                    std::string str(vect3[k]);
                    deleteCommand( address, port, vect3[k], atoi(j3["primaryKey"].get<std::string>().c_str()));
                    deleteCommand( address, port, table_database, atoi(j3["primaryKey"].get<std::string>().c_str()));
                    insertIntoSQL( open, table_database, address, port );
                }
            }
        }
        else if( strcmp(s1, "delete") == 0)
        {
            TABLE *open = openTable(table_database);
            my_printf("Inside Delete Statement");
            vect3 = ReadAllColumns( address, 8085, table_database);
            count = countColumns( address, 8085, table_database); // Add here user Id
            if (count > 0) {
                for(int k = 0; k <= count-1; k++){
                    deleteCommand( address, port, vect3[k], atoi(j3["primaryKey"].get<std::string>().c_str()));
                    insertIntoSQL( open, table_database, address, port );
                }
            } else {
                for(int k = 0; k <= count; k++){
                    std::string str(vect3[k]);
                    deleteCommand( address, port, j3[str].get<std::string>().c_str(), atoi(j3["primaryKey"].get<std::string>().c_str()));
                    insertIntoSQL( open, table_database, address, port );
                }
            }
        }
        else if( strcmp(s1, "crow") == 0)
        {
            TABLE *open = openTable(table_database);
            my_printf("Inside Create Row Statement");
            for(int i = 0; i <= j3["array"].size()-1; i++){
                try {
                    InsertColumnNameCommand( address, port, table_database, j3["array"][i].get<std::string>().c_str());
                    insertIntoSQL( open, table_database, address, port );
                } catch ( const RedisCluster::ClusterException &e ) {
                    my_printf("Redis Cluster exception Occured");
                    exit (EXIT_FAILURE);
                }
            }
        }
        else
        {
            my_printf("Wrong Option Provided");
        }    
    }
    
    if(read_size == 0)
    {
        my_printf("Client Disconnected");
        fflush(stdout);
    }
    else if(read_size == -1)
    {
        my_printf("recv failed");
    }
         
    free(socket_desc);
    file.close(); 
    return 0;
}

