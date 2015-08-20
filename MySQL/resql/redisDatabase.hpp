/*
* 
* Author : Aalekh Nigam email: aalekh.nigam@gmail.com
* A lot of code Adapted from DeNA's HandlerSocket, so thank you DeNA :)
*/

#ifndef REDISDATABASE_HPP
#define REDISDATABASE_HPP

TABLE *openTable(char *mbrace_db_name);

void insertIntoSQL( THD *thd, TABLE *table_list, char *mbrace_db_name, char *address, int port );

void updateIntoSQL( THD *thd, TABLE *table_list, char *mbrace_db_name, char *address, int port );

void deleteIntoSQL( THD *thd, TABLE *table_list, char *mbrace_db_name, char *address, int port );

#endif
