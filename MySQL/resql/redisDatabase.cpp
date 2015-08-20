#include "my_global.h"
#include "sql_class.h"   
#include <mysql/plugin.h>
#include <mysql/psi/mysql_file.h>
#include "redisDatabase.h"
#include "probes_mysql.h"

TABLE *openTable(char *mbrace_db_name) {
  THD *thd;
  TABLE *table_list;
  tableCreation t;
  table_list->db_length = strlen(mbrace_db_name);
  table_list->db = thd->strmake(mbrace_db_name, table_list->db_length);
  table_list->table_name_length = strlen(mbrace_table_name);
  table_list->table_name = thd->strmake(mbrace_table_name, table_list->table_name_length);
  table_list->alias = thd->strdup(mbrace_table_name);
  table_list->lock_type = TL_WRITE;
  table_list->select_lex = NULL;
  table_list->cacheable_table = false;

  if (!(table= open_n_lock_single_table(thd, table_list, TL_WRITE)))
  {
    my_printf("Failed to open table: %s", mbrace_db_name);
    exit;
  }

  return table_list;
}

void insertIntoSQL( THD *thd, TABLE *table_list, char *mbrace_db_name, char *address, int port ) {
  int error = 0;
  bool not_used;
  int count;
  std::vector<const char *> vect3;
  count = countColumns(address, port, table_database); // Add here user Id
  vect3 = ReadAllColumns(address, port, table_database);
  /// Read Pthread Theory and change this code
  pthread_mutex_lock(&mbrace_mutex);
  while (!mbrace_values_list_length && error != ETIMEDOUT)
  {
    struct timespec abstime;
    set_timespec(abstime, opt_mbrace_insert_interval);
    error = pthread_cond_timedwait(&mbrace_cond, &mbrace_mutex, &abstime);
  }

  values_list = vect3;
  values_list_length = count;
  // mbrace_values_list = new I_List<mbrace_request_mysql>;
  // mbrace_values_list_length = 0;
  pthread_mutex_unlock(&mbrace_mutex);
  ///////////////////
  thd->lock= mysql_lock_tables(thd, &table, 1, MYSQL_LOCK_IGNORE_GLOBAL_READ_LOCK, &not_used);
  table->use_all_columns();
  memset(table->record[0], 0, table->s->null_bytes);
  table->file->ha_start_bulk_insert(values_list_length);

  for(int k = 0; k< count-1; k++){
    table->field[0]->store( values_list[vect3[k]], true);
  }
  table->file->ha_write_row(table->record[0]);
  delete values_list;

  table->file->ha_end_bulk_insert();

  query_cache_invalidate3(thd, table, 1);
  table->file->ha_release_auto_increment();
  mysql_unlock_tables(thd, thd->lock);
  ha_autocommit_or_rollback(thd, 0);
}

void updateIntoSQL( THD *thd, TABLE *table_list, char *mbrace_db_name, char *address, int port ){
  int error = 0;
  bool not_used;
  int count;
  std::vector<const char *> vect3;
  store_record( table, record[1] );
  count = countColumns(address, port, table_database); // Add here user Id
  vect3 = ReadAllColumns(address, port, table_database);
  thd->lock= mysql_lock_tables(thd, &table, 1, MYSQL_LOCK_IGNORE_GLOBAL_READ_LOCK, &not_used);
  table->use_all_columns();
  memset(table->record[0], 0, table->s->null_bytes);
  table->file->ha_start_bulk_insert(values_list_length);

  for(int k = 0; k< count-1; k++){
    table->field[0]->store( values_list[vect3[k]], true);
  }
  table->file->ha_update_row(table->record[1], table->record[0]);
  delete values_list;

  table->file->ha_end_bulk_insert();

  query_cache_invalidate3(thd, table, 1);
  table->file->ha_release_auto_increment();
  mysql_unlock_tables(thd, thd->lock);
  ha_autocommit_or_rollback(thd, 0);
}

void deleteIntoSQL( THD *thd, TABLE *table_list, char *mbrace_db_name, char *address, int port ) {
  int error = 0;
  bool not_used;
  int count;
  std::vector<const char *> vect3;
  store_record( table, record[1] );
  count = countColumns(address, port, table_database); // Add here user Id
  vect3 = ReadAllColumns(address, port, table_database);
  thd->lock= mysql_lock_tables(thd, &table, 1, MYSQL_LOCK_IGNORE_GLOBAL_READ_LOCK, &not_used);
  table->use_all_columns();
  memset(table->record[0], 0, table->s->null_bytes);
  table->file->ha_start_bulk_insert(values_list_length);

  for(int k = 0; k< count-1; k++){
    table->field[0]->store( values_list[vect3[k]], true);
  }
  table->file->ha_delete_row(table->record[0]);
  delete values_list;

  table->file->ha_end_bulk_insert();

  query_cache_invalidate3(thd, table, 1);
  table->file->ha_release_auto_increment();
  mysql_unlock_tables(thd, thd->lock);
  ha_autocommit_or_rollback(thd, 0);
}