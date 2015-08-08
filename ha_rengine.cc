
// For Redis and basic C++ operations
#include <iostream>
#include <string>
#include <vector>
#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <msgpack.hpp>
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "hiredis/adapters/libevent.h"

// SQL Plugin header's
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "ha_example.h"
#include "sql_plugin.h"
#include "probes_mysql.h"

// using namespace RedisCluster;
using namespace std;
using namespace msgpack;


static handler *rengine_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root);

handlerton *rengine_hton;

static const char* rengine_system_database();
static bool rengine_is_supported_system_table(const char *db, const char *table_name, bool is_sql_layer_system_table);

Rengine_share::Rengine_share()
{
  thr_lock_init(&lock);
}


static int rengine_init_func(void *p)
{
  DBUG_ENTER("rengine_init_func");

  rengine_hton= (handlerton *)p;
  rengine_hton->state = SHOW_OPTION_YES;
  rengine_hton->create = rengine_create_handler;
  rengine_hton->flags= HTON_CAN_RECREATE;
  rengine_hton->system_database= rengine_system_database;
  rengine_hton->is_supported_system_table= rengine_is_supported_system_table;

  DBUG_RETURN(0);
}

Rengine_share *ha_rengine::get_share()
{
  Rengine_share *tmp_share;

  DBUG_ENTER("ha_rengine::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share= static_cast<Rengine_share*>(get_ha_share_ptr())))
  {
    tmp_share= new Rengine_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share*>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
}


static handler* rengine_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root)
{
  return new (mem_root) ha_rengine(hton, table);
}

ha_rengine::ha_rengine(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{}

static const char *ha_rengine_exts[] = {
  NullS
};

const char **ha_rengine::bas_ext() const
{
  return ha_rengine_exts;
}

const char* ha_rengine_system_database= NULL;
const char* rengine_system_database()
{
  return ha_rengine_system_database;
}

static st_system_tablename ha_rengine_system_tables[]= {
  {(const char*)NULL, (const char*)NULL}
};

static bool rengine_is_supported_system_table(const char *db,
                                              const char *table_name,
                                              bool is_sql_layer_system_table)
{
  st_system_tablename *systab;

  if (is_sql_layer_system_table)
    return false;

  systab= ha_rengine_system_tables;
  while (systab && systab->db)
  {
    if (systab->db == db &&
        strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}

//// For Redis Function

int pushAsMsgPack(vector<int> data, char *key){
  redisContext *c;
  redisReply *reply;
  c = redisConnect("127.0.0.1", 6379);
  if(c!=NULL && c->err) {
      printf("Error: %s\n", c->errstr);
  }

  sbuffer sbuf;
  pack(sbuf, data);

  reply = (redisReply *)redisCommand(c,"SET %s %s", key, sbuf.data());
  freeReplyObject(reply);
  return 0;
}

object getAsMsgPack(char *key) {

  redisContext *c;
  redisReply *reply;
  int sizeMSG = 6028;
  c = redisConnect("127.0.0.1", 6379);
  if(c!=NULL && c->err) {
      printf("Error: %s\n", c->errstr);
  }
  reply = (redisReply *)redisCommand(c, "GET %s", key);
  char* sbuf2 = reply->str;
  unpacked msg;
  unpack(&msg, sbuf2, sizeMSG);
  object obj = msg.get();
  freeReplyObject(reply);
  redisFree(c);
  return obj;
}


//// End of Redis Function

int ha_rengine::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_rengine::open");

  if (!(share = get_share()))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock,&lock,NULL);

  DBUG_RETURN(0);
}

int ha_rengine::close(void)
{
  DBUG_ENTER("ha_rengine::close");
  DBUG_RETURN(0);
}

int ha_rengine::write_row(uchar *buf)
{
  DBUG_ENTER("ha_rengine::write_row");

  DBUG_RETURN(0);
}

int ha_rengine::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_rengine::update_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_rengine::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_rengine::delete_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_rengine::index_read_map(uchar *buf, const uchar *key,
                               key_part_map keypart_map __attribute__((unused)),
                               enum ha_rkey_function find_flag
                               __attribute__((unused)))
{
  int rc;
  DBUG_ENTER("ha_rengine::index_read");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

int ha_rengine::index_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_rengine::index_next");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

int ha_rengine::index_prev(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_rengine::index_prev");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

int ha_rengine::index_first(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_rengine::index_first");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

int ha_rengine::index_last(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_rengine::index_last");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

int ha_rengine::rnd_init(bool scan)
{
  DBUG_ENTER("ha_rengine::rnd_init");
  DBUG_RETURN(0);
}

int ha_rengine::rnd_end()
{
  DBUG_ENTER("ha_rengine::rnd_end");
  DBUG_RETURN(0);
}

int ha_rengine::rnd_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_rengine::rnd_next");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);
  rc= HA_ERR_END_OF_FILE;
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

void ha_rengine::position(const uchar *record)
{
  DBUG_ENTER("ha_rengine::position");
  DBUG_VOID_RETURN;
}

int ha_rengine::rnd_pos(uchar *buf, uchar *pos)
{
  int rc;
  DBUG_ENTER("ha_rengine::rnd_pos");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


int ha_rengine::info(uint flag)
{
  DBUG_ENTER("ha_rengine::info");
  DBUG_RETURN(0);
}

int ha_rengine::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("ha_rengine::extra");
  DBUG_RETURN(0);
}

int ha_rengine::delete_all_rows()
{
  DBUG_ENTER("ha_rengine::delete_all_rows");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_rengine::truncate()
{
  DBUG_ENTER("ha_rengine::truncate");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int ha_rengine::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_rengine::external_lock");
  DBUG_RETURN(0);
}

THR_LOCK_DATA **ha_rengine::store_lock(THD *thd,
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=lock_type;
  *to++= &lock;
  return to;
}

int ha_rengine::delete_table(const char *name)
{
  DBUG_ENTER("ha_rengine::delete_table");
  /* This is not implemented but we want someone to be able that it works. */
  DBUG_RETURN(0);
}

int ha_rengine::rename_table(const char * from, const char * to)
{
  DBUG_ENTER("ha_rengine::rename_table ");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

ha_rows ha_rengine::records_in_range(uint inx, key_range *min_key,
                                     key_range *max_key)
{
  DBUG_ENTER("ha_rengine::records_in_range");
  DBUG_RETURN(10);                         // low number to force index usage
}

int ha_rengine::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("ha_rengine::create");
  /*
    This is not implemented but we want someone to be able to see that it
    works.
  */
  DBUG_RETURN(0);
}


struct st_mysql_storage_engine rengine_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static uint expiry_time_value = 1000;

const char *enum_var_names[]=
{
  "e1", "e2", NullS
};

TYPELIB enum_var_typelib=
{
  array_elements(enum_var_names) - 1, "enum_var_typelib",
  enum_var_names, NULL
};

static MYSQL_THDVAR_UINT(
  expiry_time,
  expiry_time,
  PLUGIN_VAR_OPCMDARG,
  "Sets Expiry Time for Redis Key Pair",
  NULL,
  NULL,
  expiry_time_value,
  1,
  10000,
  10
);

// Null Termination is important as per convention, this structure adds the variable to POSIX thread
static struct st_mysql_sys_var* rengine_system_variables[]= {
  MYSQL_SYSVAR(expiry_time),
  NULL
};

static int show_func_rengine(MYSQL_THD thd, struct st_mysql_show_var *var, char *buf)
{
  var->type= SHOW_CHAR;
  var->value= buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  my_snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE, "Expiry Time is : %i", expiry_time_value);
  return 0;
}

struct rengine_vars_t
{
	int  var1;
};

// rengine_vars_t rengine_vars= {100, 20.01, "three hundred", true, 0, 8250};
rengine_vars_t rengine_vars = {1000};
static st_mysql_show_var show_status_rengine[]=
{
  {"expiry_time", (char *)&rengine_vars.var1, SHOW_INT, SHOW_SCOPE_GLOBAL},
  {0,0,SHOW_UNDEF, SHOW_SCOPE_UNDEF} // null terminator required
};

mysql_declare_plugin(rengine)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &rengine_storage_engine,
  "RENGINE",
  "Aalekh Nigam",
  "Rengine storage engine",
  PLUGIN_LICENSE_GPL,
  rengine_init_func,                            /* Plugin Init */
  NULL,                                         /* Plugin Deinit */
  0x0001,                                       /* 0.1 */
  show_status_rengine,                          /* status variables */
  rengine_system_variables,                     /* system variables */
  NULL,                                         /* config options */
  0,                                            /* flags */
}
mysql_declare_plugin_end;
