#include <iostream>
#include <queue>
#include <thread>
#include <assert.h>
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "ha_rengine.h"
#include "probes_mysql.h"
#include "sql_plugin.h"

// For Redis Cluster Operations

#include "createcluster.h"
#include "cpp-hiredis-cluster/include/hirediscommand.h"

using namespace RedisCluster;
using std::string;
using std::cout;
using std::cerr;
using std::endl;
using namespace std;


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

// For pushing data into redis cluster
void processClusterCommand(const char *address, int port)
{
    Cluster<redisContext>::ptr_t cluster_p;
    redisReply * reply;
    int value = 0;
    string command;
    cluster_p = HiredisCommand<>::createCluster( address, port );
    cout << "Bus error 0" << endl;
    while (value < 20) {
      command = "FOO";
      cout << "Bus Error 1" << endl;
      command = command + to_string(value);
      cout << "Bus Error 2" << endl;
      reply = static_cast<redisReply*>( HiredisCommand<>::Command( cluster_p, "FOO", "SET %s %i", command.c_str(), value));

      if( reply->type == REDIS_REPLY_STATUS  || reply->type == REDIS_REPLY_ERROR )
      {
          cout << " Reply to SET FOO BAR " << endl;
          cout << reply->str << endl;
      }
      value = value + 1;
      freeReplyObject( reply );
    }
    delete cluster_p;
}

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

static ulong srv_enum_var= 0;
static ulong srv_ulong_var= 0;
static double srv_double_var= 0;

const char *enum_var_names[]=
{
  "e1", "e2", NullS
};

TYPELIB enum_var_typelib=
{
  array_elements(enum_var_names) - 1, "enum_var_typelib",
  enum_var_names, NULL
};

static MYSQL_SYSVAR_ENUM(
  enum_var,                       // name
  srv_enum_var,                   // varname
  PLUGIN_VAR_RQCMDARG,            // opt
  "Sample ENUM system variable.", // comment
  NULL,                           // check
  NULL,                           // update
  0,                              // def
  &enum_var_typelib);             // typelib

static MYSQL_SYSVAR_ULONG(
  ulong_var,
  srv_ulong_var,
  PLUGIN_VAR_RQCMDARG,
  "0..1000",
  NULL,
  NULL,
  8,
  0,
  1000,
  0);

static MYSQL_SYSVAR_DOUBLE(
  double_var,
  srv_double_var,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);                             // reserved always 0

static MYSQL_THDVAR_DOUBLE(
  double_thdvar,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);

static struct st_mysql_sys_var* rengine_system_variables[]= {
  MYSQL_SYSVAR(enum_var),
  MYSQL_SYSVAR(ulong_var),
  MYSQL_SYSVAR(double_var),
  MYSQL_SYSVAR(double_thdvar),
  NULL
};

static int show_func_rengine(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  var->type= SHOW_CHAR;
  var->value= buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  my_snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
              "enum_var is %lu, ulong_var is %lu, "
              "double_var is %f, %.6b", // %b is a MySQL extension
              srv_enum_var, srv_ulong_var, srv_double_var, "really");
  return 0;
}

struct rengine_vars_t
{
	ulong  var1;
	double var2;
	char   var3[64];
  bool   var4;
  bool   var5;
  ulong  var6;
};

rengine_vars_t rengine_vars= {100, 20.01, "three hundred", true, 0, 8250};

static st_mysql_show_var show_status_rengine[]=
{
  {"var1", (char *)&rengine_vars.var1, SHOW_LONG, SHOW_SCOPE_GLOBAL},
  {"var2", (char *)&rengine_vars.var2, SHOW_DOUBLE, SHOW_SCOPE_GLOBAL},
  {0,0,SHOW_UNDEF, SHOW_SCOPE_UNDEF} // null terminator required
};

static struct st_mysql_show_var show_array_rengine[]=
{
  {"array", (char *)show_status_rengine, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
  {"var3", (char *)&rengine_vars.var3, SHOW_CHAR, SHOW_SCOPE_GLOBAL},
  {"var4", (char *)&rengine_vars.var4, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
  {0,0,SHOW_UNDEF, SHOW_SCOPE_UNDEF}
};

static struct st_mysql_show_var func_status[]=
{
  {"rengine_func_rengine", (char *)show_func_rengine, SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  {"rengine_status_var5", (char *)&rengine_vars.var5, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
  {"rengine_status_var6", (char *)&rengine_vars.var6, SHOW_LONG, SHOW_SCOPE_GLOBAL},
  {"rengine_status",  (char *)show_array_rengine, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
  {0,0,SHOW_UNDEF, SHOW_SCOPE_UNDEF}
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
  0x0001 /* 0.1 */,
  func_status,                                  /* status variables */
  rengine_system_variables,                     /* system variables */
  NULL,                                         /* config options */
  0,                                            /* flags */
}
mysql_declare_plugin_end;
