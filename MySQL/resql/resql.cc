/* Copyright (c) 2003, 2010, Oracle and/or its affiliates.
   Copyright (c) 2013, MariaDB Foundation.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA *

  Author : Aalekh Nigam [Email: aalekh.nigam@gmail.com]

*/

#include <my_global.h>
#include <stdlib.h>
#include <ctype.h>
#include <mysql_version.h>
#include <mysql/plugin.h>
#include <my_dir.h>
#include "my_thread.h"
#include "my_sys.h"                             // my_write, my_malloc
#include "m_string.h"                           // strlen
#include "sql_plugin.h"                         // st_plugin_int
#include "createCluster.h"
#include "redisDatabase.h"   
#include "redisOperations.hpp"
#include "udpServer.hpp"   


static void init_resql_psi_keys()
{
  const char* category= "resql";
  int count;

  count= array_elements(all_resql_memory);
  mysql_memory_register(category, all_resql_memory, count);
};


/*
  Initialize ReSQL Plugin
*/

static int resql_plugin_init(void *p) {

  DBUG_ENTER("resql_plugin_init");

  #ifdef HAVE_PSI_INTERFACE
    init_resql_psi_keys();
  #endif

  clusterSpec s1 = {
    30000,
    2000,
    6,
    1,
    "127.0.0.1"
  };
  startCluster(s1);
  my_printf("Worked till here!!!");
  createCluster(s1); 
  runReSQL(); 
  DBUG_RETURN(0);
}


/*
  Terminate the ReSQL Daemon Plugin
*/

static int resql_plugin_deinit(void *p)
{
  DBUG_ENTER("resql_plugin_deinit");
  stopCluster(s1);
  cleanCluster();
  DBUG_RETURN(0);
}


struct st_mysql_daemon resql_plugin=
{ MYSQL_DAEMON_INTERFACE_VERSION  };

/*
  Plugin library descriptor
*/

mysql_declare_plugin(resql)
{
  MYSQL_DAEMON_PLUGIN,
  &resql_plugin,
  "resql",
  "Aalekh Nigam",
  "ReSQL Plugin to provide faster NoSQL Feature over MariaDB/MySQL",
  PLUGIN_LICENSE_GPL,
  resql_init, /* Plugin Init */
  resql_deinit, /* Plugin Deinit */
  0x0100 /* 1.0 */,
  NULL,                       /* status variables                */
  NULL,                       /* system variables                */
  NULL,                       /* config options                  */
  0,                          /* flags                           */
}
mysql_declare_plugin_end;
