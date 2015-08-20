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

#ifndef UDPSERVER_H_INCLUDE
#define UDPSERVER_H_INCLUDE

// For Vector Array 
#include <vector>
#include <iostream>
#include <string>

void *connection_handler(void *);

int runReSQL();

void *connection_handler(void *socket_desc);

#endif  