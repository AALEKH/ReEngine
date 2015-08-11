
#include "rengine_index.h"
#include <my_dir.h>
/* constuctor takes the maximum key length for the keys */
Rengine_index::Rengine_index(int keylen)
{
  root = NULL;
  crashed = false;
  max_key_len = keylen;
  index_file = -1;
  block_size = max_key_len + sizeof(long long) + sizeof(int);
}

/* constuctor (overloaded) assumes existing file */
Rengine_index::Rengine_index()
{
  root = NULL;
  crashed = false;
  max_key_len = -1;
  index_file = -1;
  block_size = -1;
}
/* destructor */
Rengine_index::~Rengine_index(void)
{
}
/* create the index file */
int Rengine_index::create_index(char *path, int keylen)
{
  DBUG_ENTER("Rengine_index::create_index");
  open_index(path);
  max_key_len = keylen;
  /*
    Block size is the key length plus the size of the index
    length variable.
  */
  block_size = max_key_len + sizeof(long long);
  write_header();
  DBUG_RETURN(0);
}
/* open index specified as path (pat+filename) */
int Rengine_index::open_index(char *path)
{
  DBUG_ENTER("Rengine_index::open_index");
  /*
    Open the file with read/write mode,
    create the file if not found,
    treat file as binary, and use default flags.
  */
  index_file = my_open(path, O_RDWR | O_CREAT | O_BINARY | O_SHARE, MYF(0));
  if(index_file == -1)
    DBUG_RETURN(errno);
  read_header();
  DBUG_RETURN(0);
}

/* read header from file */
int Rengine_index::read_header()
{
  int i;
  byte len;
  DBUG_ENTER("Rengine_index::read_header");
  if (block_size == -1)
  {
    /*
      Seek the start of the file.
      Read the maximum key length value.
    */
    my_seek(index_file, 0l, MY_SEEK_SET, MYF(0));
    i = my_read(index_file, &len, sizeof(int), MYF(0));
    memcpy(&max_key_len, &len, sizeof(int));
    /*
      Calculate block size as maximum key length plus
      the size of the key plus the crashed status byte.
    */
    block_size = max_key_len + sizeof(long long) + sizeof(int);
    i = my_read(index_file, &len, sizeof(bool), MYF(0));
    memcpy(&crashed, &len, sizeof(bool));
} else {
    i = (int)my_seek(index_file, sizeof(int) + sizeof(bool), MY_SEEK_SET, MYF(0));
  }
  DBUG_RETURN(0);
}
/* write header to file */
int Rengine_index::write_header()
{
  int i;
  byte len;
  DBUG_ENTER("Rengine_index::write_header");
  if (block_size != -1)
  {
    /*
      Seek the start of the file and write the maximum key length
      then write the crashed status byte.
*/
    my_seek(index_file, 0l, MY_SEEK_SET, MYF(0)); memcpy(&len, &max_key_len, sizeof(int));
    i = my_write(index_file, &len, sizeof(int), MYF(0)); memcpy(&len, &crashed, sizeof(bool));
    i = my_write(index_file, &len, sizeof(bool), MYF(0));
  }
  DBUG_RETURN(0);
}  

/* write a row (SDE_INDEX struct) to the index file */
long long Rengine_index::write_row(SDE_INDEX *ndx)
{
  long long pos;
  int i;
  int len;
  DBUG_ENTER("Rengine_index::write_row");
  /*
     Seek the end of the file (always append)
  */
  pos = my_seek(index_file, 0l, MY_SEEK_END, MYF(0));
  /*
    Write the key value.
  */
  i = my_write(index_file, ndx->key, max_key_len, MYF(0));
  memcpy(&pos, &ndx->pos, sizeof(long long));
  /*
    Write the file position for the key value.
  */
  i = i + my_write(index_file, (byte *)&pos, sizeof(long long), MYF(0));
  memcpy(&len, &ndx->length, sizeof(int));
  /*
    Write the length of the key.
  */
  i = i + my_write(index_file, (byte *)&len, sizeof(int), MYF(0));
  if (i == -1)
    pos = i;
  DBUG_RETURN(pos);
}
/* read a row (SDE_INDEX struct) from the index file */
SDE_INDEX *Rengine_index::read_row(long long Position)
{
  int i;
  long long pos;
  SDE_INDEX *ndx = NULL;
  DBUG_ENTER("Rengine_index::read_row");
    /*
    Seek the position in the file (Position).
  */
  pos = my_seek(index_file,(ulong) Position, MY_SEEK_SET, MYF(0));
  if (pos != -1L)
  {
    ndx = new SDE_INDEX();
    /*
      Read the key value.
    */
    i = my_read(index_file, ndx->key, max_key_len, MYF(0));
    /*
      Read the key value. If error, return NULL.
    */
    i = my_read(index_file, (byte *)&ndx->pos, sizeof(long long), MYF(0)); if (i == -1)
    {
    delete ndx;
    ndx = NULL; 
    }
  }
  DBUG_RETURN(ndx);
}


/* insert a key into the index in memory */
int Rengine_index::insert_key(SDE_INDEX *ndx, bool allow_dupes)
{
  SDE_NDX_NODE *p = NULL;
  SDE_NDX_NODE *n = NULL;
  SDE_NDX_NODE *o = NULL;
  int i = -1;
  int icmp;
  bool dupe = false;
  bool done = false;
  DBUG_ENTER("Rengine_index::insert_key");
  /*
    If this is a new index, insert first key as the root node.
  */
  if (root == NULL)
  {
    root = new SDE_NDX_NODE();
    root->next = NULL;
    root->prev = NULL;
    memcpy(root->key_ndx.key, ndx->key, max_key_len);
    root->key_ndx.pos = ndx->pos;
    root->key_ndx.length = ndx->length;
  }
  else //set pointer to root
  p = root;
  /*
  Loop through the linked list until a value greater than the key to be inserted, then insert new key before that one.
  */
  while ((p != NULL) && !done)
  {
    icmp = memcmp(ndx->key, p->key_ndx.key,
                 (ndx->length > p->key_ndx.length) ?
                  ndx->length : p->key_ndx.length);
    if (icmp > 0) // key is greater than current key in list
    {
  n = p;
  p = p->next; }
    /*
      If dupes not allowed, stop and return NULL
  */
  else if (!allow_dupes && (icmp == 0)) {
  p = NULL;
  dupe = true; }
  else {
      n = p->prev; //stop, insert at n->prev
  done = true; }
  } /*
  */
  if ((n != NULL) && !dupe)
  {
    if (p == NULL) //insert at end
    {
      p = new SDE_NDX_NODE();
      n->next = p;
      p->prev = n;
      memcpy(p->key_ndx.key, ndx->key, max_key_len);
      p->key_ndx.pos = ndx->pos;
      p->key_ndx.length = ndx->length;
    }
    else {
        o = new SDE_NDX_NODE();
        memcpy(o->key_ndx.key, ndx->key, max_key_len);
        o->key_ndx.pos = ndx->pos;
        o->key_ndx.length = ndx->length;
        o->next = p;
        o->prev = n;
        n->next = o;
        p->prev = o;
  }
  i = 1; }
  DBUG_RETURN(i);
}

/* delete a key from the index in memory. Note:
   position is included for indexes that allow dupes */
int Rengine_index::delete_key(byte *buf, long long pos, int key_len) {
  SDE_NDX_NODE *p;
  int icmp;
  int buf_len;
  bool done = false;
  DBUG_ENTER("Rengine_index::delete_key");
  p = root;
  /*
    Search for the key in the list. If found, delete it!
  */
  while ((p != NULL) && !done)
  {
    buf_len = p->key_ndx.length;
    icmp = memcmp(buf, p->key_ndx.key,
                 (buf_len > key_len) ? buf_len : key_len);
    if (icmp == 0)
    {
      if (pos != -1)
        if (pos == p->key_ndx.pos)
          done = true;
      else
        done = true;
    } else
    p = p->next; 
  }

   if (p != NULL)
  {
    /*
      Reset pointers for deleted node in list.
    */
    if (p->next != NULL)
      p->next->prev = p->prev;
    if (p->prev != NULL)
      p->prev->next = p->next;
    else
      root = p->next;
    delete p;
}
  DBUG_RETURN(0);
}
/* update key in place (so if key changes!) */
int Rengine_index::update_key(byte *buf, long long pos, int key_len) {
  SDE_NDX_NODE *p;
  bool done = false;
  DBUG_ENTER("Rengine_index::update_key");
  p = root;
  /*
    Search for the key.
  */
  while ((p != NULL) && !done)
  {
    if (p->key_ndx.pos == pos)
      done = true;
    else
      p = p->next;
} /*
    If key found, overwrite key value in node.
  */
  if (p != NULL)
  {
    memcpy(p->key_ndx.key, buf, key_len);
  }
  DBUG_RETURN(0);
}

/* get the current position of the key in the index file */ long long Rengine_index::get_index_pos(byte *buf, int key_len) {
  long long pos = -1;
  DBUG_ENTER("Rengine_index::get_index_pos");
  SDE_INDEX *ndx;
  ndx = seek_index(buf, key_len);
  if (ndx != NULL)
    pos = ndx->pos;
  DBUG_RETURN(pos);
}
/* get next key in list */
byte *Rengine_index::get_next_key() {
  byte *key = 0;
    DBUG_ENTER("Rengine_index::get_next_key");
    if (range_ptr != NULL)
    {
  key = (byte *)my_malloc(max_key_len, MYF(MY_ZEROFILL | MY_WME)); memcpy(key, range_ptr->key_ndx.key, range_ptr->key_ndx.length); range_ptr = range_ptr->next;
  }
    DBUG_RETURN(key);
  }
  /* get prev key in list */
  byte *Rengine_index::get_prev_key()
  {
  byte *key = 0;
    DBUG_ENTER("Rengine_index::get_prev_key");
    if (range_ptr != NULL)
    {
  key = (byte *)my_malloc(max_key_len, MYF(MY_ZEROFILL | MY_WME)); memcpy(key, range_ptr->key_ndx.key, range_ptr->key_ndx.length); range_ptr = range_ptr->prev;
  }
  DBUG_RETURN(key);
}

/* get first key in list */
byte *Rengine_index::get_first_key()
{
  SDE_NDX_NODE *n = root;
  byte *key = 0;
  DBUG_ENTER("Rengine_index::get_first_key");
  if (root != NULL)
  {
    key = (byte *)my_malloc(max_key_len, MYF(MY_ZEROFILL | MY_WME));
    memcpy(key, n->key_ndx.key, n->key_ndx.length);
  }
  DBUG_RETURN(key);
}
/* get last key in list */
byte *Rengine_index::get_last_key()
{
  SDE_NDX_NODE *n = root;
  byte *key = 0;
  DBUG_ENTER("Rengine_index::get_last_key");
  while (n->next != NULL)
    n = n->next;
  if (n != NULL)
  {
    key = (byte *)my_malloc(max_key_len, MYF(MY_ZEROFILL | MY_WME));
    memcpy(key, n->key_ndx.key, n->key_ndx.length);
  }
  DBUG_RETURN(key);
}
/* just close the index */
int Rengine_index::close_index()
{
  SDE_NDX_NODE *p;
  DBUG_ENTER("Rengine_index::close_index");
  if (index_file != -1)
  {
    my_close(index_file, MYF(0));
    index_file = -1;
  }
  while (root != NULL)
  {
    p = root;
    root = root->next;
    delete p;
}
  DBUG_RETURN(0);
}
/* find a key in the index */
SDE_INDEX *Rengine_index::seek_index(byte *key, int key_len)
{
  SDE_INDEX *ndx = NULL;
  SDE_NDX_NODE *n = root;
  int buf_len;
  bool done = false;
  DBUG_ENTER("Rengine_index::seek_index");
  if (n != NULL)
  {
    while((n != NULL) && !done)
    {
      buf_len = n->key_ndx.length;
      if (memcmp(n->key_ndx.key, key,
          (buf_len > key_len) ? buf_len : key_len) == 0)
        done = true;
      else
        n = n->next;
} }
  if (n != NULL)
  {
    ndx = &n->key_ndx;
    range_ptr = n;
  }
  DBUG_RETURN(ndx);
}
/* find a key in the index and return position too */
SDE_NDX_NODE *Rengine_index::seek_index_pos(byte *key, int key_len)
{
  SDE_NDX_NODE *n = root;
  int buf_len;
  bool done = false;
  DBUG_ENTER("Rengine_index::seek_index_pos");
  if (n != NULL)
  {
    while((n->next != NULL) && !done)
    {
      buf_len = n->key_ndx.length;
      if (memcmp(n->key_ndx.key, key,
          (buf_len > key_len) ? buf_len : key_len) == 0)
        done = true;
      else if (n->next != NULL)
        n = n->next;
} }
  DBUG_RETURN(n);
}
/* read the index file from disk and store in memory */
int Rengine_index::load_index()
{
  SDE_INDEX *ndx;
  int i = 0;
  DBUG_ENTER("Rengine_index::load_index");
  if (root != NULL)
    destroy_index();
  /*
    First, read the metadata at the front of the index.
  */
  read_header();
  while(!eof(index_file))
  {
ndx = new SDE_INDEX();
i = my_read(index_file, (byte *)&ndx->key, max_key_len, MYF(0));
i = my_read(index_file, (byte *)&ndx->pos, sizeof(long long), MYF(0)); i = my_read(index_file, (byte *)&ndx->length, sizeof(int), MYF(0)); insert_key(ndx, false);
}
  DBUG_RETURN(0);
}
/* get current position of index file */
long long Rengine_index::curfpos()
{
  long long pos = 0;
  DBUG_ENTER("Rengine_index::curfpos");
  pos = my_seek(index_file, 0l, MY_SEEK_CUR, MYF(0));
  DBUG_RETURN(pos);
}
/* write the index back to disk */
int Rengine_index::save_index()
{
  SDE_NDX_NODE *n = root;
  int i;
  DBUG_ENTER("Rengine_index::save_index");
  i = chsize(index_file, 0L);
  write_header();
  while (n != NULL)
  {
    write_row(&n->key_ndx);
    n = n->next;
}
  DBUG_RETURN(0);
}
int Rengine_index::destroy_index()
{
  SDE_NDX_NODE *n = root;
  DBUG_ENTER("Rengine_index::destroy_index");
  while (root != NULL)
  {
    n = root;
    root = n->next;
    delete n;
  }
  root = NULL;
  DBUG_RETURN(0);
}
/* ket the file position of the first key in index */
long long Rengine_index::get_first_pos()
{
  long long pos = -1;
  DBUG_ENTER("Rengine_index::get_first_pos");
  if (root != NULL)
    pos = root->key_ndx.pos;
  DBUG_RETURN(pos);
}

/* truncate the index file */
int Rengine_index::trunc_index()
{
  DBUG_ENTER("Rengine_index::trunc_table");
  if (index_file != -1)
  {
    my_chsize(index_file, 0, 0, MYF(MY_WME));
    write_header();
  }
  DBUG_RETURN(0);
}
