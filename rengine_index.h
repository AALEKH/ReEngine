
#include "my_global.h"
#include "my_sys.h"
const long METADATA_SIZE = sizeof(int) + sizeof(bool);

struct SDE_INDEX
{
  byte *key;
  long long pos;
  int length;
};

struct SDE_NDX_NODE
{
  SDE_INDEX key_ndx;
  SDE_NDX_NODE *next;
  SDE_NDX_NODE *prev;
};
class Rengine_index
{
public:
Rengine_index(int keylen);
Rengine_index();
~Rengine_index(void);
int open_index(char *path);
int create_index(char *path, int keylen);
int insert_key(SDE_INDEX *ndx, bool allow_dupes);
int delete_key(byte *buf, long long pos, int key_len); long long get_index_pos(byte *buf, int key_len);
  long long get_first_pos();
  byte *get_first_key();
  byte *get_last_key();
  byte *get_next_key();
  byte *get_prev_key();
  int close_index();
  int load_index();
  int destroy_index();
  SDE_INDEX *seek_index(byte *key, int key_len);
  SDE_NDX_NODE *seek_index_pos(byte *key, int key_len);
  int save_index();
  int trunc_index();
private:
  File index_file;
  int max_key_len;
  SDE_NDX_NODE *root;
  SDE_NDX_NODE *range_ptr;
  int block_size;
  bool crashed;
  int read_header();
  int write_header();
  long long write_row(SDE_INDEX *ndx);
  SDE_INDEX *read_row(long long Position);
  long long curfpos();
};