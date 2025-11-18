// hash_file.c - The implementation of a dynamic hash table database 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "bf.h"
#include "hash_file.h"

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////// Structs and other definitions /////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////

#define MAX_OPEN_FILES 20

// Just a macro that makes error checking cleaner
#define CALL_BF(call, errorSignal)  \
{                                   \
  BF_ErrorCode code = call;         \
  if (code != BF_OK) {              \
    return errorSignal;             \
  }                                 \
}

/* Structs for the data stored in the files */

typedef struct file_info{
  uint32_t depth;                                // the depth of the header file
  uint32_t num_of_indexes;                       // basically equals to 2^(depth), its the size of the hashtable
  uint32_t recs_per_block;                       // how many records can fit in a record block
} file_info;

typedef struct block_info{
  uint32_t block_id;                             // the id of the block
  uint32_t num_of_recs;                          // number of records in block
  uint32_t local_depth;                          // local depth of the block
  uint32_t friends;                              // how many friend indexes it has
  uint32_t lowest_index;                         // the lowest index that shows to this node
  uint32_t next_block_id;                        // id of the next block
} block_info;

/* Structs for the hashtable stored in memory */

typedef struct HT_Handler{
  int fileDesc;                                  // The file descriptor of the file
  file_info* file_header;                        // The header block of the file
  int* arr;                                      // arr[hash_value] = block_id
} HT_Handler;

HT_Handler* hashtables[MAX_OPEN_FILES];
int numOfFiles = 0;

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////// Useful general purpose functions //////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Some functions that are used in multiple places for general purposes

int ptwo(int n){
  int result = 1;                                // Returns 2^n 
  for(int i = 0; i < n; i++ ){
    result *= 2;
  }
  return result;
}

block_info* getBlockInfo(BF_Block* block){
  void* data = BF_Block_GetData(block);          // Returns pointer to header of block
  data += BF_BLOCK_SIZE - sizeof(block_info);
  return (block_info*) data;
}

int getNumOfBlocks(int FileDesc){                // For getting num of blocks in one line
  int numOfBlocks;
  BF_GetBlockCounter(FileDesc, &numOfBlocks);
  return numOfBlocks;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////// Block Iterator functions //////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////

// For easier itaration over the blocks of the file, we decided to create an iterator. What's cool about this is
// that it can be used to increase size, to read, or to write. All very easily, without having to deal with disk
// blocks, and setting dirty and unpinning and stuff like that.

BF_Block* it_block;                                         // Block the iterator currently looks at
Record* it_data;                                            // pointer to this block's data in library's buffer
int it_offset;                                              // offset of where to read next record from the buffer
block_info* it_block_header;                                // pointer to header of the block that the iterator looks at

HT_Handler* it_HT;                                          // Pointer to hashtable the iterator currently reads from


void iterator_init(int block_id, HT_Handler* handler){      // Initializes the iterator at block_id
  BF_Block_Init(&it_block);
  BF_GetBlock(handler->fileDesc, block_id, it_block);
  it_data = (Record*) BF_Block_GetData(it_block);
  it_offset = 0;
  it_block_header = getBlockInfo(it_block);
  it_HT = handler;
}

void iterator_reset(int block_id){                          // Resets iterator at block_id
  BF_Block_SetDirty(it_block);
  BF_UnpinBlock(it_block);
  BF_GetBlock(it_HT->fileDesc, block_id, it_block);
  it_data = (Record*) BF_Block_GetData(it_block);
  it_offset = 0;
  it_block_header = getBlockInfo(it_block);
}

BF_Block* iterator_skip_block(){                            // Skip current block and go to next block in the hashtable
  if(it_block_header->next_block_id == -1){
    return NULL;
  } else{
    iterator_reset(it_block_header->next_block_id);
  }
  return it_block;
}

Record* iterator_next_rec(){                                // This function can be used to iterate through all the records,
  while(it_offset >= it_block_header->num_of_recs){         // it will return NULL when there are no more records left.
    if(iterator_skip_block() == NULL) return NULL;
  }
  Record* result = it_data + it_offset;
  it_offset += 1;
  return result;
}

HT_ErrorCode iterator_add_rec(Record newRecord){            // This function can add a new record to current block, returns error if it cant fit
  if(it_block_header->num_of_recs == it_HT->file_header->recs_per_block){
    return HT_ERROR;
  }
  it_data[it_block_header->num_of_recs] = newRecord;
  it_block_header->num_of_recs += 1;
  return HT_OK;
}

void iterator_clear_recs(){                                 // Removes all records from the current block
  it_block_header->num_of_recs = 0;
  it_offset = 0;
}

HT_ErrorCode iterator_insert_block(int friends, int local_depth, int lowest_index){
  int temp = it_block_header->next_block_id;                // This inserts block next to current block, updating next_block_id values as necessary

  int new_block_id = getNumOfBlocks(it_HT->fileDesc);
  it_block_header->next_block_id = new_block_id;

  BF_Block_SetDirty(it_block);
  BF_UnpinBlock(it_block);

  CALL_BF(BF_AllocateBlock(it_HT->fileDesc, it_block), BF_ERROR);
  it_data = (Record*) BF_Block_GetData(it_block);
  it_offset = 0;
  it_block_header = getBlockInfo(it_block);

  it_block_header->block_id = new_block_id;                 // Note that after a successful insertion the iterator will now be looking at the new
  it_block_header->friends = friends;                       // block inserted, if you don't want this you should store block_id of previous block
  it_block_header->local_depth = local_depth;               // so you can reset back to it.
  it_block_header->lowest_index = lowest_index;
  it_block_header->num_of_recs = 0;
  it_block_header->next_block_id = temp;
  return HT_OK;
}

void iterator_destroy(){                                    // The destroy function should be used at the end to free memory and store block data
  BF_Block_SetDirty(it_block);
  BF_UnpinBlock(it_block);
  BF_Block_Destroy(&it_block);
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////// Functions for the creation of file ////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////

HT_ErrorCode create_file_header(BF_Block* header_block, int fileDesc, int depth){
  CALL_BF(BF_AllocateBlock(fileDesc, header_block), HT_ERROR);
  file_info* file_header = (file_info*) BF_Block_GetData(header_block); 
  file_header->depth = depth;
  file_header->num_of_indexes = ptwo(depth);
  file_header->recs_per_block = (BF_BLOCK_SIZE - sizeof(block_info)) / sizeof(Record);
  return HT_OK;
}

HT_ErrorCode create_hashtable_blocks(BF_Block* header_block, int fileDesc){
  file_info* file_header = (file_info*) BF_Block_GetData(header_block);

  // We create the first block of the hashtable, that's because
  // iterator cannot be used if there is no block to start from
  BF_Block* block;
  BF_Block_Init(&block);
  CALL_BF(BF_AllocateBlock(fileDesc, block), HT_ERROR);
  block_info* block_header = getBlockInfo(block);

  block_header->block_id = 1;
  block_header->num_of_recs = 0;
  block_header->friends = 1;
  block_header->local_depth = file_header->depth;
  block_header->lowest_index = 0;
  block_header->next_block_id = -1;

  BF_Block_SetDirty(block);
  BF_UnpinBlock(block);
  BF_Block_Destroy(&block);

  // Now we can use the iterator to insert more blocks,
  // we will need a temporar handler for this:
  HT_Handler handler;
  handler.fileDesc = fileDesc;
  handler.file_header = file_header;

  iterator_init(1, &handler);
  for(int i = 1; i < file_header->num_of_indexes; i++){
    CALL_BF(iterator_insert_block(1, file_header->depth, i), HT_ERROR);
  }
  iterator_destroy();

  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int depth) {
  // Creates a new hashtable file
  int fileDesc;
  CALL_BF(BF_CreateFile(filename), HT_ERROR);
  CALL_BF(BF_OpenFile(filename, &fileDesc), HT_ERROR);

  BF_Block* header_block;
  BF_Block_Init(&header_block);

  CALL_BF(create_file_header(header_block, fileDesc, depth), HT_ERROR);
  CALL_BF(create_hashtable_blocks(header_block, fileDesc), HT_ERROR);

  BF_Block_SetDirty(header_block);
  BF_UnpinBlock(header_block);
  BF_Block_Destroy(&header_block);
  BF_CloseFile(fileDesc);

  return HT_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////// Functions for opening file ////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////

int findAvailableIndex(){
  for(int i = 0; i < MAX_OPEN_FILES; i++){
    if(hashtables[i] == NULL)
      return i;
  } 
}

HT_ErrorCode initialize_handler(HT_Handler* handler, file_info* file_header, int fileDesc){
  handler->arr = malloc(sizeof(int) * file_header->num_of_indexes);
  if(handler->arr == NULL) return HT_ERROR;

  handler->file_header = file_header;
  handler->fileDesc = fileDesc;

  iterator_init(1, handler);

  int i = 0;
  while(i < file_header->num_of_indexes){
    for(int k = 0; k < it_block_header->friends; k++){
      handler->arr[i] = it_block_header->block_id;
      i++;
    }
    iterator_skip_block();
  }

  iterator_destroy();
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
  if(numOfFiles == MAX_OPEN_FILES) return HT_ERROR;
  
  int fileDesc;
  BF_Block* header_block;

  CALL_BF(BF_OpenFile(fileName, &fileDesc), HT_ERROR);

  BF_Block_Init(&header_block);
  BF_GetBlock(fileDesc, 0, header_block);

  file_info* file_header = (file_info*) BF_Block_GetData(header_block);

  int index = findAvailableIndex();

  hashtables[index] = malloc(sizeof(HT_Handler));
  if(hashtables[index] == NULL) return HT_ERROR;
  initialize_handler(hashtables[index], file_header, fileDesc);
  
  *indexDesc = index;
  numOfFiles++;
  BF_Block_Destroy(&header_block);
  return HT_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////// Functions for closing file ////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////

HT_ErrorCode HT_CloseFile(int indexDesc){
  HT_Handler* handler = hashtables[indexDesc];
  BF_Block* header_block;
  BF_Block_Init(&header_block);
  BF_GetBlock(handler->fileDesc, 0, header_block);
  BF_Block_SetDirty(header_block);
  BF_UnpinBlock(header_block);
  BF_Block_Destroy(&header_block);
  free(handler->arr);
  free(handler);
  hashtables[indexDesc] = NULL;
  numOfFiles -= 1;
  return HT_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////// Functions for inserting an entry //////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////

#define FNV_OFFSET_BASIS 0x811C9DC5
#define FNV_PRIME 0x01000193

// Function to calculate FNV-1a hash for an integer
// We took this hash function from the internet, we didn't invent it
uint32_t hash(int id, int depth) {
  uint32_t hash_value = FNV_OFFSET_BASIS;
  unsigned char *byte_ptr = (unsigned char *)&id;

  for (size_t i = 0; i < sizeof(int); ++i) {
    hash_value ^= byte_ptr[i];
    hash_value *= FNV_PRIME;
  }

  return (hash_value >> (32 - depth));
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
  HT_Handler* handler = hashtables[indexDesc];

  uint32_t i = hash(record.id, handler->file_header->depth);

  iterator_init(handler->arr[i], handler);

  BF_ErrorCode signal;
  signal = iterator_add_rec(record);

  if(signal == HT_OK){
    iterator_destroy();
    return HT_OK;
  }

  if(it_block_header->friends > 1){
    // We try splitting a block in two
    int numOfRecs = it_block_header->num_of_recs;
    Record* records = malloc(sizeof(Record) * numOfRecs);
    for(int k = 0; k < numOfRecs; k++){
      records[k] = *(iterator_next_rec());
    }
    iterator_clear_recs();

    it_block_header->friends = it_block_header->friends / 2;
    it_block_header->local_depth = it_block_header->local_depth + 1;
  
    int lowest_index = it_block_header->lowest_index + it_block_header->friends;
    int friends = it_block_header->friends;
    int local_depth = it_block_header->local_depth;
    CALL_BF(iterator_insert_block(friends, local_depth, lowest_index), HT_ERROR);

    for(int k = 0; k < it_block_header->friends; k++){
      handler->arr[it_block_header->lowest_index + k] = it_block_header->block_id;
    }

    iterator_destroy();

    // Recursion happens here
    for(int k = 0; k < numOfRecs; k++){
      CALL_BF(HT_InsertEntry(indexDesc, records[k]), HT_ERROR);
    }
    CALL_BF(HT_InsertEntry(indexDesc, record), HT_ERROR);
  
    free(records);
    return HT_OK;
  }

  // We double the size of the hashtable
  handler->file_header->num_of_indexes *= 2;
  handler->file_header->depth += 1;
  handler->arr = realloc(handler->arr, sizeof(int) * handler->file_header->num_of_indexes);
  if(handler->arr == NULL) return HT_ERROR;

  iterator_reset(1);
  int index = 0;

  while(1){
    it_block_header->friends *= 2;
    it_block_header->lowest_index = index;
    for(int k = 0; k < it_block_header->friends; k++){
      handler->arr[index] = it_block_header->block_id;
      index++;
    }
    if(it_block_header->next_block_id == -1) break;
    iterator_skip_block();
  }

  iterator_destroy();

  // Recursion happens here
  CALL_BF(HT_InsertEntry(indexDesc, record), HT_ERROR);

  return HT_OK;
} // If I have thought this well the code above should work

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////// Functions for printing entries ////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////

void printRecord(Record record){
  // A very hard and complex function for printing a record
  printf("%d, %s, %s, %s\n", record.id, record.name, record.surname, record.city);
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  HT_Handler* handler = hashtables[indexDesc];

  if(id == NULL){
    // print EVERYTHING!!!
    iterator_init(1, handler);

    Record* recPtr;
 
    while(1){
      recPtr = iterator_next_rec();
      if(recPtr == NULL){
        break;
      }
      printRecord(*recPtr);
    }
  
    iterator_destroy();
    return HT_OK;
  }

  // Print only records of id
  uint32_t index = hash(*id, handler->file_header->depth);

  iterator_init(handler->arr[index], handler);

  Record* recPtr;
  for(int i = 0; i < it_block_header->num_of_recs; i++){
    recPtr = iterator_next_rec();
    if(recPtr->id == *id){
      printRecord(*recPtr);
    }
  }

  iterator_destroy();

  return HT_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////// Functions for printing statistics /////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////

HT_ErrorCode HashStatistics(int indexDesc){
  int fileDesc;
  HT_Handler* handler = hashtables[indexDesc];

  iterator_init(1, handler);

  int num_of_blocks = getNumOfBlocks(fileDesc);
  int num_of_recs = 0;
  int min_recs = handler->file_header->recs_per_block;
  int max_recs = 0;

  while(1){
    int recs = it_block_header->num_of_recs;
    num_of_recs += recs;
    if(recs < min_recs) min_recs = recs;
    if(recs > max_recs) max_recs = recs;
    if(it_block_header->next_block_id == -1) break;
    iterator_skip_block();
  }

  iterator_destroy();

  int mean_num_of_recs = num_of_recs / (num_of_blocks - 1);

  printf("Number of blocks: %d\n", num_of_blocks);
  printf("Least number of records in a block: %d\n", min_recs);
  printf("Avarage number of records in blocks: %d\n", mean_num_of_recs);
  printf("Max number of records in a block: %d\n", max_recs);

  return HT_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////// Other functions ///////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////

HT_ErrorCode HT_Init() {
  // Important because to check if an index is available we check if the pointer is NULL
  for(int i = 0; i < MAX_OPEN_FILES; i++){
    hashtables[i] = NULL;
  }
  return HT_OK;
}