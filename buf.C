#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


//----------------------------------------
// allocBuf(): allocate a free buffer frame
//----------------------------------------
const Status BufMgr::allocBuf(int &frame) 
{
    int probes = 0; // number of frames inspected

    while(probes < 2*numBufs) 
    {
        advanceClock();

        BufDesc* desc = &bufTable[clockHand];

        // Case 1 : found an invalid (empty) frame - take it
        if (desc->valid == false) 
        {
            // found an invalid frame
            frame = desc->frameNo;
            return OK;
        }

        // Case 2: frame has been recently referenced - give it a second chance
        if (desc->refbit == true) 
        {
            // give a second chance
            desc->refbit = false;
            ++probes;
            continue;
        }

        // Case 3: pinned — cannot evict
        if (desc->pinCnt > 0) {
            ++probes;
            continue;
        }

        // Case 4: 
        if(desc->dirty == true) 
        {
            // write the page back to disk
#ifdef DEBUGBUF
            cout << "writing dirty page " << desc->pageNo
                    << " from frame " << desc->frameNo << endl;
#endif
        Status w = desc->file->writePage(desc->pageNo,
                                              &(bufPool[desc->frameNo]));
            if (w != OK)
                return UNIXERR;
            bufStats.diskwrites++;
            desc->dirty = false;
        }

        // remove this page from the hash table
        hashTable->remove(desc->file, desc->pageNo);

        // Clear the buffer descriptor
        desc->Clear();

        // return the frame number
        frame = desc->frameNo;
        return OK;

    }
        // All cases exhausted - keep looking
        return BUFFEREXCEEDED;
        
}


//----------------------------------------
// readPage(): read a page from file into buffer pool
//----------------------------------------	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{

int frameNo = -1;

// 1) look up (file, PageNo) in the hash table
Status status = hashTable->lookup(file, PageNo, frameNo);

// Case 2: if found, pin it and return pointer to page in buffer pool
if (status == OK)
{
    BufDesc &desc = bufTable[frameNo];
    desc.refbit = true; // recently referenced
    desc.pinCnt++; // extend pin count
    page = &bufPool[frameNo];
    return OK;
}

// If lookup failed with anything other than "not found", it’s a hash error.
if (status != HASHNOTFOUND) return HASHTBLERROR;

// Case 1: page not found in buffer pool

// allocate a buffer frame
Status a = allocBuf(frameNo);
if (a != OK) return a; /// BUFFEREXCEEDED or UNIXERR from allocBuf

// read the page from disk into the allocated frame
Status r = file->readPage(PageNo, &bufPool[frameNo]);
if (r != OK) return r; // UNIXERR from readPage
bufStats.diskreads++;

// Insert (file, PageNo) into hash table mapping to frameNo
Status h = hashTable->insert(file, PageNo, frameNo);
if (h != OK) return HASHTBLERROR;

// Initialize the buffer descriptor (pinCnt=1, dirty=false, valid=true)
bufTable[frameNo].Set(file, PageNo);

// return pointer to the page in buffer pool
page = &bufPool[frameNo];
return OK;
}





const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo = -1;

    // look up (file, PageNo) in the hash table
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if(status != OK){
        // Spec : return HASHNOTFOUND if page is not in buffer pool
        return HASHNOTFOUND;
    }

    BufDesc &desc= bufTable[frameNo];

    // If pinCnt is already zero, return PAGENOTPINNED
    if(desc.pinCnt == 0){
        return PAGENOTPINNED;
    }

    // decrement pinCnt
    desc.pinCnt--;

    // If caller reports modification, set dirty bit
    if(dirty){
        desc.dirty = true;
    }

    return OK;

}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{

// 1. allocate a new page in the file
Status status = file->allocatePage(pageNo);
if (status != OK) return UNIXERR; // UNIXERR from allocatePage

// 2. allocate a buffer frame
int frameNo = -1;
Status a = allocBuf(frameNo);
if (a != OK) return a; // BUFFEREXCEEDED or UNIXERR from allocBuf

// 3. Insert (file, pageNo) into hash table mapping to frameNo
Status h = hashTable->insert(file, pageNo, frameNo);
if (h != OK) return HASHTBLERROR;

// 4. Initialize the buffer descriptor (pinCnt=1, dirty=false, valid=true)
bufTable[frameNo].Set(file, pageNo);

// 5. return pointer to the page in buffer pool
page = &bufPool[frameNo];
return OK;



}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}

//----------------------------------------
// end of buf.C
//----------------------------------------



