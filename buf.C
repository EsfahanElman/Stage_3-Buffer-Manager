#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
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

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

/*
    Allocates a free buffer frame for use in the buffer pool using the Clock replacement policy.
    If a victim page is dirty, it is written back to disk before the frame storing it is being reused.

    Input:
        int & frame - reference to an integer that will store the index of the allocated frame

    Output:
        Updates 'frame' to the number of the allocated buffer frame.

    Return value:
        OK - if a free or replaceable frame is successfully allocated
        BUFFEREXCEEDED - if all buffer frames are pinned
        UNIXERR - if the call to the I/O layer returned an error when a dirty page was being written to disk
*/
const Status BufMgr::allocBuf(int &frame)
{

    // TO DELETE
    /*
        Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to disk.
        Returns BUFFEREXCEEDED if all buffer frames are pinned, UNIXERR if the call to the I/O layer returned an error when a dirty page was being written to disk and OK otherwise.
        This private method will get called by the readPage() and allocPage() methods described below.
        Make sure that if the buffer frame allocated has a valid page in it, that you remove the appropriate entry from the hash table.
    */
    int hand = 0;
    while (hand < 2 * numBufs)
    {
        advanceClock();
        BufDesc *frame_desc = &bufTable[clockHand];

        // Case 1: Empty frame available - invalid frame
        if (!frame_desc->valid)
        {
            frame = frame_desc->frameNo;
            return OK;
        }

        // Case 2: Recently referenced – give second chance
        if (frame_desc->refbit)
        {
            frame_desc->refbit = false;
            hand++;
            continue;
        }

        // Case 3: Pinned – cannot evict
        if (frame_desc->pinCnt > 0)
        {
            hand++;
            continue;
        }

        // Case 4: Dirty page – write page back to disk
        if (frame_desc->dirty)
        {
            Status writepage_status = frame_desc->file->writePage(frame_desc->pageNo, &(bufPool[frame_desc->frameNo]));
            if (writepage_status != OK)
                return UNIXERR;
            bufStats.diskwrites++;
            frame_desc->dirty = false;
        }

        // Remove page from hash table and clear buffer descriptor
        hashTable->remove(frame_desc->file, frame_desc->pageNo);
        frame_desc->Clear();

        frame = frame_desc->frameNo;
        return OK;
    }

    // No available frame found after full scan
    return BUFFEREXCEEDED;
}

/*
    Reads a page from disk into the buffer pool, if it is not already present.
    If the page exists in memory, increments its pin count.

    Input:
        File* file - pointer to the file containing the page
        int PageNo - page number of the page to be read from the file

    Output:
        Page*& page - reference to a pointer that will be set to the in-memory page

    Return value:
        OK - if the page is successfully loaded or already in memory, no errors occur
        HASHTBLERROR - if an error occurs while accessing the hash table
        BUFFEREXCEEDED - if all buffer frames are pinned, no frame can be allocated
        UNIXERR - if a disk read error occurs

*/
const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    // TO DELETE
    /*
        First check whether the page is already in the buffer pool by invoking the lookup() method on the hashtable to get a frame number.
        There are two cases to be handled depending on the outcome of the lookup() call:

        Case 1) Page is not in the buffer pool.  Call allocBuf() to allocate a buffer frame and then call the method file->readPage() to read the page from disk into the buffer pool frame.
        Next, insert the page into the hashtable. Finally, invoke Set() on the frame to set it up properly. Set() will leave the pinCnt for the page set to 1.
        Return a pointer to the frame containing the page via the page parameter.
        Case 2)  Page is in the buffer pool.
        In this case set the appropriate refbit, increment the pinCnt for the page, and then return a pointer to the frame containing the page via the page parameter.

        Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned, HASHTBLERROR if a hash table error occurred.
    */

    int frameNo = -1;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    // Case 1: Page is in the buffer pool
    if (status == OK)
    {
        BufDesc &frame_desc = bufTable[frameNo];
        frame_desc.refbit = true;
        frame_desc.pinCnt++;
        page = &bufPool[frameNo];
        return OK;
    }

    // TO DELETE I GUESS
    // if (status != HASHNOTFOUND)
    //     return HASHTBLERROR;

    // Case 2: Page is not in the buffer pool – need to read from disk
    Status allocBuf_status = allocBuf(frameNo);
    if (allocBuf_status != OK)
        return allocBuf_status; // BUFFEREXCEEDED or UNIXERR from allocBuf

    Status diskread_status = file->readPage(PageNo, &bufPool[frameNo]);
    if (diskread_status != OK)
        return diskread_status; // UNIXERR from readPage
    bufStats.diskreads++;

    Status ht_insert_status = hashTable->insert(file, PageNo, frameNo);
    if (ht_insert_status != OK)
        return HASHTBLERROR;

    bufTable[frameNo].Set(file, PageNo);
    page = &bufPool[frameNo];
    return OK;
}

/*
    Decrements the pinCnt of the frame containing (file, PageNo)
    If dirty == true, sets the dirty bit.

    Input:
        File* file - pointer to the file containing the page
        int PageNo - page number of the page to be unpinned in the file
        bool dirty - indicates whether the page was modified since last writtem

    Return value:
        OK - if the page is unpinned successfully, no errors occurred
        HASHNOTFOUND - if the page is not in the buffer pool hash table currently
        PAGENOTPINNED - if the pin count is already 0 when this function is called
*/
const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty)
{

    // TO DELETE
    /*
        Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, sets the dirty bit.
        Returns OK if no errors occurred, HASHNOTFOUND if the page is not in the buffer pool hash table, PAGENOTPINNED if the pin count is already 0.
    */

    int frameNo = -1;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if (status != OK)
        return HASHNOTFOUND;

    BufDesc &page_desc = bufTable[frameNo];

    if (page_desc.pinCnt == 0)
        return PAGENOTPINNED;

    page_desc.pinCnt--;
    if (dirty)
        page_desc.dirty = true;

    return OK;
}

/*
    Allocates a new page in the specified nfile and brings it into the buffer pool.
    The new page is pinned and ready for use.

    Input:
        File* pointer - pointer to the file to allocate a page in

    Output:
        int& pageNo - reference to an integer that will store the new page number
        Page*& page - reference to a pointer that will be set to the in-memory page

    Return value:
        OK - if the new page is successfully allocated and loaded, no errors occured
        UNIXERR - if a disk allocation or I/O error occurs
        BUFFEREXCEEDED - if no buffer frame could be allocated
        HASHTBLERROR - if the hash table insertion fails
*/
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
    // TO DELETE
    /*
        This call is kind of weird.
        The first step is to to allocate an empty page in the specified file by invoking the file->allocatePage() method.
        This method will return the page number of the newly allocated page.  Then allocBuf() is called to obtain a buffer pool frame.
        Next, an entry is inserted into the hash table and Set() is invoked on the frame to set it up properly.
        The method returns both the page number of the newly allocated page to the caller via the pageNo parameter and a pointer to the buffer frame allocated for the page via the page parameter.
        Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned and HASHTBLERROR if a hash table error occurred.
    */

    Status status = file->allocatePage(pageNo);
    if (status != OK)
        return UNIXERR;

    int frameNo = -1;
    Status allocBuf_status = allocBuf(frameNo);
    if (allocBuf_status != OK)
        return allocBuf_status; // BUFFEREXCEEDED or UNIXERR from allocBuf

    Status ht_status = hashTable->insert(file, pageNo, frameNo);
    if (ht_status != OK)
        return HASHTBLERROR;

    bufTable[frameNo].Set(file, pageNo);
    page = &bufPool[frameNo];
    return OK;
}

const Status BufMgr::disposePage(File *file, const int pageNo)
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

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

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
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
