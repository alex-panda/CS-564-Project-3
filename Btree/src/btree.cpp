/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

// GIVEN IMPORTS START
#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
// GIVEN IMPORTS END

// STUDENT IMPORTS START
// STUDENT IMPORTS END

//#define DEBUG

namespace badgerdb
{

    // -----------------------------------------------------------------------------
    // BTreeIndex::BTreeIndex -- Constructor
    // -----------------------------------------------------------------------------

    /**
* BTreeIndex Constructor.
* Check to see if the corresponding index file exists. If so, open the file.
* If not, create it and insert entries for every tuple in the base relation using FileScan class.
*
* @param relationName               Name of file.
* @param outIndexName               Return the name of index file.
* @param bufMgrIn                   Buffer Manager Instance
* @param attrByteOffset             Offset of attribute, over which index is to be built, in the record
* @param attrType                   Datatype of attribute over which index is built
* @throws  BadIndexInfoException    If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
*/
    BTreeIndex::BTreeIndex(const std::string &relationName,
                           std::string &outIndexName,
                           BufMgr *bufMgrIn,
                           const int attrByteOffset,
                           const Datatype attrType)
    {
        // Create the file name
        std::ostringstream idxStr;
        idxStr << relationName << " . " << attrByteOffset;
        outIndexName = idxStr.str(); // indexName is the name of the index file

        // set fields
        this->bufMgr = bufMgrIn;
        this->attrByteOffset = attrByteOffset;
        this->attributeType = attributeType;

        this->leafOccupancy = INTARRAYLEAFSIZE;
        this->nodeOccupancy = INTARRAYNONLEAFSIZE;
        this->scanExecuting = false;

        // Check to see if the corresponding index file exists
        try
        {
            // if file exists, open the file
            this->file = new File(outIndexName, false); // Try to open existing file

            // Read file info
            this->headerPageNum = this->file->getFirstPageNo();

            // Retrieve header page
            Page *headerPage;
            this->bufMgr->readPage(this->file, this->headerPageNum, headerPage);

            // Get meta info from headerPage
            IndexMetaInfo *meta = (IndexMetaInfo *)headerPage;
            this->rootPageNum = meta->rootPageNo;

            // Unpin page that was pinned when readPage was called
            bufMgr->unPinPage(this->file, this->headerPageNum, false);

            // Make sure that this is valid index info
            if (this->relationName != meta->relationName || this->attrType != meta->attrType || this->attrByteOffset != meta->attrByteOffset)
                throw BadIndexInfoException(outIndexName);
        }
        catch (FileNotFoundException &e)
        {
            // if file does not exist, create it and insert entries for every tuple
            // in the base relation using FileScan class
            this->file = File(outIndexName, true); // Create new file

            // allocate pages
            Page *headerPage;
            this->bufMgr->allocPage(this->file, this->headerPageNum, headerPage);

            Page *rootPage;
            this->bufMgr->allocPage(this->file, this->rootPageNum, rootPage);

            // complete meta information for new pages
            IndexMetaInfo *meta = (IndexMetaInfo *)headerPage;
            meta->attrByteOffset = this->attrByteOffset;
            meta->attrType = this->attrType;
            meta->rootPageNo = this->rootPageNum;
            strncpy((char *)(&(meta->relationName)), relationName.c_str(), 20);
            meta->relationName[19] = 0;

            // init root
            LeafNodeInt *root = (LeafNodeInt *)rootPage;
            root->rightSibPageNo = 0;

            // Unpin pages, they are no longer needed
            bufMgr->unPinPage(this->file, this->headerPageNum, true);
            bufMgr->unPinPage(this->file, this->rootPageNum, true);

            // insert entries for every tuple in the base relation using FileScan
            FileScan fileScan(outIndexName, this->bufMgr);
            RecordId rid;

            try
            {
                // Actually insert the entries in a while loop
                while (true)
                {
                    fileScan.scanNext(rid);
                    std::string record = fileScan.getRecord();
                    this->insertEntry(record.c_str() + this->attrByteOffset, rid);
                }
            }
            catch (EndOfFileException &e)
            {
                // Save the Index to the file
                this->bufMgr->FlushFile(this->file);
            }
        }
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::~BTreeIndex -- destructor
    // -----------------------------------------------------------------------------

    /**
* BTreeIndex Destructor.
* End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
* and delete file instance thereby closing the index file.
* Destructor should not throw any exceptions. All exceptions should be caught in here itself.
**/
    BTreeIndex::~BTreeIndex()
    {
        // ends any initialized scan, unpinning any pages pinned by it
        try
        {
            this->endScan();
        }
        catch (ScanNotInitializedException &e)
        {
            // No scan had been initialized so don't need to do anything
        }

        // flushes file, throws error
        try
        {
            this->bufMgr->flushFile(this->file);
        }
        catch (PagePinnedException &e)
        {
            throw e;
            // pages where pinned if this is caught so they need to be unpinned
        }

        // deletes file if necessary
        if (this->file != NULL)
        {
            delete this->file;
            this->file = NULL;
        }
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::insertEntry
    // -----------------------------------------------------------------------------

    /**
* Insert a new entry using the pair <value,rid>.
* Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
* This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
* This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
* Make sure to unpin pages as soon as you can.
* @param key			Key to insert, pointer to integer/double/char string
* @param rid			Record ID of a record whose entry is getting inserted into the index.
**/
    void BTreeIndex::insertEntry(const void *key, const RecordId rid)
    {
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::startScan
    // -----------------------------------------------------------------------------

    /**
* Begin a filtered scan of the index.  For instance, if the method is called
* using ("a",GT,"d",LTE) then we should seek all entries with a value
* greater than "a" and less than or equal to "d".
* If another scan is already executing, that needs to be ended here.
* Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
* that satisfies the scan parameters. Keep that page pinned in the buffer pool.
* @param lowVal	Low value of range, pointer to integer / double / char string
* @param lowOp		Low operator (GT/GTE)
* @param highVal	High value of range, pointer to integer / double / char string
* @param highOp	High operator (LT/LTE)
* @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values
* @throws  BadScanrangeException If lowVal > highval
* @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
**/
    void BTreeIndex::startScan(const void *lowValParm,
                               const Operator lowOpParm,
                               const void *highValParm,
                               const Operator highOpParm)
    {
        // Check that the parameters are valid
        if (!((lowOpParm == GT || lowOpParm == GTE) && (highOpParm == LT || highOpParm == LTE)))
        {
            throw BadOpcodesException();
        }

        // Get Int values range for scan
        this->lowValInt = *((int *)lowValParm);
        this->highValInt = *((int *)highValParm);

        if (this->attrByteOffset ==)
            // Check that the ranges are valid
            if (this->lowValInt > this->highValInt)
            {
                throw BadScanrangeException();
            }

        this->lowOp = lowOpParm;
        this->highOp = highOpParm;

        // If the scan is already started, end it and start a new one
        if (this->scanExecuting)
        {
            this->endScan();
        }

        this->currentPageNum = this->rootPageNum;

        // Read root page into the buffer pool
        this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);

        NonLeafNodeInt *currNode = (NonLeafNodeInt *)this->currentPageData;
        bool leafFound = false;

        // if currNode (the root node) is not a leaf node, then we need to find the
        //      leaf node
        if (currNode->level != 0)
        {

            // search for leaves
            while (!leafFound)
            {
                currNode = (NonLeafNodeInt *)this->currentPageData;
                bool pageFound = false;

                // if correct leaf has been found, end while loop
                if (currNode->level == 1)
                {
                    leafFound = true;
                }

                for (int i = 0; i < this->nodeOccupancy; i++)
                {

                    if (currNode->keyArray[i] == 0 && this->lowValInt > currNode->keyArray[i])
                    {
                        pageFound = true;
                        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
                        this->currentPageNum = this->currNode->pageNoArray[i + 1];
                        this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
                        break;
                    }

                    if (this->lowValInt <= this->currNode->keyArray[i])
                    {
                        pageFound = true;
                        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
                        this->currentPageNum = currNode->pageNoArray[i];
                        this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
                        break;
                    }
                }
            }

            // We found the leaf node
            LeafNodeInt *leafPage = (LeafNodeInt *)this->currentPageData;

            bool looking = true;
            int keyIdx;

            while (looking)
            {
                leafPage = (LeafNodeInt *)this->currentPageData;
                for (keyIdx = 0; keyIdx < this->leafOccupancy; keyIdx++)
                {
                    if (lowOpParm == GT)
                    {
                        if (lowValInt >= leafPage->keyArray[keyIndex])
                        {
                            continue;
                        }
                    }
                    else
                    {
                        if (lowValInt > leafPage->keyArray[keyIndex])
                        {
                            continue;
                        }
                    }

                    if (highOpParm == LT)
                    { // not in criteria if key <= highValInt
                        if (highValInt <= leafPage->keyArray[keyIndex])
                        {
                            throw NoSuchKeyFoundException();
                        }
                    }
                    else
                    { // not in criteria, if key < highValInt
                        if (highValInt < leafPage->keyArray[keyIndex])
                        {
                            throw NoSuchKeyFoundException();
                        }
                    }

                    looking = false;
                    break;
                }

                // If we have reached this point without finding the specified key,
                //      then throw exception
                if (leafPage->rightSibPageNo == 0 && looking)
                {
                    throw NoSuchKeyFoundException();
                }

                // look at leaf node to the right of this one
                if (looking)
                {
                    this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
                    this->currentPageNum = leafPage->rightSibPageNo;
                    this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
                }
            }
            this->nextEntry = keyIdx + 1;
        }
        else
        {
            // make the current page the root if it is a leaf
            this->currentPageNum = rootPageNum;
            this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageNum);
            this->nextEntry = 0; // reset next entry because we have reached a new page
        }
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::scanNext
    // -----------------------------------------------------------------------------

    /**
* Fetch the record id of the next index entry that matches the scan.
* Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
* @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
* @throws ScanNotInitializedException If no scan has been initialized.
* @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
**/
    void BTreeIndex::scanNext(RecordId &outRid)
    {
        if (!scanExecuting)
            throw ScanNotInitializedException();

        LeafNodeInt *node = (LeafNodeInt *)currentPageData;
        outRid = node->ridArray[nextEntry];
        int val = node->keyArray[nextEntry];

        if ((outRid.page_number == 0 && outRid.slot_number == 0) || val > highValInt || (val = highValInt && highOp == LT))
        {
            throw IndexScanCompletedException();
        }

        nextEntry++;
        LeafNodeInt *node = (LeafNodeInt *)currentPageData;

        if (nextEntry >= INTARRAYLEAFSIZE || node->ridArray[nextEntry].page_number == 0)
        {
            bufMgr->unPinPage(file, currentPageNum, false);
            currentPageNum = node->rightSibPageNo;
            bufMgr->readPage(file, currentPageNum, currentPageData);
            nextEntry = 0;
        }
    }

    // -----------------------------------------------------------------------------
    // BTreeIndex::endScan
    // -----------------------------------------------------------------------------

    /**
* Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
* @throws ScanNotInitializedException If no scan has been initialized.
**/
    void BTreeIndex::endScan()
    {
        if (!scanExecuting)
            throw ScanNotInitializedException();
        scanExecuting = false;
        bufMgr->unPinPage(file, currentPageNum, false);
    }

}
