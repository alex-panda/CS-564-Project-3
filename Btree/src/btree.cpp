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
#include "exceptions/page_pinned_exception.h"

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
            this->file = new BlobFile(outIndexName, false); // Try to open existing file

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
            if (relationName != meta->relationName || attrType != meta->attrType || this->attrByteOffset != meta->attrByteOffset)
                throw BadIndexInfoException(outIndexName);
        }
        catch (FileNotFoundException &e)
        {
            // if file does not exist, create it and insert entries for every tuple
            // in the base relation using FileScan class
            this->file = new BlobFile(outIndexName, true); // Create new file

            // allocate pages
            Page *headerPage;
            this->bufMgr->allocPage(this->file, this->headerPageNum, headerPage);

            Page *rootPage;
            this->bufMgr->allocPage(this->file, this->rootPageNum, rootPage);

            // complete meta information for new pages
            IndexMetaInfo *meta = (IndexMetaInfo *)headerPage;
            meta->attrByteOffset = this->attrByteOffset;
            meta->attrType = attrType;
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
                this->bufMgr->flushFile(this->file);
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
* @param key            Key to insert, pointer to integer/double/char string
* @param rid            Record ID of a record whose entry is getting inserted into the index.
**/
    const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
    {
        RIDKeyPair<int> newEntry;
        newEntry.set(rid, *((int *)key));

        PageKeyPair<int> *newInternal = nullptr;

        Page *rootPage;
        this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);

        insert(rootPage, this->rootPageNum, initialRootPageNum == this->rootPageNum, newEntry, newInternal);
    }

    const void BTreeIndex::insert(Page *currPage, PageId currPageId, bool isLeaf, const RIDKeyPair<int> newEntry, PageKeyPair<int> *&newInternal)
    {
        if (isLeaf)
        {
            // get current page
            LeafNodeInt *leaf = reinterpret_cast<LeafNodeInt *>(currPage);

            if (leaf->ridArray[this->leafOccupancy - 1].page_number == 0)
            {
                insertLeaf(leaf, newEntry); // node is not full, so insert leaf
                this->bufMgr->unPinPage(this->file, currPageId, true);
            }
            else
            {
                splitLeaf(leaf, currPageId, newInternal, newEntry); // split leaf into two nodes, push mid val up to parent node
            }
        }
        else
        {
            NonLeafNodeInt *currNode = reinterpret_cast<NonLeafNodeInt *>(currPage);

            Page *nextPage;
            PageId nextNodeNum;

            // find next node one level down
            findNextInternal(currNode, nextNodeNum, newEntry.key);

            // determine node type of next page
            this->bufMgr->readPage(this->file, nextNodeNum, nextPage);

            // traverse
            insert(nextPage, nextNodeNum, currNode->level == 1, newEntry, newInternal);

            if (!newInternal)
            {
                this->bufMgr->unPinPage(this->file, currPageId, false); // parent node did not need to be split
            }
            else
            {
                if (currNode->pageNoArray[this->nodeOccupancy] == 0)
                {
                    insertInternal(currNode, newInternal); // internal not full so insert
                    newInternal = nullptr;
                    this->bufMgr->unPinPage(this->file, currPageId, true);
                }
                else
                {
                    splitInternal(currNode, currPageId, newInternal);
                }
            }
        }
    }

    const void BTreeIndex::findNextInternal(NonLeafNodeInt *internal, PageId &pageId, int key)
    {
        int i;

        for (i = this->nodeOccupancy; i > 0; i--)
        {
            if (internal->pageNoArray[i] != 0 && internal->keyArray[i - 1] < key)
            {
                pageId = internal->pageNoArray[i];
                return;
            }
        }

        pageId = internal->pageNoArray[0];
    }

    const void BTreeIndex::updateRoot(PageId firstPageInRoot, PageKeyPair<int> *newInternal)
    {
        // create a new root
        PageId newRootPageNum;
        Page *newRoot;
        this->bufMgr->allocPage(this->file, newRootPageNum, newRoot);
        NonLeafNodeInt *newRootPage = reinterpret_cast<NonLeafNodeInt *>(newRoot);

        // update metadata
        newRootPage->level = initialRootPageNum == this->rootPageNum ? 1 : 0;
        newRootPage->pageNoArray[0] = firstPageInRoot;
        newRootPage->pageNoArray[1] = newInternal->pageNo;
        newRootPage->keyArray[0] = newInternal->key;

        Page *meta;
        this->bufMgr->readPage(this->file, this->headerPageNum, meta);
        IndexMetaInfo *metaPage = (IndexMetaInfo *)meta;
        metaPage->rootPageNo = newRootPageNum;
        this->rootPageNum = newRootPageNum;
        // unpin unused page
        this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
        this->bufMgr->unPinPage(this->file, newRootPageNum, true);
    }

    const void BTreeIndex::splitLeaf(LeafNodeInt *leaf, PageId leafPageId, PageKeyPair<int> *&newInternal, RIDKeyPair<int> newEntry)
    {
        // allocate a new leaf page
        PageId newPageId;
        Page *newPage;
        this->bufMgr->allocPage(this->file, newPageId, newPage);
        LeafNodeInt *newLeaf = reinterpret_cast<LeafNodeInt *>(newPage);

        int mid = this->leafOccupancy % 2 == 0 ? (this->leafOccupancy / 2) : (this->leafOccupancy / 2 + 1);

        // copy half the page to newLeaf
        for (int i = mid; i < this->leafOccupancy; i++)
        {
            newLeaf->keyArray[i - mid] = leaf->keyArray[i];
            newLeaf->ridArray[i - mid] = leaf->ridArray[i];
            leaf->keyArray[i] = 0;
            leaf->ridArray[i].page_number = 0;
        }

        // insert newEntry into appropriate leaf
        if (newEntry.key > leaf->keyArray[mid - 1])
        {
            insertLeaf(newLeaf, newEntry);
        }
        else
        {
            insertLeaf(leaf, newEntry);
        }

        // update sibling pointers
        newLeaf->rightSibPageNo = leaf->rightSibPageNo;
        leaf->rightSibPageNo = newPageId;

        // copy up the smallest key from new leaf to parent
        newInternal = new PageKeyPair<int>();
        PageKeyPair<int> newKeyPair;
        newKeyPair.set(newPageId, newLeaf->keyArray[0]);
        newInternal = &newKeyPair;

        this->bufMgr->unPinPage(this->file, leafPageId, true);
        this->bufMgr->unPinPage(this->file, newPageId, true);

        if (leafPageId == this->rootPageNum)
        { // leaf is the root
            updateRoot(leafPageId, newInternal);
        }
    }

    const void BTreeIndex::insertLeaf(LeafNodeInt *leaf, RIDKeyPair<int> newEntry)
    {
        if (leaf->ridArray[0].page_number != 0)
        { // leaf is not empty
            int i;

            for (i = this->leafOccupancy - 1; i >= 0; i--)
            {
                if (leaf->ridArray[i].page_number != 0)
                {
                    if (leaf->keyArray[i] > newEntry.key)
                    { // shift existing entry
                        leaf->keyArray[i + 1] = leaf->keyArray[i];
                        leaf->ridArray[i + 1] = leaf->ridArray[i];
                    }
                    else
                    { // insert new entry
                        leaf->keyArray[i + 1] = newEntry.key;
                        leaf->ridArray[i + 1] = newEntry.rid;
                        return;
                    }
                }
            }
        }

        leaf->keyArray[0] = newEntry.key;
        leaf->ridArray[0] = newEntry.rid;
    }

    const void BTreeIndex::splitInternal(NonLeafNodeInt *oldNode, PageId oldPageId, PageKeyPair<int> *&newInternal)
    {
        // allocate a new internal node
        PageId newPageId;
        Page *newPage;
        this->bufMgr->allocPage(this->file, newPageId, newPage);
        NonLeafNodeInt *newNode = reinterpret_cast<NonLeafNodeInt *>(newPage);

        int mid = this->nodeOccupancy / 2;
        int pushupIndex = mid;
        PageKeyPair<int> pushupEntry;
        // even number of keys
        if (this->nodeOccupancy % 2 == 0)
        {
            pushupIndex = newInternal->key < oldNode->keyArray[mid] ? mid - 1 : mid;
        }
        pushupEntry.set(newPageId, oldNode->keyArray[pushupIndex]);

        mid = pushupIndex + 1;
        // move half the entries to the new node
        for (int i = mid; i < this->nodeOccupancy; i++)
        {
            newNode->keyArray[i - mid] = oldNode->keyArray[i];
            newNode->pageNoArray[i - mid] = oldNode->pageNoArray[i + 1];
            oldNode->pageNoArray[i + 1] = (PageId)0;
            oldNode->keyArray[i + 1] = 0;
        }

        newNode->level = oldNode->level;
        // remove the entry that is pushed up from current node
        oldNode->keyArray[pushupIndex] = 0;
        oldNode->pageNoArray[pushupIndex] = (PageId)0;
        // insert the new child entry
        insertInternal(newInternal->key < newNode->keyArray[0] ? oldNode : newNode, newInternal);
        // newInternal = new PageKeyPair<int>();
        newInternal = &pushupEntry;
        this->bufMgr->unPinPage(this->file, oldPageId, true);
        this->bufMgr->unPinPage(this->file, newPageId, true);

        // if the currNode is the root
        if (oldPageId == this->rootPageNum)
        {
            updateRoot(oldPageId, newInternal);
        }
    }

    const void BTreeIndex::insertInternal(NonLeafNodeInt *internal, PageKeyPair<int> *newEntry)
    {
        int i;

        for (i = this->nodeOccupancy; i >= 0; i--)
        {
            if (internal->pageNoArray[i] == 0)
            { // no page here
                continue;
            }
            else
            {
                if (internal->keyArray[i - 1] > newEntry->key)
                { // shift existing entry
                    internal->keyArray[i] = internal->keyArray[i - 1];
                    internal->pageNoArray[i + 1] = internal->pageNoArray[i];
                }
                else
                { // insert new entry
                    internal->keyArray[i] = newEntry->key;
                    internal->pageNoArray[i + 1] = newEntry->pageNo;
                    return;
                }
            }
        }

        internal->keyArray[0] = newEntry->key;
        internal->pageNoArray[1] = newEntry->pageNo;
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
* @param lowVal Low value of range, pointer to integer / double / char string
* @param lowOp      Low operator (GT/GTE)
* @param highVal    High value of range, pointer to integer / double / char string
* @param highOp High operator (LT/LTE)
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
                        this->currentPageNum = currNode->pageNoArray[i + 1];
                        this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
                        break;
                    }

                    if (this->lowValInt <= currNode->keyArray[i])
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
                        if (lowValInt >= leafPage->keyArray[keyIdx])
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
* @param outRid RecordId of next record found that satisfies the scan criteria returned in this
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
