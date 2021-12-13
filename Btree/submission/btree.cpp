/**
 * Student 1
 * Name: Alexander Peseckis
 * Student ID: 908-154-1840
 * Email: peseckis@wisc.edu
 *
 * Student 2
 * Name: Win San
 * Student ID: 907-936-4031
 * Email: wsan3@wisc.edu
 *
 * Student 3
 * Name: Steven Hizmi
 * Student ID: 907-965-9059
 * Email: shizmi@wisc.edu
 */

/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

// GIVEN IMPORTS START
#include "btree.h"

#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "filescan.h"

// GIVEN IMPORTS END

// STUDENT IMPORTS START
#include "exceptions/page_not_pinned_exception.h"
// STUDENT IMPORTS END

//#define DEBUG

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

/**
 * BTreeIndex Constructor.
 * Check to see if the corresponding index file exists. If so, open the file.
 * If not, create it and insert entries for every tuple in the base relation
 * using FileScan class.
 *
 * @param relationName               Name of file.
 * @param outIndexName               Return the name of index file.
 * @param bufMgrIn                   Buffer Manager Instance
 * @param attrByteOffset             Offset of attribute, over which index is to
 * be built, in the record
 * @param attrType                   Datatype of attribute over which index is
 * built
 * @throws  BadIndexInfoException    If the index file already exists for the
 * corresponding attribute, but values in metapage(relationName, attribute byte
 * offset, attribute type etc.) do not match with values received through
 * constructor parameters.
 */
BTreeIndex::BTreeIndex(const std::string &relationName,
                       std::string &outIndexName, BufMgr *bufMgrIn,
                       const int attrByteOffset, const Datatype attrType) {
  // Create the file name
  std::ostringstream idxStr;
  idxStr << relationName << "." << attrByteOffset;
  outIndexName = idxStr.str();  // indexName is the name of the index file

  // set fields
  this->bufMgr = bufMgrIn;
  this->attrByteOffset = attrByteOffset;
  this->attributeType = attrType;
  this->leafOccupancy = INTARRAYLEAFSIZE;
  this->nodeOccupancy = INTARRAYNONLEAFSIZE;
  this->scanExecuting = false;

  // Check to see if the corresponding index file exists
  try {
    // If file exists, open the file
    this->file = new BlobFile(outIndexName, false);  // Try to open existing file

    // Read file info
    this->headerPageNum = this->file->getFirstPageNo();
    Page *headerPage;
    this->bufMgr->readPage(this->file, this->headerPageNum, headerPage);

    // Get meta info from headerPage
    IndexMetaInfo *meta = (IndexMetaInfo *)headerPage;
    this->rootPageNum = meta->rootPageNo;

    // Make sure that this is valid index info
    if (relationName != meta->relationName || attrType != meta->attrType ||
        this->attrByteOffset != meta->attrByteOffset)
      throw BadIndexInfoException(outIndexName);

    // Unpin page that was pinned when readPage was called
    bufMgr->unPinPage(this->file, this->headerPageNum, false);

  } catch (FileNotFoundException &e) {
    // If file does not exist, create it and insert entries for every tuple
    // in the base relation using FileScan class
    this->file = new BlobFile(outIndexName, true);  // Create new file

    // allocate pages
    Page *headerPage;
    this->bufMgr->allocPage(this->file, this->headerPageNum, headerPage);

    Page *rootPage;
    this->bufMgr->allocPage(this->file, this->rootPageNum, rootPage);

    // Complete meta information for new pages
    IndexMetaInfo *meta = (IndexMetaInfo *)headerPage;
    meta->attrByteOffset = this->attrByteOffset;
    meta->attrType = attrType;
    meta->rootPageNo = this->rootPageNum;
    strncpy((char *)(&(meta->relationName)), relationName.c_str(), 20);
    meta->relationName[19] = 0;

    this->initialRootPageId = this->rootPageNum;

    // init root
    LeafNodeInt *root = reinterpret_cast<LeafNodeInt *>(rootPage);
    // root->rightSibPageNo = 0;

    // Unpin pages, they are no longer needed
    this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
    this->bufMgr->unPinPage(this->file, this->rootPageNum, true);

    // Insert entries for every tuple in the base relation using FileScan
    FileScan fileScan(relationName, this->bufMgr);
    RecordId rid;

    try {
      // Actually insert the entries in a while loop
      while (true) {
        fileScan.scanNext(rid);
        std::string record = fileScan.getRecord();
        this->insertEntry((int *)(record.c_str() + this->attrByteOffset), rid);
      }
    } catch (EndOfFileException &e) {
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
 * End any initialized scan, flush index file, after unpinning any pinned pages,
 *from the buffer manager and delete file instance thereby closing the index
 *file. Destructor should not throw any exceptions. All exceptions should be
 *caught in here itself.
 **/
BTreeIndex::~BTreeIndex() {
  // Ends any initialized scan, unpinning any pages pinned by it
  try {
    this->endScan();
  } catch (ScanNotInitializedException &e) {
    // No scan had been initialized so don't need to do anything
  }

  // Flushes file, throws error
  this->bufMgr->flushFile(this->file);

  // Deletes file if necessary
  if (this->file != NULL) {
    delete this->file;
    this->file = NULL;
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

/**
 * Insert a new entry using the pair <value,rid>.
 * Start from root to recursively find out the leaf to insert the entry in. The
 * insertion may cause splitting of leaf node. This splitting will require
 * addition of new leaf page number entry into the parent non-leaf, which may
 * in-turn get split. This may continue all the way upto the root causing the
 * root to get split. If root gets split, metapage needs to be changed
 * accordingly. Make sure to unpin pages as soon as you can.
 * @param key            Key to insert, pointer to integer/double/char string
 * @param rid            Record ID of a record whose entry is getting inserted
 *into the index.
 **/
void BTreeIndex::insertEntry(void *key, const RecordId rid) {
  RIDKeyPair<int> newEntry;
  newEntry.set(rid, *((int *)key));

  PageKeyPair<int> *newInternal = nullptr;

  Page *rootPage;
  this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);

  insert(rootPage, this->rootPageNum, this->initialRootPageId == this->rootPageNum, newEntry, newInternal);
}

  /**
   * A helper method that inserts a data entry into the index
   *
   * @param currPage         Current page we are on
   * @param currPageId       pageId of current page
   * @param isLeaf           True if current page is leaf
   * @param newEntry         Data entry of interest
   * @param newInternal      Internal used for pushing up key
  */
void BTreeIndex::insert(Page *currPage, PageId currPageId, bool isLeaf, const RIDKeyPair<int> newEntry, PageKeyPair<int> *&newInternal) {
  if (isLeaf) {
    // Get current page
    LeafNodeInt *leaf = reinterpret_cast<LeafNodeInt *>(currPage);

    if (!leaf->ridArray[this->leafOccupancy - 1].page_number) {
      insertLeaf(leaf, newEntry);  // Node is not full, so insert leaf
      this->bufMgr->unPinPage(this->file, currPageId, true);
    } else {
      splitLeaf(leaf, currPageId, newInternal, newEntry);
    }
  } else {
    NonLeafNodeInt *currNode = reinterpret_cast<NonLeafNodeInt *>(currPage);

    Page *nextPage;
    PageId nextNodeId;

    // Find next node one level down
    findNextInternal(currNode, nextNodeId, newEntry.key);

    // Get next page
    this->bufMgr->readPage(this->file, nextNodeId, nextPage);

    insert(nextPage, nextNodeId, currNode->level, newEntry, newInternal);

    if (!newInternal) {
      this->bufMgr->unPinPage(this->file, currPageId, false);  // Parent node did not need to be split
    } else {
      if (!currNode->pageNoArray[this->nodeOccupancy]) {
        insertInternal(currNode, newInternal);  // Internal not full so insert
        newInternal = nullptr;
        this->bufMgr->unPinPage(this->file, currPageId, true);
      } else {
        splitInternal(currNode, currPageId, newInternal);
      }
    }
  }
}

/**
  * A helper method that finds next node to traverse to down the tree
  *
  * @param internal Current node we are on
  * @param pageId   Return value for the next level page ID
  * @param key      Key being compared
 */
void BTreeIndex::findNextInternal(NonLeafNodeInt *internal, PageId &pageId, int key) {
  for (int i = this->nodeOccupancy; i > 0; i--) {
    if (internal->pageNoArray[i] != 0 && internal->keyArray[i - 1] < key) {
      pageId = internal->pageNoArray[i];
      return;
    }
  }
  pageId = internal->pageNoArray[0];
}

/**
 * A helper mehtod that is called when the root node needs to be split
 *
 * @param firstPage   pageId of first page in root
 * @param newInternal Internal for split process
*/
void BTreeIndex::splitRoot(PageId firstPage, PageKeyPair<int> *newInternal) {
  // Create a new root
  PageId newRootPageNum;
  Page *newRoot;
  this->bufMgr->allocPage(this->file, newRootPageNum, newRoot);
  NonLeafNodeInt *newRootPage = reinterpret_cast<NonLeafNodeInt *>(newRoot);

  int level;
  if (initialRootPageId == this->rootPageNum) {
    level = 1;
  } else {
    level = 0;
  }

  // update metadata of the root page
  newRootPage->level = level;
  newRootPage->keyArray[0] = newInternal->key;
  newRootPage->pageNoArray[0] = firstPage;
  newRootPage->pageNoArray[1] = newInternal->pageNo;

  Page *meta;
  this->bufMgr->readPage(this->file, this->headerPageNum, meta);
  IndexMetaInfo *metaPage = (IndexMetaInfo *)meta;
  metaPage->rootPageNo = newRootPageNum;
  this->rootPageNum = newRootPageNum;

  // unpin pages that are no longer needed
  this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
  this->bufMgr->unPinPage(this->file, newRootPageNum, true);
}

/**
 * A helper method that splits a leaf and copys middle key up the tree.
 *
 * @param leaf        Leaf of interest
 * @param leafPageId  Page Id of leaf of interest
 * @param newInternal New internal node if parent node is full
 * @param newEntry    The new entry of interest
*/
void BTreeIndex::splitLeaf(LeafNodeInt *leaf, PageId leafPageId, PageKeyPair<int> *&newInternal, RIDKeyPair<int> newEntry) {
  // Create new leaf
  PageId newPageId;
  Page *newPage;
  this->bufMgr->allocPage(this->file, newPageId, newPage);
  LeafNodeInt *newLeaf = reinterpret_cast<LeafNodeInt *>(newPage);

  int mid;
  if (this->leafOccupancy % 2) {
    mid = this->leafOccupancy / 2;
  } else {
    mid = this->leafOccupancy / 2 + 1;
  }

  // Copy half the page to newLeaf
  for (int i = mid; i < this->leafOccupancy; i++) {
    newLeaf->keyArray[i - mid] = leaf->keyArray[i];
    newLeaf->ridArray[i - mid] = leaf->ridArray[i];
    leaf->keyArray[i] = 0;
    leaf->ridArray[i].page_number = 0;
  }

  // Insert newEntry into appropriate leaf
  if (newEntry.key > leaf->keyArray[mid - 1]) {
    insertLeaf(newLeaf, newEntry);
  } else {
    insertLeaf(leaf, newEntry);
  }

  // Update sibling pointers
  newLeaf->rightSibPageNo = leaf->rightSibPageNo;
  leaf->rightSibPageNo = newPageId;

  // Copy up the smallest key from new leaf to parent
  newInternal = new PageKeyPair<int>();
  PageKeyPair<int> newKeyPair;
  newKeyPair.set(newPageId, newLeaf->keyArray[0]);
  newInternal = &newKeyPair;

  if (leafPageId == this->rootPageNum) splitRoot(leafPageId, newInternal); // Leaf is the root

  this->bufMgr->unPinPage(this->file, leafPageId, true);
  this->bufMgr->unPinPage(this->file, newPageId, true);
}

/**
  * A helper method that inserts a data entry into a leaf
  * @param leaf     Leaf of interest
  * @param newEntry Entry of interest
  */
void BTreeIndex::insertLeaf(LeafNodeInt *leaf, RIDKeyPair<int> newEntry) {
  if (leaf->ridArray[0].page_number != 0) {  // Leaf is not empty
    for (int i = this->leafOccupancy - 1; i >= 0; i--) {
      if (leaf->ridArray[i].page_number != 0) {
        if (leaf->keyArray[i] > newEntry.key) {
          // Move the existing entry
          leaf->keyArray[i + 1] = leaf->keyArray[i];
          leaf->ridArray[i + 1] = leaf->ridArray[i];
        } else {
          // Insert the new entry
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

/**
  * A helper method that inserts splits an internal node and pushes the middle key upwards. Can recursively
  * propagate up the tree
  *
  * @param oldNode         The internal node that's being split
  * @param oldPageId       The page id of the internal node that's being split
  * @param newInternal     Node with entry that will get pushed up
 */
void BTreeIndex::splitInternal(NonLeafNodeInt *oldNode, PageId oldPageId, PageKeyPair<int> *&newInternal) {
  // Allocate a new internal node
  PageId newPageId;
  Page *newPage;

  this->bufMgr->allocPage(this->file, newPageId, newPage);
  NonLeafNodeInt *newNode = reinterpret_cast<NonLeafNodeInt *>(newPage);

  int mid = this->nodeOccupancy / 2;
  int pushupIndex = mid;
  PageKeyPair<int> pushupEntry;

  if (!(this->nodeOccupancy % 2)) {
    // There are an even number of keys
    if (newInternal->key < oldNode->keyArray[mid]) {
      pushupIndex = mid - 1;
    } else {
      pushupIndex = mid;
    }
  }

  pushupEntry.set(newPageId, oldNode->keyArray[pushupIndex]);
  mid = pushupIndex + 1;

  // Move half of the entries to the new node and keep the other half here
  for (int i = mid; i < this->nodeOccupancy; i++) {
    newNode->keyArray[i - mid] = oldNode->keyArray[i];
    newNode->pageNoArray[i - mid] = oldNode->pageNoArray[i + 1];
    oldNode->pageNoArray[i + 1] = (PageId)0;
    oldNode->keyArray[i + 1] = 0;
  }

  newNode->level = oldNode->level;
  oldNode->keyArray[pushupIndex] = 0;
  oldNode->pageNoArray[pushupIndex] = (PageId) 0;

  // Insert the child entry into the tree
  NonLeafNodeInt *child;
  if (newInternal->key < newNode->keyArray[0]) {
    child = oldNode;
  } else {
    child = newNode;
  }

  insertInternal(child, newInternal);
  newInternal = &pushupEntry;

  if (oldPageId == this->rootPageNum) splitRoot(oldPageId, newInternal); // currNode is the root

  this->bufMgr->unPinPage(this->file, oldPageId, true);
  this->bufMgr->unPinPage(this->file, newPageId, true);
}

/**
 * A helper method that inserts a data entry into an internal node
 *
 * @param internal The internal node of interest
 * @param entry    The entry of interest
 *
 */
void BTreeIndex::insertInternal(NonLeafNodeInt *internal, PageKeyPair<int> *newEntry) {
  for (int i = this->nodeOccupancy; i >= 0; i--) {
    if (!internal->pageNoArray[i]) {
      continue; // There is no page here
    } else {
      if (internal->keyArray[i - 1] > newEntry->key) {
        // move the pre-existing entry
        internal->keyArray[i] = internal->keyArray[i - 1];
        internal->pageNoArray[i + 1] = internal->pageNoArray[i];
      } else {
        // insert a new entry into the tree
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
 * Set up all the variables for scan. Start from root to find out the leaf page
 *that contains the first RecordID that satisfies the scan parameters. Keep that
 *page pinned in the buffer pool.
 * @param lowVal Low value of range, pointer to integer / double / char string
 * @param lowOp      Low operator (GT/GTE)
 * @param highVal    High value of range, pointer to integer / double / char
 *string
 * @param highOp High operator (LT/LTE)
 * @throws  BadOpcodesException If lowOp and highOp do not contain one of their
 *their expected values
 * @throws  BadScanrangeException If lowVal > highval
 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that
 *satisfies the scan criteria.
 **/
void BTreeIndex::startScan(void *lowValParm, const Operator lowOpParm,
                           void *highValParm, const Operator highOpParm) {
  // Get Int values range for scan
  this->lowValInt = *((int *)lowValParm);
  this->highValInt = *((int *)highValParm);

  if (!((lowOpParm == GT || lowOpParm == GTE) && (highOpParm == LT || highOpParm == LTE))) {
    // Check that the parameters are valid
    throw BadOpcodesException();
  } else if (this->lowValInt > this->highValInt) {
    // Check that the ranges are valid
    throw BadScanrangeException();
  }

  // If the scan is already started, end it and start a new one
  if (this->scanExecuting) this->endScan();

  this->scanExecuting = true;
  this->lowOp = lowOpParm;
  this->highOp = highOpParm;
  this->currentPageNum = this->rootPageNum;

  // Read root page into the buffer pool
  this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);

  // The initialRootPageId is not the root
  if (this->initialRootPageId != this->rootPageNum) {
    NonLeafNodeInt *currNode = reinterpret_cast<NonLeafNodeInt *>(this->currentPageData);

    bool leafFound = false;

    while (!leafFound) {
      currNode = reinterpret_cast<NonLeafNodeInt *>(this->currentPageData);

      // if this is the level above the leaf, end while loop
      if (currNode->level) leafFound = true;
      PageId nextNode;

      // find the next node that is not a leaf
      {
        int i = this->nodeOccupancy;
        while (i >= 0 && (!currNode->pageNoArray[i])) {
          i--;
        }

        while (i > 0 && (currNode->keyArray[i - 1] >= lowValInt)) {
          i--;
        }
        nextNode = currNode->pageNoArray[i];
      }

      // Unpin the current page
      this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
      this->currentPageNum = nextNode;  // current page is not a leaf

      // read the page in
      this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
    }
  }

  // Now that the current Node is the leaf node, try to find the smallest record that satisfies the operands
  bool looking = true;
  while (looking) {
    LeafNodeInt *currLeaf = reinterpret_cast<LeafNodeInt *>(this->currentPageData);

    // If the page is null, then the key kould not be found
    if (!currLeaf->ridArray[0].page_number) {
      this->bufMgr->unPinPage(this->file, this->currentPageNum, false);  // make sure page is unpinned
      throw NoSuchKeyFoundException();
    }

    bool noVal = false;
    // Search from left leaf page to right leaf page for the first value
    for (int i = 0; i < leafOccupancy && !noVal; i++) {
      int key = currLeaf->keyArray[i];

      // Check if the next one in the key is not inserted
      if (i < this->leafOccupancy - 1 && !currLeaf->ridArray[i + 1].page_number) noVal = true;

      // check that the current key is valid
      bool validKey;
      if (lowOp == GTE && highOp == LTE) {
        validKey = (key <= this->highValInt && key >= this->lowValInt);
      } else if (lowOp == GT && highOp == LTE) {
        validKey = (key <= this->highValInt && key > this->lowValInt);
      } else if (lowOp == GTE && highOp == LT) {
        validKey = (key < this->highValInt && key >= this->lowValInt);
      } else {
        validKey = (key < this->highValInt && key > this->lowValInt);
      }

      if (validKey) {
        // use this valid key
        this->nextEntry = i;
        this->scanExecuting = true;
        looking = false;
        break;
      } else if ((this->highOp == LT && key >= this->highValInt) ||
                 (this->highOp == LTE && key > this->highValInt)) {
        // Key is invalid and the situation is unsalvagable
        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
        throw NoSuchKeyFoundException();
      }

      if (i == this->leafOccupancy - 1 || noVal) {
        // No matching key was found in this leaf so go to the next one
        this->bufMgr->unPinPage(this->file, this->currentPageNum, false);

        // no next leaf so no such page was found
        if (!currLeaf->rightSibPageNo) {
          throw NoSuchKeyFoundException();
        }

        this->currentPageNum = currLeaf->rightSibPageNo;
        this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
      }
    }
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

/**
 * Fetch the record id of the next index entry that matches the scan.
 * Return the next record from current page being scanned. If current page has
 * been scanned to its entirety, move on to the right sibling of current page,
 * if any exists, to start scanning that page. Make sure to unpin any pages
 * that are no longer required.
 * @param outRid RecordId of next record found that satisfies the scan
 * criteria returned in this
 * @throws ScanNotInitializedException If no scan has been initialized.
 * @throws IndexScanCompletedException If no more records, satisfying the scan
 * criteria, are left to be scanned.
 **/
void BTreeIndex::scanNext(RecordId &outRid) {
  // If startScan has not been called, then we don't know what we are scanning
  // for so throw error
  if (!scanExecuting) throw ScanNotInitializedException();

  // Look at current page as a node
  LeafNodeInt *node = reinterpret_cast<LeafNodeInt *>(this->currentPageData);

  if (!node->ridArray[this->nextEntry].page_number || this->nextEntry == this->leafOccupancy) {
    // Unpin page and read it
    this->bufMgr->unPinPage(this->file, this->currentPageNum, false);

    // Check whether there is a next leaf node
    if (!node->rightSibPageNo) {
      throw IndexScanCompletedException(); // No next leaf node
    }

    this->currentPageNum = node->rightSibPageNo;
    this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
    node = reinterpret_cast<LeafNodeInt *>(this->currentPageData);

    this->nextEntry = 0;
  }

  // Check if rid has a good/valid key
  int key = node->keyArray[this->nextEntry];

  bool validKey;
  if (lowOp == GTE && highOp == LTE) {
    validKey = key <= highValInt && key >= lowValInt;
  } else if (lowOp == GT && highOp == LTE) {
    validKey = key <= highValInt && key > lowValInt;
  } else if (lowOp == GTE && highOp == LT) {
    validKey = key < highValInt && key >= lowValInt;
  } else {
    validKey = key < highValInt && key > lowValInt;
  }

  if (validKey) {
    outRid = node->ridArray[this->nextEntry];
    this->nextEntry++;
  } else {
    // If the current page has been scanned to its entirety, then the scan is complete
    throw IndexScanCompletedException();
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------

/**
 * Terminate the current scan. Unpin any pinned pages. Reset scan specific
 *variables.
 * @throws ScanNotInitializedException If no scan has been initialized.
 **/
void BTreeIndex::endScan() {
  if (!this->scanExecuting) throw ScanNotInitializedException();

  try {
    bufMgr->unPinPage(this->file, this->currentPageNum, false);
  } catch (PageNotPinnedException &e) {
  }

  // Deinit necessary fields
  this->nextEntry = -1;
  this->currentPageData = nullptr;
  this->scanExecuting = false;
  this->currentPageNum = static_cast<PageId>(-1);
}

}  // namespace badgerdb
