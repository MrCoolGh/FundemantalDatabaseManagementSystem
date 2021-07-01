package simpledb;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    /* a share lock on a transaction(key) correspond to multiple page (value).*/
    public Map<TransactionId, HashSet<PageId>> sharedLockPages;
    /* an exclusive lock on a transaction(key) correspond to one page(value).
    a transaction could write to many pages.*/
    public Map<TransactionId, HashSet<PageId>> exclusiveLockPages;
    /* for a page (key) there could be several read locks(value)*/
    public Map<PageId, HashSet<TransactionId>> pageReadLockMap;
    /* for a page (key) there could only be one write lock(value) */
    public Map<PageId, TransactionId> pageWriteLockMap;
    /* check if the transaction is waiting */
    public Map<TransactionId, PageId> waitingInfo;

    public LockManager() {
        sharedLockPages = new ConcurrentHashMap<>();
        exclusiveLockPages = new ConcurrentHashMap<>();
        pageReadLockMap = new ConcurrentHashMap<>();
        pageWriteLockMap = new ConcurrentHashMap<>();
        waitingInfo = new ConcurrentHashMap<>();
    }

    /**
     * Check if the transaction is locked. Check all the locked read write transaction for the page
     * to see if given transaction is included in the locked transaction.
     * @param tid given transaction
     * @param pid page need to perform operatior on
     * @return true if transaction is locked otherwise false
     */
    public boolean isTransactionLock(TransactionId tid, PageId pid) {
        HashSet<TransactionId> readLockedTransactions = pageReadLockMap.get(pid);
        TransactionId writeLockedTransaction = pageWriteLockMap.get(pid);
        if (readLockedTransactions.contains(tid) || writeLockedTransaction == tid) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Release given transaction 's lock on given page
     * @param pid pageId of page
     * @param tid TransactionId of the transaction
     */
    public synchronized void release(TransactionId tid, PageId pid) {
        HashSet<PageId> sharedPageSet = sharedLockPages.get(tid);
        HashSet<TransactionId> readTransactionSet = pageReadLockMap.get(pid);
        if (readTransactionSet!= null) {
            readTransactionSet.remove(tid);
            pageReadLockMap.put(pid, readTransactionSet);
        }
        pageWriteLockMap.remove(pid);
        if (sharedPageSet != null) {
            sharedPageSet.remove(pid);
            sharedLockPages.put(tid, sharedPageSet);
        }
        if (exclusiveLockPages != null) {
            exclusiveLockPages.remove(tid);
        }
    }

    /**
     * Release all the locks of a given transaction
     * @param tid transaction id for the transaction to release
     */
    public synchronized void transactionReleaseAll (TransactionId tid) {
        for (PageId pageId : pageReadLockMap.keySet()) {
            HashSet<TransactionId> tidSet = pageReadLockMap.get(pageId);
            if (tidSet != null) {
                tidSet.remove(tid);
                pageReadLockMap.put(pageId, tidSet);

            }
        }
        for (PageId pageId : pageWriteLockMap.keySet()) {
            if (pageWriteLockMap.get(pageId) != null && pageWriteLockMap.get(pageId) == tid) {
                pageWriteLockMap.remove(pageId);
            }
        }
        sharedLockPages.remove(tid);
        exclusiveLockPages.remove(tid);
    }


    /**
     * Lock the the transaction and page based on permission. Record the lock in the four maps if possible to lock.
     * If READ_ONLY permission put into the pageReadLockMap and sharedLockPages
     * If READ_WRITE permssion put into the pageWriteLockMap and exclusiveLockPage
     * @param pid page id for page need to be locked
     * @param tid transaction id for transaction need to lock
     * @param perm permission type whether is READ_ONLY or READ_WRITE
     * @return true if lock successful false if fail to lock
     */
    public synchronized boolean lock(PageId pid, TransactionId tid, Permissions perm){
        if (perm.equals(Permissions.READ_ONLY)) {
            HashSet<TransactionId> readTidSet = pageReadLockMap.get(pid);
            TransactionId writetid = pageWriteLockMap.get(pid);
            /* If other transaction is not writing the page or we are writing on the same transaction successfully lock*/
            if (writetid == null || writetid.equals(tid)) {
                if (readTidSet == null) {
                    readTidSet = new HashSet<>();
                }
                readTidSet.add(tid);
                pageReadLockMap.put(pid, readTidSet);
                HashSet<PageId> pageIdSet = sharedLockPages.get(tid);
                if (pageIdSet == null) {
                    pageIdSet = new HashSet<>();
                }
                pageIdSet.add(pid);
                sharedLockPages.put(tid, pageIdSet);
                waitingInfo.remove(tid);
                return true;
            } else {
                /* If other transaction is write to the page deny read lock */
                waitingInfo.put(tid, pid);
                return false;
            }
        } else {
            Set<TransactionId> tidSet = pageReadLockMap.get(pid);
            TransactionId writetid = pageWriteLockMap.get(pid);
            if (tidSet != null){
                /* if more than one transaction is reading the page deny lock*/
                /* if other transaction is reading the page deny lock */
                if (tidSet.size() > 1 || (tidSet.size() == 1 && !tidSet.contains(tid))) {
                    waitingInfo.put(tid, pid);
                    return false;
                }
            }
            /* If other transaction is writing the page deny lock */
            if (writetid != null && !writetid.equals(tid)){
                waitingInfo.put(tid, pid);
                return false;
            } else {
                pageWriteLockMap.put(pid, tid);
                HashSet<PageId> pageIdSet = exclusiveLockPages.get(tid);
                if (pageIdSet == null) {
                    pageIdSet = new HashSet<>();
                }
                pageIdSet.add(pid);
                exclusiveLockPages.put(tid, pageIdSet);
                waitingInfo.remove(tid);
                return true;
            }
        }

    }

    /**
     * Check for a given transaction, if it want to perform action on pid will that cause dead lock.
     * @param tid id of transaction
     * @param pid id of the page
     * @return true if dead lock occured and false if not
     */
    public synchronized boolean isDeadLockOccurred(TransactionId tid, PageId pid) {
        if (this.pageReadLockMap.get(pid) == null && this.pageWriteLockMap == null) {
            return false;
        } else {
            HashSet<TransactionId> allLockedTransOnPage = new HashSet<>(this.pageReadLockMap.getOrDefault(pid, new HashSet<>()));
            allLockedTransOnPage.add(this.pageWriteLockMap.get(pid));
            if (allLockedTransOnPage.isEmpty()) {
                return false;
            }
            HashSet<PageId> allPageTransHolds = new HashSet<>(this.sharedLockPages.getOrDefault(tid, new HashSet<>()));
            if (this.exclusiveLockPages.get(tid) != null) {
                allPageTransHolds.addAll(this.exclusiveLockPages.get(tid));
            }
            if (allPageTransHolds.isEmpty()) {
                return false;
            }
            // if other transaction that holds lock on the given page is
            // waiting for tid to open lock on their target page
            for (TransactionId lockedTrans: allLockedTransOnPage) {
                if (lockedTrans != null) {
                    if (!lockedTrans.equals(tid)) {
                        // lockedTrans: the other transaction that hold lock on given page
                        // the pages that this transaction hold lock on
                        // tid continue not consider this transaction itself
                        boolean isWaiting = isWaitingResources(lockedTrans, allPageTransHolds, tid);
                        if (isWaiting) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }

    /**
     * Check: Does the other transaction, which holds lock on the page this transaction wants, is waiting for this transaction
     * to release lock? The waiting relation could be directly or undirectly.
     * (bracket's means hold relation)
     * ThisTid --wait--> targetPage (the otherTid) --wait-->
     * @param theOtherTid the other transaction's tid
     * @param thisLockedPages all pages locked by target transaction
     * @param thisTid target transaction's tid
     * @return true if cycle waiting occured, false if not
     */
    public synchronized boolean isWaitingResources(TransactionId theOtherTid, HashSet<PageId> thisLockedPages, TransactionId thisTid) {
            PageId waitingPageId = waitingInfo.get(theOtherTid);
            if (waitingPageId == null) {
                return false;
            }

            // Directly waiting base case
            for (PageId pid: thisLockedPages) {
                if (pid.equals(waitingPageId)) {
                    return true;
                }
            }

            // Undirectly waiting T1 wait T2's P2, T2 wait T3's P3, so T1 is undirectly waiting for T3
            // theOtherTid --wait-> waitingPage (hold by waitingPageHolders) -- ??wait-> thisLockPages?
            HashSet<TransactionId> waitingPageHolders = new HashSet<>(this.pageReadLockMap.getOrDefault(waitingPageId, new HashSet<>()));
            if (this.pageWriteLockMap.get(waitingPageId) != null) {
                waitingPageHolders.add(this.pageWriteLockMap.get(waitingPageId));
            }
            for (TransactionId waitingPageHolder: waitingPageHolders) {
                if (!waitingPageHolder.equals(thisTid)) {
                    boolean isWaiting = isWaitingResources(waitingPageHolder, thisLockedPages, thisTid);
                    if (isWaiting) {
                        return true;
                    }
                }
            }
            return false;
    }




}
