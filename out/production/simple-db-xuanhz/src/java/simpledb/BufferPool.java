package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    public ConcurrentHashMap<PageId, Page> bufferPool;
    public ConcurrentHashMap<TransactionId, Set<PageId>> transactionPageMap;
    public ConcurrentHashMap<TransactionId, Integer> transactionStatusMap;

    public int numPages = 0;
    TransactionId tid;

    boolean isFlushing = false;

    //If any page is dirty
    boolean isDirty = false;
    LockManager lockManager;

    private final long SLEEP_INTERVAL = 2;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.bufferPool = new ConcurrentHashMap<>();
        this.lockManager = new LockManager();
        this.transactionPageMap = new ConcurrentHashMap<>();
        this.transactionStatusMap = new ConcurrentHashMap<>();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        boolean ifGrantLock = lockManager.lock(pid, tid, perm);
        while (!ifGrantLock) {
            if (lockManager.isDeadLockOccurred(tid, pid)) {
                throw new TransactionAbortedException();
            }
            try {
                Thread.sleep(this.SLEEP_INTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ifGrantLock = lockManager.lock(pid, tid, perm);
        }

        Set<PageId> transPages = transactionPageMap.getOrDefault(tid, new HashSet<>());
        transPages.add(pid);
        transactionPageMap.put(tid, transPages);
        // updating
        transactionStatusMap.put(tid, 3);

        if (this.bufferPool.containsKey(pid)) {
            Page page = this.bufferPool.get(pid);
            ((HeapPage)this.bufferPool.get(pid)).updataLastAccessTime();
            return page;
        } else {
            if (this.bufferPool.size() >= this.numPages) {
                evictPage();
            }
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page result = file.readPage(pid);
            if (!isFlushing) {
                bufferPool.put(pid, result);
            }
            return result;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.release(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.isTransactionLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        Set<PageId> transPageIdSet = new HashSet<>();
        if (transactionPageMap.containsKey(tid)) {
            transPageIdSet = transactionPageMap.get(tid);
        }
        if (commit) {
            // No Force
//            flushPages(tid);
            // commit
            for (Page page: bufferPool.values()) {
                TransactionId dirtier = page.isDirty();
                if (dirtier != null) {
                    Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
                    Database.getLogFile().force();
                }
                page.setBeforeImage();
            }
            transactionStatusMap.put(tid, 2);
        } else {
            for (PageId pid: transPageIdSet) {
                if (this.bufferPool.get(pid) != null) {
                    Page page = this.bufferPool.get(pid);
                    bufferPool.get(pid);
                    Page oldPageVersion = page.getBeforeImage();
                    bufferPool.remove(page.getId());
                    bufferPool.put(oldPageVersion.getId(), oldPageVersion);
                }
            }
            // abort
            transactionStatusMap.put(tid, 1);
        }
        lockManager.transactionReleaseAll(tid);
        transactionPageMap.remove(tid);

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        this.isDirty = true;
        HeapFile file = (HeapFile)Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = file.insertTuple(tid, t);
        for (int i = 0; i < dirtyPages.size(); i++) {
            dirtyPages.get(i).markDirty(true, tid);
            this.bufferPool.put(dirtyPages.get(i).getId(), dirtyPages.get(i));
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        isDirty = true;
        int tableId = t.getRecordId().getPageId().getTableId();
        HeapFile file = (HeapFile)Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPage = file.deleteTuple(tid, t);
        dirtyPage.get(0).markDirty(true, tid);
        this.bufferPool.put(dirtyPage.get(0).getId(), dirtyPage.get(0));
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Page page: bufferPool.values()) {
            flushPage(page.getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        isFlushing = true;
        HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
        try {
            HeapPage page = (HeapPage) this.bufferPool.get(pid);
            TransactionId dirtier = page.isDirty();
            // STEAL
            if (dirtier != null) {
                if (this.transactionPageMap.containsKey(dirtier)) {
//                    Database.getLogFile().print();
                    Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
                    Database.getLogFile().force();
//                    System.out.println("---------after Update Log----------");
//                    Database.getLogFile().print();
                }
//                else {
//                    if (transactionStatusMap.get(dirtier) == 1) {
//                        Database.getLogFile().logAbort(dirtier);
//                    }
//                    if (transactionStatusMap.get(dirtier) == 2) {
//                        Database.getLogFile().logAbort(dirtier);
//                    }
//                }

            }
            file.writePage(page);
            page.setBeforeImage();
            page.markDirty(false, null);
        } catch (Exception e) {
            throw new IOException();
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Page page: this.bufferPool.values()) {
            flushPage(page.getId());
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Page eldestPage = findLeastUsedPage();
        if (eldestPage == null) {
            throw new DbException("All pages are dirty can't evict page in buffer pool");
        } else {
            try {
                flushPage(eldestPage.getId());
                this.bufferPool.remove(eldestPage.getId());
            } catch (IOException e) {
               throw new DbException("Flush page to disk fail, page can't be evicted from buffer pool");
            }
        }
    }

    HeapPage findLeastUsedPage() {
        HeapPage leastUsed = null;
        for (Page page: this.bufferPool.values()) {
            if (page.isDirty() == null) {
                HeapPage heappage = (HeapPage) page;
                if (leastUsed == null || heappage.getLastAccessTime() < leastUsed.getLastAccessTime()) {
                    leastUsed = heappage;
                }
            }
        }
        return leastUsed;
    }

}