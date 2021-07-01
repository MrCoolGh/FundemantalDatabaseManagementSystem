package simpledb;

import java.util.Iterator;

/**
 * HeapFile Iterator for Iterating HeapFile
 */
public class HeapFileIterator extends AbstractDbFileIterator {

    public HeapFile heapFile;
    public TransactionId tid;
    public int currentPageNumber;
    public Iterator<Tuple> currentPageTupleIterator;

    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
        this.currentPageNumber = 0;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (this.currentPageTupleIterator == null) return null;

        if (this.currentPageTupleIterator.hasNext()) {
            return this.currentPageTupleIterator.next();
        } else {
            while (this.currentPageNumber < this.heapFile.numPages()) {
                this.currentPageTupleIterator = this.getNextPage().iterator();
                if (this.currentPageTupleIterator.hasNext()) {
                    return this.currentPageTupleIterator.next();
                }
            }
        }

        return null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.currentPageTupleIterator = getNextPage().iterator();
    }

    public void close() {
        super.close();
        this.currentPageNumber = 0;
        this.currentPageTupleIterator = null;
//        Database.getBufferPool().lockManager.transactionReleaseAll(tid);
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    private HeapPage getNextPage() throws DbException, TransactionAbortedException {
        HeapPageId pageId = new HeapPageId(this.heapFile.getId(), this.currentPageNumber);
        this.currentPageNumber++;
        return (HeapPage) Database.getBufferPool().getPage(this.tid, pageId, Permissions.READ_ONLY);
    }
}