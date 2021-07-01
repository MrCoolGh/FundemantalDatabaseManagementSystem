package simpledb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File backingFile;
    TupleDesc td;

    // file stream to read in disk
    RandomAccessFile readFile;
    RandomAccessFile writeFile;
    int numPages = 0;

//    HeapFileIterator fileIter;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.backingFile = f;
        this.td = td;
        this.numPages = (int) Math.ceil((double)this.backingFile.length() / BufferPool.getPageSize());


//        // some code goes here
//        this.fileIter = new HeapFileIterator(this.numPages(), this.getId());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.backingFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.backingFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid.getPageNumber() > this.numPages()) {
            throw new NoSuchElementException();
        }
        try {
            if (pid.getPageNumber() == this.numPages()) {
                this.numPages++;
                return new HeapPage((HeapPageId) pid, HeapPage.createEmptyPageData());
            } else {
                HeapFile correctFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
                int pageNumber = pid.getPageNumber();
                readFile = new RandomAccessFile(correctFile.getFile(), "r");
                byte[] dest = new byte[BufferPool.getPageSize()];
                readFile.seek(pageNumber * BufferPool.getPageSize());
                int readBytes = readFile.read(dest);
                if (readBytes < 0) {
                    throw new NoSuchElementException();
                }
                readFile.close();
                return new HeapPage((HeapPageId) pid, dest);
            }
        } catch (NoSuchElementException nee) {
            throw new IllegalArgumentException();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try {
            PageId pid = page.getId();
            HeapFile correctFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
            int pageNumber = pid.getPageNumber();
            writeFile = new RandomAccessFile(correctFile.getFile(), "rw");
            writeFile.seek(pageNumber * BufferPool.getPageSize());
            writeFile.write(page.getPageData());
            writeFile.close();
        } catch (NoSuchElementException nee) {
            throw new IllegalArgumentException();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil((double)this.backingFile.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        BufferPool thisBufferPool = Database.getBufferPool();
        int pageId = 0;
        int tableId = this.getId();
        PageId newPageId = new HeapPageId(tableId,pageId);
        ArrayList<Page> newPageList = new ArrayList<>();
        while (true) {
            newPageId = new HeapPageId(tableId, pageId);
            HeapPage filePage = (HeapPage) thisBufferPool.getPage(tid, newPageId, Permissions.READ_ONLY);
            if (filePage.getNumEmptySlots() > 0) {
                RecordId newRid = new RecordId(newPageId, tableId);
                t.setRecordId(newRid);
                filePage = (HeapPage) thisBufferPool.getPage(tid, newPageId, Permissions.READ_WRITE);
                filePage.insertTuple(t);
                newPageList.add(filePage);
                return newPageList;
            } else {
                Database.getBufferPool().releasePage(tid, newPageId);
                pageId++;
                if (pageId >= this.numPages()) {
                    numPages++;
                    HeapPage newHeapPage = new HeapPage(new HeapPageId(tableId, pageId), HeapPage.createEmptyPageData());
                    writePage(newHeapPage);
                    HeapPage pageOnBuffer = null;
                    try {
                        pageOnBuffer = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pageId), Permissions.READ_WRITE);
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                    t.setRecordId(new RecordId(newHeapPage.getId(), 0));
                    if (pageOnBuffer != null) {
                        pageOnBuffer.insertTuple(t);
                    } else {
                        throw new DbException("created a new page to insert and fetch it from buffer but failed ");
                    }
                    newPageList.add(pageOnBuffer);
                    return newPageList;
                }
            }
        }

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        BufferPool thisBufferPool = Database.getBufferPool();
        HeapPage targetPage = (HeapPage)thisBufferPool.getPage(tid, t.getRecordId().getPageId(),Permissions.READ_WRITE);
        targetPage.deleteTuple(t);
        ArrayList<Page> modifiedPages = new ArrayList<>();
        modifiedPages.add(targetPage);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }


}
