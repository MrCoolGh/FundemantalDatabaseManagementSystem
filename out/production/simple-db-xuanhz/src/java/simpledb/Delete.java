package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    OpIterator child;
    TransactionId t;
    boolean open;
    boolean fetchNextCalled = false;
    int deleteCount = 0;


    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.child = child;
        this.t = t;

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[] {"Delete Count"});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        open = true;
        child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        open = false;
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        this.fetchNextCalled = false;
        this.deleteCount = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!this.open && fetchNextCalled) {
            return null;
        } else {
            // some code goes here
            while (child.hasNext()) {
                int successDelete = 0;
                try {
                    Tuple nextTuple = child.next();
                    Database.getBufferPool().deleteTuple(t, nextTuple);
                    successDelete = 1;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                deleteCount += successDelete;
            }
            if (!this.fetchNextCalled) {
                this.fetchNextCalled = true;
                Tuple insertCountTup = new Tuple(this.getTupleDesc());
                insertCountTup.setField(0, new IntField(deleteCount));
                return insertCountTup;
            }
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
