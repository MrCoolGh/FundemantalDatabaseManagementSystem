package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    OpIterator childOp;
    int tableId;
    TransactionId t;
    boolean open;
    boolean fetchNextCalled = false;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.childOp = child;
        this.tableId = tableId;
        this.t = t;

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[] {"Insert Count"});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        open = true;
        childOp.open();
    }

    public void close() {
        // some code goes here
        super.close();
        open = false;
        childOp.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childOp.rewind();
        this.fetchNextCalled = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!this.open && fetchNextCalled) {
            return null;
        } else {
            // some code goes here
            int insertCount = 0;
            while (childOp.hasNext()) {
                int successInsert;
                try {
                    Database.getBufferPool().insertTuple(t, this.tableId, childOp.next());
                    successInsert = 1;
                } catch (Exception e) {
                    successInsert = 0;
                }
                insertCount += successInsert;
            }
            if (!this.fetchNextCalled) {
                this.fetchNextCalled = true;
                Tuple insertCountTup = new Tuple(this.getTupleDesc());
                insertCountTup.setField(0, new IntField(insertCount));
                return insertCountTup;
            }
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.childOp};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.childOp = children[0];
    }
}
