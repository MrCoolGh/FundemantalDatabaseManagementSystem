package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int gbfield;
    Type gbfieldtype;
    int afield;
    Op aggOp;
    HashMap<Field, Field> groupAggregate = new HashMap<>();
    TupleDesc aggDesc = null;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        } else {
            // some code goes here
            this.gbfield = gbfield;
            this.gbfieldtype = gbfieldtype;
            this.afield = afield;
            this.aggOp = what;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupByField = tup.getField(this.gbfield);
        this.aggDesc = tup.getTupleDesc();
        if (!groupAggregate.containsKey(groupByField)) {
            Field value = new IntField(1);
            groupAggregate.put(groupByField, value);
        } else {
            IntField agg = (IntField) groupAggregate.get(groupByField);
            Field newCount = new IntField(agg.getValue() + 1);
            groupAggregate.put(groupByField, newCount);
        }
    }

    public Field[][] mapToArr(HashMap<Field, Field> map) {
        Field[][] arr = new Field[map.size()][2];
        Set<Map.Entry<Field, Field>> entries = map.entrySet();
        Iterator<Map.Entry<Field, Field>> entriesIterator = entries.iterator();

        int i = 0;
        while(entriesIterator.hasNext()){

            Map.Entry<Field, Field> mapping = entriesIterator.next();

            arr[i][0] = mapping.getKey();
            arr[i][1] = mapping.getValue();

            i++;
        }
        return arr;
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            int iter = 0;
            final Field[][] fieldArr = mapToArr(groupAggregate);
            boolean open = false;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                open = true;
                iter = 0;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (open) {
                    return iter < fieldArr.length;
                } else {
                    return false;
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (open) {
                    if (iter < fieldArr.length){
                        Type[] newTypeArr = new Type[]{gbfieldtype, Type.INT_TYPE};
                        TupleDesc newTupleDesc = new TupleDesc(newTypeArr);
                        Tuple aggTuple = new Tuple(newTupleDesc);
                        aggTuple.setField(0, fieldArr[iter][0]);
                        aggTuple.setField(1, fieldArr[iter][1]);
                        iter++;
                        return aggTuple;
                    } else {
                        throw new NoSuchElementException();
                    }
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                iter = 0;
            }

            @Override
            public TupleDesc getTupleDesc() {
                return aggDesc;
            }

            @Override
            public void close() {
                iter = 0;
                open = false;
            }
        };
    }

}
