package simpledb;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {


    // assume input is only two integer
    private static final long serialVersionUID = 1L;
    int gbfieldN = 0;
    Type gbfieldtype = null;
    int afield = 0;
    Op aggOp = null;
    HashMap<Field, Field> groupAggregate = new HashMap<>();
    //In case of AVG groupAgg2 stores count groupAgg stores AVG
    //count * AVG = total, total + new / count + 1 = new AVG
    HashMap<Field, Double> groupAggregate1 = new HashMap<>();
    HashMap<Field, Field> groupAggregate2 = new HashMap<>();
    TupleDesc aggDesc = null;



    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfieldN = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.aggOp = what;

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupByField = tup.getField(this.gbfieldN);
        Field aggregateField = tup.getField(this.afield);
        this.aggDesc = tup.getTupleDesc();
        if (!groupAggregate.containsKey(groupByField)){
            Field value = (this.aggOp == Op.COUNT) ? new IntField(1) : aggregateField;
            groupAggregate1.put(groupByField, ((IntField)value).getValue() * 1.0);
            groupAggregate.put(groupByField, value);
        } else {
            IntField agg = (IntField)groupAggregate.get(groupByField);
            IntField newAgg = (IntField) aggregateField;
            if (this.aggOp == Op.SUM) {
                Field newSum = new IntField(agg.getValue() + newAgg.getValue());
                groupAggregate.put(groupByField, newSum);
            } else if (this.aggOp == Op.MIN) {
                Field newMin = new IntField(Math.min(agg.getValue(), newAgg.getValue()));
                groupAggregate.put(groupByField, newMin);
            } else if (this.aggOp == Op.MAX) {
                Field newMax = new IntField(Math.max(agg.getValue(), newAgg.getValue()));
                groupAggregate.put(groupByField, newMax);
            } else if (this.aggOp == Op.COUNT) {
                Field newMax = new IntField(agg.getValue() + 1);
                groupAggregate.put(groupByField, newMax);
            } else if (this.aggOp == Op.AVG) {
                if (!groupAggregate2.containsKey(groupByField)) {
                    groupAggregate2.put(groupByField, new IntField(1));
                }
                IntField countAgg = (IntField) groupAggregate2.get(groupByField);
                IntField newCount = new IntField(countAgg.getValue() + 1);
                groupAggregate2.put(groupByField, newCount);
                double totalBefore = 0;
                if (groupAggregate1.containsKey(groupByField)) {
                    totalBefore = groupAggregate1.get(groupByField);
                }
                double total = totalBefore + newAgg.getValue();
                double average = (total) / newCount.getValue();
                this.groupAggregate1.put(groupByField, total);
                Field newAVG = new IntField((int)Math.floor(average));
                groupAggregate.put(groupByField, newAVG);
            }
        }


    }

    public Field[][] mapToArr(HashMap<Field, Field> map) {
        Field[][] arr = new Field[map.size()][2];
        Set<Entry<Field, Field>> entries = map.entrySet();
        Iterator<Entry<Field, Field>> entriesIterator = entries.iterator();

        int i = 0;
        while(entriesIterator.hasNext()){

            Entry<Field, Field> mapping = entriesIterator.next();

            arr[i][0] = mapping.getKey();
            arr[i][1] = mapping.getValue();

            i++;
        }
        return arr;
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        OpIterator groupAggIter = new OpIterator() {
            int iter = 0;
            Field[][] fieldArr = mapToArr(groupAggregate);
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
                        Tuple aggTuple = new Tuple(aggDesc);
                        aggTuple.setField(0, fieldArr[iter][0]);
                        aggTuple.setField(1, fieldArr[iter][1]);
                        iter++;
                        return aggTuple;
                    } else {
                        throw new NoSuchElementException("there is no tuple left in aggregation");
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
        return groupAggIter;
    }

}
