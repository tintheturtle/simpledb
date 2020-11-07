package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private HashMap<Field, Integer> map;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException {
        // some code goes here

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        assert(this.what == Op.COUNT);

        this.map = new HashMap<Field, Integer>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        Field field;

        if (this.gbfield == Aggregator.NO_GROUPING) {
            field = null;
        } else {
            field = tup.getField(this.gbfield);
        }

        if (this.map.containsKey(field)) {

            int currentCount = this.map.get(field);
            int current = currentCount + 1;
            this.map.put(field, current);
        } else {
            this.map.put(field, 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {

        ArrayList<Tuple> tuples = new ArrayList<Tuple>();

        String[] names;
        Type[] types;
        if (this.gbfield == Aggregator.NO_GROUPING)
        {
            names = new String[] {"aggregateValue"};
            types = new Type[] {Type.INT_TYPE};
        }
        else
        {
            names = new String[] {"groupValue", "aggregateValue"};
            types = new Type[] {this.gbfieldtype, Type.INT_TYPE};
        }
        TupleDesc td =  new TupleDesc(types, names);

        Tuple next;

        for (Field field : this.map.keySet()) {
            int value = this.map.get(field);

            next = new Tuple(td);
            if (this.gbfield == Aggregator.NO_GROUPING) {
                next.setField(0, new IntField(value));
            } else {
                next.setField(0, field);
                next.setField(1, new IntField(value));
            }
            tuples.add(next);
        }
        return new TupleIterator(td, tuples);
    }

}
