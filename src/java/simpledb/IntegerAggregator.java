package simpledb;

import java.util.HashMap;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldIndex;
    private Type gbFieldType;
    private int aFieldIndex;
    private Op opper;
    private HashMap<Field, Integer> aData;
    private HashMap<Field, Integer> count;

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
        gbFieldIndex = gbfield;
        gbFieldType = gbfieldtype;
        aFieldIndex = afield;
        opper = what;
        aData = new HashMap<Field, Integer>();
        count = new HashMap<Field, Integer>();
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
        Field tupGBField;
        if(gbFieldIndex == Aggregator.NO_GROUPING){
            tupGBField = null;
        }
        else{
            tupGBField = tup.getField(gbFieldIndex);
        }

        if(!aData.containsKey(tupGBField)){
            aData.put(tupGBField, startingData());
            count.put(tupGBField,0);
        }

        int currVal = aData.get(tupGBField);
        int newVal = currVal;
        int currCount = count.get(tupGBField);
        int tupVal = ((IntField) tup.getField(aFieldIndex)).getValue();

        switch(opper){
            case MAX:
                newVal = Math.max(tupVal, currVal);
                break;
            case MIN:
                newVal = Math.min(tupVal, currVal);
                break;
            case SUM:
            case AVG:
                count.put(tupGBField,currCount+1);
                newVal = tupVal + currVal;
                break;
            case COUNT:
                newVal = currVal + 1;
                break;
            default:
                break;
        }
        aData.put(tupGBField, newVal);
    }

    private int startingData(){
        switch(opper){
            case MAX: return Integer.MIN_VALUE;
            case MIN: return Integer.MAX_VALUE;
            case SUM: case AVG: case COUNT: return 0;
            default: return 0;
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> arrListTuples = new ArrayList<Tuple>();
        TupleDesc tupleDesc = createGBTupleDesc();

        Tuple tuple;
        for(Field group : aData.keySet()){
            int aggregateVal;
            if(opper == Op.AVG){
                aggregateVal = aData.get(group)/ count.get(group);
            }
            else{
                aggregateVal = aData.get(group);
            }
            tuple = new Tuple(tupleDesc);

            if(gbFieldIndex == Aggregator.NO_GROUPING){
                tuple.setField(0, new IntField(aggregateVal));
            }
            else{
                tuple.setField(0, group);
                tuple.setField(1, new IntField(aggregateVal));
            }
            arrListTuples.add(tuple);
        }
        return new TupleIterator(tupleDesc, arrListTuples);
    }

    private TupleDesc createGBTupleDesc(){
        Type[] arrTypes;
        String[] arrNames;

        if(gbFieldIndex == Aggregator.NO_GROUPING){
            arrTypes = new Type[] {Type.INT_TYPE};
            arrNames = new String[] {"aggregateVal"};
        }
        else{
            arrTypes = new Type[] {gbFieldType, Type.INT_TYPE};
            arrNames = new String[] {"groupVal", "aggregateVal"};
        }
        return new TupleDesc(arrTypes, arrNames);
    }
}