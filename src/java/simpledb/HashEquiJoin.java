package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;

    private Tuple left;
    private Tuple right;

    private TupleDesc td;

    private HashMap<Object, ArrayList<Tuple>> map = new HashMap<Object, ArrayList<Tuple>>();


    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;

        this.right = null;
        this.left = null;

        this.td = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }
    
    public String getJoinField1Name()
    {
	    return this.child1.getTupleDesc().getFieldName(this.p.getField1());
    }

    public String getJoinField2Name()
    {
        return this.child2.getTupleDesc().getFieldName(this.p.getField2());
    }
    
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.child1.open();
        this.child2.open();
        super.open();
    }

    public void close() {
        // some code goes here
        this.child1.close();
        this.child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child1.rewind();
        this.child2.rewind();
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child1, this.child2 };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child1 = children[0];
        this.child2 = children[1];
    }
    
}
