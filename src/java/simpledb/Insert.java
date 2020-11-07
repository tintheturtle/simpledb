package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator child;
    private int tableId;

    private boolean insertion;
    private TupleDesc insertTD;

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
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tableId = tableId;

        String[] names = new String[] {"Inserted"};
        Type[] types = new Type[] {Type.INT_TYPE};
        this.insertTD = new TupleDesc(types, names);

        this.insertion = false;
    }

    public TupleDesc getTupleDesc() {
        return this.insertTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        super.open();
    }

    public void close() {
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
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
        // some code goes here

        if (this.insertion) {
            return null;
        }

        int count = 0;
        while (this.child.hasNext()) {
            Tuple next = this.child.next();
            try {
                Database.getBufferPool().insertTuple(this.t, this.tableId, next);
            } catch (IOException e) {
                throw new DbException("Error inserting tuple.");
            }
            count++;
        }

        Tuple inserted = new Tuple(this.insertTD);
        inserted.setField(0, new IntField(count));
        this.insertion = true;
        return inserted;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }
}
