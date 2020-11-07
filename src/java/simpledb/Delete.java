package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private TransactionId t;

    private boolean deletion;
    private TupleDesc deleteTD;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.t = t;
        this.child = child;

        String[] names = new String[] {"Deleted"};
        Type[] types = new Type[] {Type.INT_TYPE};
        this.deleteTD = new TupleDesc(types, names);

        this.deletion = false;
    }

    public TupleDesc getTupleDesc() {
        return this.deleteTD;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.deletion) {
            return null;
        }

        int count = 0;
        while (this.child.hasNext()) {
            Tuple next = this.child.next();
            try {
                Database.getBufferPool().deleteTuple(this.t, next);
            } catch (IOException e) {
                throw new DbException("Error inserting tuple.");
            }
            count++;
        }

        Tuple deleted = new Tuple(this.deleteTD);
        deleted.setField(0, new IntField(count));
        this.deletion = true;
        return deleted;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] {this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}
