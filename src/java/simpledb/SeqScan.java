package simpledb;

import javax.xml.crypto.Data;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private int tableId;
    private String tableAlias;

    private DbFileIterator fileIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableId
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableId, String tableAlias) {

        this.tid = tid;
        this.tableId = tableId;
        this.tableAlias = tableAlias;
        this.fileIterator = Database.getCatalog().getDatabaseFile(this.tableId).iterator(this.tid);


    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        if (this.tableAlias == "null") {
            return "null";
        }
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {

        this.tableAlias = tableAlias;
        this.tableId = tableid;

    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here

        DbFile file = Database.getCatalog().getDatabaseFile(this.tableId);

        this.fileIterator = file.iterator(this.tid);

        this.fileIterator.open();


    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here

        TupleDesc tds = Database.getCatalog().getTupleDesc(this.tableId);

        Iterator<TupleDesc.TDItem> tdItems = tds.iterator();

        Type[] typeAr = new Type[tds.numFields()];
        String[] fieldNameAr= new String[tds.numFields()];

        int index = 0;

        while (tdItems.hasNext()) {
            TupleDesc.TDItem tdItem = tdItems.next();

            typeAr[index] = tdItem.fieldType;
            fieldNameAr[index] = this.tableAlias + '.' + tdItem.fieldName;

            index++;


        }

        return new TupleDesc(typeAr, fieldNameAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {

        return this.fileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return this.fileIterator.next();
    }

    public void close() {
        // some code goes here
        this.fileIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {

        this.fileIterator.rewind();
        // some code goes here
    }
}
