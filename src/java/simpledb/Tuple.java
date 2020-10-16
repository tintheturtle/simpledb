package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    // Holds the different Fields of the Tuple
    private TupleDesc schema;
    private ArrayList<Field> tupleFields;
    private RecordId rid;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) throws NullPointerException {
        // some code goes here

        if (td == null) {
            throw new NullPointerException();
        }
        if (td.getSize() < 1) {
            throw new NullPointerException();
        }

        // Set the schema
        this.schema = td;

        // Initialize the array holding the tuple information
        this.tupleFields = new ArrayList<Field>();

        // Iterate through the number of fields to set the tuple to null values
        for (int index = 0; index < td.numFields(); index++) {
            this.tupleFields.add(null);
        }

    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here

        this.tupleFields.set(i, f);

    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return this.tupleFields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */

    public String toString() {
        // some code goes here

        String tupleString = "";

        for(int i = 0; i < this.tupleFields.size(); i++) {
            tupleString += this.tupleFields.get(i).toString() + "\t";
        }

        return tupleString;

    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return this.tupleFields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here

        for (int i = 0; i < this.tupleFields.size(); i++) {
            this.tupleFields.set(i, null);
        }
    }
}
