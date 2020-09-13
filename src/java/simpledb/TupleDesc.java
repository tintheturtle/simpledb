package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> TDItemList;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return this.TDItemList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this.TDItemList = new ArrayList<TDItem>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItem newTD = new TDItem(typeAr[i], fieldAr[i]);
            this.TDItemList.add(newTD);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here

        // Initialize a new array list to hold the tuples
        this.TDItemList = new ArrayList<TDItem>();

        // Iterate through the typeAr input
        for (int i = 0; i < typeAr.length; i++) {

            // Create a new TDItem with a type and null field
            TDItem newTD = new TDItem(typeAr[i], null);
            this.TDItemList.add(newTD);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.TDItemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here

        String fieldName;

        try {
            fieldName = this.TDItemList.get(i).fieldName;
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException();
        }

        if (fieldName == null) {
            return "null";
        } else {
            return fieldName;
        }

    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        Type fieldType;

        try {
            fieldType = this.TDItemList.get(i).fieldType;
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new NoSuchElementException();
        }

        return fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here

        for (int i = 0; i < this.TDItemList.size(); i++) {
            String fieldName = this.TDItemList.get(i).fieldName;

            if (fieldName != null && name != null) {
                if (fieldName.equals(name)) {
                    return i;
                }
            }
        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // Variable to accumulate the byte size
        int byteSize = 0;

        // Looping through all of the items in the Item List
        for (int i = 0; i < this.TDItemList.size(); i++) {

            // Retrieve the item
            TDItem item = this.TDItemList.get(i);

            // Add the size in bytes by calling .getLen()
            byteSize += item.fieldType.getLen();

        }

        // Return
        return byteSize;

    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here

        int td1Len = td1.numFields();
        int td2Len = td2.numFields();

        Type[] typeAr = new Type[td1Len + td2Len];
        String[] fieldAr = new String[td1Len + td2Len];


        for (int i = 0; i < td1Len; i++) {
            typeAr[i] = td1.getFieldType(i);
            fieldAr[i] = td1.getFieldName(i);
        }

        for (int j = 0; j < td2Len; j++ ) {
            typeAr[j + td1Len] = td2.getFieldType(j);
            fieldAr[j + td1Len] = td2.getFieldName(j);
        }



        TupleDesc td3 = new TupleDesc(typeAr, fieldAr);

        return td3;


    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {

        if (!(o instanceof TupleDesc)) { return false; }

        TupleDesc oTuple = (TupleDesc) o;

        if (oTuple.getSize() != this.getSize()) { return false; }

        for (int i = 0; i < this.TDItemList.size(); i++ ) {
            if (oTuple.getFieldType(i) != this.getFieldType(i)) {
                return false;
            }
        }

        // some code goes here
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results




        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here

        String descriptor = "";

        for (int i = 0; i < this.TDItemList.size(); i++ ) {

            descriptor += this.getFieldType(i).toString() + '(' + this.getFieldName(i) + "),";
        }

        return descriptor;
    }
}
