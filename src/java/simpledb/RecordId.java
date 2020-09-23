package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private PageId recordPID;
    private int recordTupleno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.recordPID = pid;
        this.recordTupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        return this.recordTupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return this.recordPID;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof RecordId) {
            RecordId oRecordId = (RecordId) o;
            return oRecordId.getPageId().equals(this.getPageId()) && oRecordId.tupleno() == this.tupleno();
        }
        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        String stringHash = this.getPageId().toString() + Integer.toString(this.tupleno());
        return stringHash.hashCode();
//        throw new UnsupportedOperationException("implement this");

    }

}
