package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here

        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here

        // First calculate bit offset
        int bitOffset = BufferPool.getPageSize() * pid.pageNumber();

        byte[] data = new byte[BufferPool.getPageSize()];

        try {
            RandomAccessFile randomFile = new RandomAccessFile(this.file, "r");
            randomFile.seek(bitOffset);
            randomFile.read(data, 0, BufferPool.getPageSize());
            randomFile.close();

            HeapPageId newPid = (HeapPageId) pid;

            return new HeapPage(newPid, data);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here

        int fileNumPages = (int) ( file.length() / BufferPool.getPageSize());

        return fileNumPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        int maxPages = (int) this.file.length() / BufferPool.getPageSize() - 1;

        return new FileIterator(tid, this.getId(), maxPages);
    }

    public class FileIterator implements DbFileIterator {

        private TransactionId tid;
        private Iterator<Tuple> tupleList;
        private HeapPageId pid;

        private int currentPage;
        private int tableId;
        private int totalpages;

        // Constructor for calling this class
        public FileIterator(TransactionId tid, int tableId, int totalPages) {

            // Setting ids and total pages for iterator
            this.tid = tid;
            this.tableId = tableId;
            this.totalpages = totalPages;

            // Since pages are indexed starting at zero,
            // then upon initializing this class current page will be zero
            this.currentPage = 0;
        }


        @Override
        public void open() throws DbException, TransactionAbortedException {

            this.currentPage = 0;

            HeapPageId heapPageId = new HeapPageId(HeapFile.this.getId(), this.currentPage);

            HeapPage page = (HeapPage) Database.getBufferPool().getPage(
                    this.tid,
                    heapPageId,
                    Permissions.READ_ONLY
            );

            // Opens the table at page 0 and sets tuple list to starting page (i.e. page 0)
            this.tupleList = page.iterator();

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {

            if (this.tupleList == null) {
                return false;
            }

            if (this.tupleList.hasNext()) {
                return true;
            }

            if (this.currentPage < this.totalpages) {

                int tableId = HeapFile.this.getId();

                this.currentPage++;

                this.pid = new HeapPageId(tableId, this.currentPage);

                Page curPage = Database.getBufferPool().getPage(this.tid, this.pid, null);

                this.tupleList = ((HeapPage) curPage).iterator();

                return this.hasNext();


            }

            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {

            if (this.tupleList != null) {
                return this.tupleList.next();
            }
            throw new NoSuchElementException();

        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        // Close iterator by setting list and pid to null
        @Override
        public void close() {
            this.currentPage = 0;
            this.tupleList = null;
            this.pid = null;

        }
    }





}

