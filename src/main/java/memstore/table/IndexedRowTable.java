package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;


/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private int indexColumn;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        // TODO: Implement this!

        this.index = new TreeMap<>();

        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();

            
        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            int value = curRow.getInt(ByteFormat.FIELD_LEN * this.indexColumn);

            if (!this.index.containsKey(value)) {
                this.index.put(value, new IntArrayList());
            }

            this.index.get(value).add(rowId);
        }



        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                this.rows.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
            }
        }


    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);

        return this.rows.getInt(offset);
        //return 0;
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {

        int cur_val = getIntField(rowId, colId);
        
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        rows.putInt(offset, field);

        if(colId == this.indexColumn) {
            IntArrayList tmp = this.index.get(cur_val);
            if (tmp != null) {
                int idx_to_remove = tmp.indexOf(rowId);
                if (idx_to_remove >= 0) {
                    tmp.remove(idx_to_remove);
                }
            }

            tmp = this.index.get(field);
            if (tmp == null) {
                tmp = new IntArrayList(100);
                this.index.put(field, tmp); 
            }
            tmp.add(rowId);
        }
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        // TODO: Implement this!

        long sum = 0;
        for (int rowId = 0; rowId < numRows; rowId++) {
            sum += getIntField(rowId, 0);
        }
        return sum;

        //return 0;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        // TODO: Implement this!

        long sum = 0;

        if (this.indexColumn == 1) {
            for (Integer key : index.tailMap(threshold1, false).keySet()) {
                IntArrayList row_numbers = index.get(key);
                for(int rowId : row_numbers){
                    if (getIntField(rowId, 2)  < threshold2) {
                        sum += getIntField(rowId, 0);
                    }
                }
            }
            return sum;
        }

        if (this.indexColumn == 2) {
            for (Integer key : index.headMap(threshold2, false).keySet()) {
                IntArrayList row_numbers = index.get(key);
                for(int rowId : row_numbers){
                    if (getIntField(rowId, 1)  > threshold1) {
                        sum += getIntField(rowId, 0);
                    }
                }
            }
            return sum;
        }




        for (int rowId = 0; rowId < numRows; rowId++) {
            if (getIntField(rowId, 1) > threshold1 && getIntField(rowId, 2) < threshold2) {
                sum += getIntField(rowId, 0);
            }
        }
        return sum;

        //return 0;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        // TODO: Implement this!

        long sum = 0;
        for (int rowId = 0; rowId < numRows; rowId++) {
            if (getIntField(rowId, 0) > threshold) {
                for (int colId = 0; colId < numCols; colId++) {
                    sum += getIntField(rowId, colId);
                }
            }
        }
        return sum;


        //return 0;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        // TODO: Implement this!

        int num = 0;
        for (Integer key : this.index.headMap(threshold, false).keySet()) {
            IntArrayList row_numbers = index.get(key);
            for(int rowId : row_numbers){
                num ++; 
                putIntField(rowId, 3, getIntField(rowId, 2) + getIntField(rowId, 3));
            }
        }
        return num;

        //return 0;
    }
}
