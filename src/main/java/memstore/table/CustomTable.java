package memstore.table;

import memstore.data.DataLoader;
import memstore.table.RowTable;
import memstore.table.ColumnTable;

import java.io.IOException;

/**
 * Custom table implementation to adapt to provided query mix.
 */
public class CustomTable implements Table {
    protected int numCols;
    protected int numRows;
    RowTable row_table;
    ColumnTable column_table;
    boolean updated;
    long col_0_sum;
    public CustomTable() { }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.row_table = new RowTable();
        this.row_table.load(loader);
        this.column_table = new ColumnTable();
        this.column_table.load(loader);
        this.updated = false;
        this.col_0_sum = column_table.columnSum();
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        return column_table.getIntField(rowId, colId);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        column_table.putIntField(rowId, colId, field);
        if (colId == 0) {
            updated = true;
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
        if (updated) {
            col_0_sum = column_table.columnSum();
            updated = false;
        }
        return col_0_sum;
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
        return column_table.predicatedColumnSum(threshold1, threshold2);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        return column_table.predicatedAllColumnsSum(threshold);

    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        updated = true;
        return column_table.predicatedUpdate(threshold);
    }

}
