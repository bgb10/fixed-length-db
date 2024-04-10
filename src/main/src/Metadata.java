import java.util.ArrayList;
import java.util.HashMap;


public class Metadata {
    private final String tableName;
    private final ArrayList<Column> columns;
    private final String primaryKey;
    private final Integer recordSize;
    public Metadata(String tableName, HashMap<String, Integer> columns, String primaryKey) {
        this.tableName = tableName;
        this.columns = new ArrayList<>();
        this.primaryKey = primaryKey;

        // First loop: find and add the primary key column
        int pos = 0;
        for (HashMap.Entry<String, Integer> entry : columns.entrySet()) {
            String columnName = entry.getKey();
            Integer size = entry.getValue();
            if (columnName.equals(primaryKey)) {
                this.columns.add(new Column(columnName, 0, size)); // Add primary key column at position 0
                pos += size; // Increment position by size of the primary key column
                break; // Exit loop after finding the primary key column
            }
        }

        // Second loop: add the remaining columns
        for (HashMap.Entry<String, Integer> entry : columns.entrySet()) {
            String columnName = entry.getKey();
            Integer size = entry.getValue();
            if (!columnName.equals(primaryKey)) { // Skip primary key column
                this.columns.add(new Column(columnName, pos, size)); // Add column at current position
                pos += size; // Increment position by size of the column
            }
        }

        // Calculate the total record size
        this.recordSize = pos;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public Integer getRecordSize() {
        return recordSize;
    }

    public ArrayList<Column> getColumns() {
        return columns;
    }
}
