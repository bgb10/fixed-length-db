import java.util.HashMap;


public class Metadata {
    private final String tableName;
    private final HashMap<String, Integer> columns;
    private final String primaryKey;
    private final Integer recordSize;
    public Metadata(String tableName, HashMap<String, Integer> columns, String primaryKey) {
        this.tableName = tableName;
        this.columns = columns;
        this.primaryKey = primaryKey;
        this.recordSize = columns.values().stream().mapToInt(Integer::intValue).sum();
    }
    public String getTableName() {
        return tableName;
    }

    public HashMap<String, Integer> getColumns() {
        return columns;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public Integer getRecordSize() {
        return recordSize;
    }
}
