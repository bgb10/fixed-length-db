import java.util.HashMap;


public class Metadata {
    private String tableName;
    private HashMap<String, Integer> columns;
    private String primaryKey;
    private Integer recordSize;
    public Metadata(String tableName, HashMap<String, Integer> columns, String primaryKey) {
        this.tableName = tableName;
        this.columns = columns;
        this.primaryKey = primaryKey;
        this.recordSize = columns.values().stream().mapToInt(Integer::intValue).sum();
    }

    public Metadata() {

    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public HashMap<String, Integer> getColumns() {
        return columns;
    }

    public void setColumns(HashMap<String, Integer> columns) {
        this.columns = columns;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Integer getRecordSize() {
        return recordSize;
    }

    public void setRecordSize(Integer recordSize) {
        this.recordSize = recordSize;
    }
}
