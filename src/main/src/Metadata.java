import java.util.HashMap;


public class Metadata {

    public String tableName;
    public HashMap<String, Integer> columns;
    public String primaryKey;

    public Integer recordSize;

    public Metadata(String tableName, HashMap<String, Integer> columns, String primaryKey) {
        this.tableName = tableName;
        this.columns = columns;
        this.primaryKey = primaryKey;
        this.recordSize = columns.values().stream().mapToInt(Integer::intValue).sum();

    }

    public Metadata() {
        this.tableName = "";
        this.columns = new HashMap<>();
        this.primaryKey = "";
    }
}
