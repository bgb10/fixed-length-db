import java.util.HashMap;

public class QueryEngine {

    private MetadataManager metadataManager;
    private FileManager fileManager;
    public QueryEngine() {
        metadataManager = new MetadataManager();
        fileManager = new FileManager();
    }

    public void createTable(String tableName, HashMap<String, Integer> columns, String primaryKey) {
        if (metadataManager.isTableExists(tableName)) {
            System.out.println(tableName + " Exists!");
            return;
        }

        metadataManager.createTable(tableName, columns, primaryKey);
        fileManager.createTable(tableName);
        System.out.println("Table Created!");
    }
}
