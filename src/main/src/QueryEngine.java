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

        Metadata metadata = new Metadata(tableName, columns, primaryKey);
        metadataManager.createTable(metadata);
        fileManager.createTable(metadata);
        System.out.println("Table Created!");
    }

    public void insertTuple(String tableName, HashMap<String, String> attributes) {
        if (!metadataManager.isTableExists(tableName)) {
            System.out.println(tableName + " NO Exists!");
            return;
        }

        Metadata metadata = metadataManager.getTableMetadata(tableName);
        fileManager.insertTuple(metadata, attributes);
        System.out.println("Tuple Inserted!");
    }
}
