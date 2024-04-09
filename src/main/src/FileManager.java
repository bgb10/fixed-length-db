import java.io.RandomAccessFile;

public class FileManager {

    private final String filePath = "/Users/parkgwanbin/CAU/20240401-8/fixed-length-db/";
    public FileManager() {

    }


    public void createTable(String tableName) {
        try {
            RandomAccessFile file = new RandomAccessFile(filePath + tableName + ".db" , "rw");
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
