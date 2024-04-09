import java.io.RandomAccessFile;
import java.util.HashMap;

public class FileManager {

    private final Integer BLOCK_SIZE = 140;
    private final String filePath = "/Users/parkgwanbin/CAU/20240401-8/fixed-length-db/";
    public FileManager() {

    }


    // TODO: metadata add, freelist add in header,
    public void createTable(String tableName, HashMap<String, Integer> columns, String primaryKey) {
        try {
            RandomAccessFile file = new RandomAccessFile(filePath + tableName + ".db" , "rw");
            // Calculate record size = floor(BLOCK_SIZE / sum of column integer values (column size))
            int recordSize = columns.values().stream().mapToInt(Integer::intValue).sum();
//            int blockingFactor = BLOCK_SIZE / recordSize;
            byte[] freeListRecord = new byte[recordSize];
            // Next free node index (null is 0)
            freeListRecord[0] = '!'; // ! character
            freeListRecord[1] = '@'; // @ character
            freeListRecord[2] = '#'; // # character
            freeListRecord[3] = 0;   // Next free node index (initially 0)
            for (int i = 4; i < recordSize; i++) {
                freeListRecord[i] = '#';
            }
            file.write(freeListRecord);
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
