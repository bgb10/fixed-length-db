import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.HashMap;

public class FileManager {

    private final Integer BLOCK_SIZE = 140;
    private final String filePath = "/Users/parkgwanbin/CAU/20240401-8/fixed-length-db/";
    public FileManager() {

    }

    public void createTable(Metadata metadata) {
        try {
            RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db" , "rw");
            // Calculate record size = floor(BLOCK_SIZE / sum of column integer values (column size))
            int recordSize = metadata.getColumns().values().stream().mapToInt(Integer::intValue).sum();
//            int blockingFactor = BLOCK_SIZE / recordSize;
            byte[] freeListRecord = new byte[recordSize];
            // Next free node index (null is 0)
            freeListRecord[0] = '!'; // ! character
            freeListRecord[1] = '@'; // @ character
            freeListRecord[2] = '#'; // # character
            freeListRecord[3] = '0';   // Next free node index (initially 0)
            for (int i = 4; i < recordSize; i++) {
                freeListRecord[i] = '#';
            }
            file.write(freeListRecord);
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // current implementation no freelist, only append. (but block)
    public void insertTuple(Metadata metadata, HashMap<String, String> attributes) {
        try {
            RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db" , "rw");
            byte[] record = new byte[metadata.getRecordSize()];
            // If the attribute value is longer than the column size, truncate it
            // If the attribute value is shorter than the column size, pad it with empty spaces
            int offset = 0; // Current offset within the record
            for (String columnName : metadata.getColumns().keySet()) {
                String value = attributes.getOrDefault(columnName, ""); // Get attribute value or empty string if not present
                int columnSize = metadata.getColumns().get(columnName); // Get column size from metadata
                byte[] valueBytes = value.getBytes(); // Convert attribute value to bytes
                if (valueBytes.length > columnSize) {
                    // Truncate the value if it's longer than the column size
                    System.arraycopy(valueBytes, 0, record, offset, columnSize);
                } else {
                    // Pad the value with empty spaces if it's shorter than the column size
                    System.arraycopy(valueBytes, 0, record, offset, valueBytes.length);
                    for (int i = valueBytes.length; i < columnSize; i++) {
                        record[offset + i] = ' '; // Padding with empty spaces
                    }
                }
                offset += columnSize; // Move the offset to the next column
            }

            file.seek(file.length());

            file.write(record);
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
