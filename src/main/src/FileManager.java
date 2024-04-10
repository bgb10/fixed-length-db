import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

public class FileManager {

    private final Integer BLOCK_SIZE = 140;
    private final String filePath = "/Users/parkgwanbin/CAU/20240401-8/fixed-length-db/";
    public FileManager() {

    }

    public void createTable(Metadata metadata) {
        try {
            RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db" , "rw");
            // Calculate record size = floor(BLOCK_SIZE / sum of column integer values (column size))
            int recordSize = metadata.getRecordSize();
//            int blockingFactor = BLOCK_SIZE / recordSize;
            byte[] freeListRecord = new byte[recordSize];
            // Next free node index (null is 0)
            freeListRecord[3] = '0';   // Next free node index (initially 0)
            file.write(freeListRecord);
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // current implementation no freelist, only append. (but block)
    public void insertTuple(Metadata metadata, HashMap<String, String> attributes) {
        try {
            RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db", "rw");
            byte[] record = new byte[metadata.getRecordSize()];
            // If the attribute value is longer than the column size, truncate it
            // If the attribute value is shorter than the column size, pad it with empty spaces
            List<Column> sortedColumns = new ArrayList<>(metadata.getColumns());
            sortedColumns.sort(Comparator.comparingInt(Column::getPos)); // Sort columns by pos
            int offset = 0; // Current offset within the record
            for (Column column : sortedColumns) {
                String columnName = column.getName();
                String value = attributes.getOrDefault(columnName, ""); // Get attribute value or empty string if not present
                int columnSize = column.getSize(); // Get column size from metadata
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
    public void selectAllTuple(Metadata metadata) {
        try {
            RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db", "r");

            long fileLength = file.length();
            long currentPosition = 0;

            int recordSize = metadata.getRecordSize();
            byte[] block = new byte[BLOCK_SIZE];
            byte[] record = new byte[recordSize];

            int fileIOCount = 0;
            while (currentPosition < fileLength) {
                file.seek(currentPosition);
                int bytesRead = file.read(block);
                if (bytesRead == -1) {
                    break; // End of file
                }
                fileIOCount++; // Increment file I/O counter for each read operation
                int numRecordsInBlock = bytesRead / recordSize;
                currentPosition += (long) numRecordsInBlock * recordSize;
                System.out.println("currentPosition = " + currentPosition);

                for (int i = 0; i < numRecordsInBlock; i++) {
                    System.arraycopy(block, i * recordSize, record, 0, recordSize);
                    // Process the record here (for example, print it)
                    System.out.println(new String(record)); // Assuming the record is stored as a string
                }
                block = new byte[BLOCK_SIZE];
            }

            System.out.println("fileIOCount = " + fileIOCount);

            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void selectById(Metadata metadata, String primaryKeyValue) {
        try {
            RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db", "r");

            long fileLength = file.length();
            long currentPosition = 0;

            int recordSize = metadata.getRecordSize();
            byte[] block = new byte[BLOCK_SIZE];
            byte[] record = new byte[recordSize];

            int fileIOCount = 0;
            boolean found = false;
            while (currentPosition < fileLength && !found) {
                file.seek(currentPosition);
                int bytesRead = file.read(block);
                if (bytesRead == -1) {
                    break; // End of file
                }
                fileIOCount++; // Increment file I/O counter for each read operation
                int numRecordsInBlock = bytesRead / recordSize;
                currentPosition += (long) numRecordsInBlock * recordSize;

                for (int i = 0; i < numRecordsInBlock; i++) {
                    System.arraycopy(block, i * recordSize, record, 0, recordSize);
                    // Extract primary key value from the record
                    String primaryKey = extractPrimaryKey(record, metadata.getPrimaryKeyColumn());

                    // Check if the primary key matches the specified value
                    if (primaryKey.equals(primaryKeyValue)) {
                        // Process the record (for example, print it)
                        System.out.println(new String(record).trim()); // Assuming the record is stored as a string
                        found = true;
                        break; // Exit the loop if record is found
                    }
                }
                block = new byte[BLOCK_SIZE];
            }

            if (!found) {
                System.out.println("Record with primary key " + primaryKeyValue + " not found.");
            }

            System.out.println("fileIOCount = " + fileIOCount);

            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteById(Metadata metadata, String primaryKeyValue) {
        try {
            RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db", "rw");

            long fileLength = file.length();
            long currentPosition = 0;

            int recordSize = metadata.getRecordSize();
            byte[] block = new byte[BLOCK_SIZE];
            byte[] record = new byte[recordSize];

            int fileIOCount = 0;
            boolean found = false;
            while (currentPosition < fileLength && !found) {
                file.seek(currentPosition);
                int bytesRead = file.read(block);
                if (bytesRead == -1) {
                    break; // End of file
                }
                fileIOCount++; // Increment file I/O counter for each read operation
                int numRecordsInBlock = bytesRead / recordSize;
                currentPosition += (long) numRecordsInBlock * recordSize;

                for (int i = 0; i < numRecordsInBlock; i++) {
                    System.arraycopy(block, i * recordSize, record, 0, recordSize);
                    // Extract primary key value from the record
                    String primaryKey = extractPrimaryKey(record, metadata.getPrimaryKeyColumn());

                    // Check if the primary key matches the specified value
                    if (primaryKey.equals(primaryKeyValue)) {
                        // Overwrite the record with empty spaces
                        byte[] emptyRecord = new byte[recordSize];
                        file.seek(currentPosition - recordSize); // Move file pointer back to the start of the record
                        file.write(emptyRecord); // Write empty record to overwrite existing record
                        found = true;
                        break; // Exit the loop if record is found and deleted
                    }
                }
                block = new byte[BLOCK_SIZE];
            }

            if (!found) {
                System.out.println("Record with primary key " + primaryKeyValue + " not found.");
            }

            System.out.println("fileIOCount = " + fileIOCount);

            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Helper method to extract primary key value from the record
    private String extractPrimaryKey(byte[] record, Column primaryKeyColumn) {
        int pos = primaryKeyColumn.getPos();
        int size = primaryKeyColumn.getSize();
        byte[] primaryKeyBytes = new byte[size];
        System.arraycopy(record, pos, primaryKeyBytes, 0, size);
        return new String(primaryKeyBytes).trim(); // Assuming the primary key is stored as a string
    }

}
