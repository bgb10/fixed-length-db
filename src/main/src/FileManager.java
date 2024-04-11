import java.io.RandomAccessFile;
import java.util.*;

public class FileManager {

    private final Integer BLOCK_SIZE = 140;
    private final String filePath = "/Users/parkgwanbin/CAU/20240401-8/fixed-length-db/";
    public FileManager() {

    }

    public void createTable(Metadata metadata) {
        try {
            Block firstBlock = new Block(metadata);
            firstBlock.addConvertible(new FreeListNode(metadata.getRecordSize(), 0));
            write(firstBlock, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void write(Block block, int pos) {
        try (RandomAccessFile file = new RandomAccessFile(filePath + block.getMetadata().getTableName() + ".db" , "rw");) {
            if (pos == -1) {
                file.seek(file.length());
            } else {
                file.seek(pos);
            }
            file.write(block.getRaw());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Block read(Metadata metadata, int pos) {
        byte[] raw = new byte[BLOCK_SIZE];
        try (RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db" , "rw");) {
            file.seek(pos);
            file.read(raw);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Block(metadata, raw);
    }

    public void insertTuple(Metadata metadata, HashMap<String, String> attributes) {
        try {
             Record newRecord = new Record(metadata, attributes);
             Block firstBlock = this.read(metadata, 0); // read first block
             List<FreeListNode> freeListNodes = firstBlock.getFreeListNodes();
             if (freeListNodes.isEmpty() || firstBlock.getFreeListNodes().get(0).getNext() == 0) {
                 Block newBlock = new Block(metadata);
                 newBlock.addConvertible(newRecord);
                 write(newBlock, -1);
             }
             else {
                 if (freeListNodes.size() >= 2) {
                     firstBlock.changeFreeListNode(newRecord, 1);
                     write(firstBlock, 0);
                 }
                 else {
                     FreeListNode freeListHeader = firstBlock.getFreeListNodes().get(0);
                     Block secondBlock = this.read(metadata, freeListHeader.getNext());
                     secondBlock.changeFreeListNode(newRecord, 0);
                     write(secondBlock, freeListHeader.getNext());
                 }
             }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void selectAllTuple(Metadata metadata) {
        try (RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db", "r")) {
            long currentPosition = 0;
            int fileIOCount = 0;

            // Iterate through the file
            while (currentPosition < file.length()) {
                // Read a block from the file
                Block block = read(metadata, (int) currentPosition);

                // Iterate through the records in the block
                for (Convertible convertible : block.getConvertibleList()) {
                    if (convertible instanceof Record) {
                        Record record = (Record) convertible;
                        // Process the record (for example, print it)
                        System.out.println(new String(record.getRaw()));
                    }
                }
                // Move to the next block
                int bf = BLOCK_SIZE / metadata.getRecordSize();
                currentPosition += (long) bf * metadata.getRecordSize();
                fileIOCount++;
            }

            System.out.println("fileIOCount = " + fileIOCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void selectById(Metadata metadata, String primaryKeyValue) {
        try (RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db", "r")) {
            long currentPosition = 0;
            int fileIOCount = 0;
            boolean found = false;

            // Iterate through the file
            while (currentPosition < file.length() && !found) {
                // Read a block from the file
                Block block = read(metadata, (int) currentPosition);

                // Iterate through the records in the block
                for (Convertible convertible : block.getConvertibleList()) {
                    if (convertible instanceof Record) {
                        Record record = (Record) convertible;
                        // Extract primary key value from the record
                        String primaryKey = extractPrimaryKey(record.getRaw(), metadata.getPrimaryKeyColumn());

                        // Check if the primary key matches the specified value
                        if (primaryKey.equals(primaryKeyValue)) {
                            // Process the record (for example, print it)
                            System.out.println(new String(record.getRaw()));
                            found = true;
                            break; // Exit the loop if record is found
                        }
                    }
                }

                int bf = BLOCK_SIZE / metadata.getRecordSize();
                currentPosition += (long) bf * metadata.getRecordSize();
                fileIOCount++;
            }

            if (!found) {
                System.out.println("Record with primary key " + primaryKeyValue + " not found.");
            }

            System.out.println("fileIOCount = " + fileIOCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteById(Metadata metadata, String primaryKeyValue) {
        int fileIOCount = 0;
        Block firstBlock = this.read(metadata, 0);
        fileIOCount++;
        // check firstBlock
        int idx = 0;
        boolean found = false;
        for (Convertible convertible : firstBlock.getConvertibleList()) {
            if (convertible instanceof Record) {
                Record record = (Record) convertible;
                // Extract primary key value from the record
                String primaryKey = extractPrimaryKey(record.getRaw(), metadata.getPrimaryKeyColumn());

                // Check if the primary key matches the specified value
                if (primaryKey.equals(primaryKeyValue)) {
                    // Overwrite the record with empty spaces
                    FreeListNode freeListNode = new FreeListNode(metadata.getRecordSize(), firstBlock.getFreeListNodes().get(0).getNext());
                    FreeListNode freeListHeader = new FreeListNode(metadata.getRecordSize(), idx * metadata.getRecordSize());
                    firstBlock.getConvertibleList().set(0, freeListHeader);
                    firstBlock.getConvertibleList().set(idx, freeListNode);
                    write(firstBlock, 0);
                    fileIOCount++;
                    found = true;
                    break; // Exit the loop if record is found and deleted
                }
            }
            idx++;
        }

        if (!found) {
            try (RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db", "rw")) {
                int bf = BLOCK_SIZE / metadata.getRecordSize();
                long currentPosition = (long) bf * metadata.getRecordSize();

                // Iterate through the file
                while (currentPosition < file.length()) {
                    // Read a block from the file
                    Block block = read(metadata, (int) currentPosition);
                    fileIOCount++;

                    idx = 0;
                    for (Convertible convertible : block.getConvertibleList()) {
                        if (convertible instanceof Record) {
                            Record record = (Record) convertible;
                            // Extract primary key value from the record
                            String primaryKey = extractPrimaryKey(record.getRaw(), metadata.getPrimaryKeyColumn());

                            // Check if the primary key matches the specified value
                            if (primaryKey.equals(primaryKeyValue)) {
                                // Overwrite the record with empty spaces
                                FreeListNode freeListNode = new FreeListNode(metadata.getRecordSize(), firstBlock.getFreeListNodes().get(0).getNext());
                                FreeListNode freeListHeader = new FreeListNode(metadata.getRecordSize(), (int) currentPosition + idx * metadata.getRecordSize());
                                firstBlock.getConvertibleList().set(0, freeListHeader);
                                block.getConvertibleList().set(idx, freeListNode);
                                write(firstBlock, 0);
                                write(block, (int) currentPosition);
                                fileIOCount++;
                                fileIOCount++;
                                found = true;
                                break; // Exit the loop if record is found and deleted
                            }
                        }
                        idx++;
                    }

                    currentPosition += (long) bf * metadata.getRecordSize();
                    fileIOCount++;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("fileIOCount = " + fileIOCount);

        if (!found) {
            System.out.println("Record with primary key " + primaryKeyValue + " not found.");
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
