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
        System.out.println("Write I/O");
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
        System.out.println("Read I/O");
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
                     FreeListNode secondFreeListNode = firstBlock.getFreeListNodes().get(1);
                     int nextOfNext = secondFreeListNode.getNext();
                     firstBlock.getConvertibleList().set(0, new FreeListNode(metadata.getRecordSize(), nextOfNext));
                     firstBlock.changeFreeListNode(newRecord, 1);
                     write(firstBlock, 0);
                 }
                 else {
                     FreeListNode freeListHeader = firstBlock.getFreeListNodes().get(0);
                     Block secondBlock = this.read(metadata, freeListHeader.getNext());
                     firstBlock.getConvertibleList().set(0, secondBlock.getFreeListNodes().get(0));
                     secondBlock.changeFreeListNode(newRecord, 0);
                     write(firstBlock, 0);
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
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void selectById(Metadata metadata, String primaryKeyValue) {
        try (RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db", "r")) {
            long currentPosition = 0;
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
            }

            if (!found) {
                System.out.println("Record with primary key " + primaryKeyValue + " not found.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteById(Metadata metadata, String primaryKeyValue) {
        try (RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db", "r")) {
            long currentPosition = 0;
            boolean found = false;
            Block lastFreeListNodeBlock = null;
            int lastFreeListNodeBlockFP = 0;
            FreeListNode lastFreeListNode = null;

            int lastFreeListNodeIdx = 0;
            while (currentPosition < file.length() && !found) {
                Block block = read(metadata, (int) currentPosition);
                int idx = 0;
                for (Convertible convertible : block.getConvertibleList()) {
                    if (convertible instanceof Record) {
                        Record record = (Record) convertible;
                        String primaryKey = extractPrimaryKey(record.getRaw(), metadata.getPrimaryKeyColumn());
                        if (primaryKey.equals(primaryKeyValue)) {
                            assert lastFreeListNode != null;

                            FreeListNode newFreeListNode = new FreeListNode(metadata.getRecordSize(), lastFreeListNode.getNext());
                            FreeListNode newLastFreeListNode = new FreeListNode(metadata.getRecordSize(), (int) currentPosition + idx * metadata.getRecordSize());

                            lastFreeListNodeBlock.getConvertibleList().set(lastFreeListNodeIdx, newLastFreeListNode);
                            block.getConvertibleList().set(idx, newFreeListNode);

                            if (block != lastFreeListNodeBlock) {
                                write(lastFreeListNodeBlock, lastFreeListNodeBlockFP);
                            }
                            write(block, (int) currentPosition);
                            found = true;
                            break; // Exit the loop if record is found
                        }
                    } else if (convertible instanceof FreeListNode) {
                        lastFreeListNodeBlock = block;
                        lastFreeListNode = (FreeListNode) convertible;
                        lastFreeListNodeIdx = idx;
                        lastFreeListNodeBlockFP = (int) currentPosition;
                    }
                    idx++;
                }

                int bf = BLOCK_SIZE / metadata.getRecordSize();
                currentPosition += (long) bf * metadata.getRecordSize();
            }



            if (!found) {
                System.out.println("Record with primary key " + primaryKeyValue + " not found.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void debug(Metadata metadata) {
        try (RandomAccessFile file = new RandomAccessFile(filePath + metadata.getTableName() + ".db", "r")) {
            long currentPosition = 0;

            // Iterate through the file
            while (currentPosition < file.length()) {
                // Read a block from the file
                Block block = read(metadata, (int) currentPosition);

                // Iterate through the records in the block
                for (Convertible convertible : block.getConvertibleList()) {
                    System.out.println(new String(convertible.getRaw()));
                }
                // Move to the next block
                int bf = BLOCK_SIZE / metadata.getRecordSize();
                currentPosition += (long) bf * metadata.getRecordSize();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("EOF!");
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
