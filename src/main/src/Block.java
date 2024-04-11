import java.util.ArrayList;
import java.util.Iterator;

public class Block {
    private ArrayList<Convertible> convertibleList;
    private Metadata metadata;

    public Block(Metadata metadata) {
        this.metadata = metadata;
        this.convertibleList = new ArrayList<>();
    }

    public Block(Metadata metadata, byte[] raw) {
        this.metadata = metadata;
        this.convertibleList = new ArrayList<>();

        int recordSize = metadata.getRecordSize();
        for (int i = 0; i < raw.length; i += recordSize) {
            if (i + recordSize > raw.length) {
                break;
            }

            byte[] recordBytes = new byte[recordSize];
            System.arraycopy(raw, i, recordBytes, 0, recordSize);

            // if all bytes are null, then break;
            // Check if all bytes are null (zero)
            boolean allNull = true;
            for (byte b : recordBytes) {
                if (b != 0) {
                    allNull = false;
                    break;
                }
            }
            if (allNull) {
                break;
            }

            if (isFreeListNode(recordBytes)) {
                FreeListNode freeListNode = new FreeListNode(recordBytes);
                System.out.println("freeListNode added!");
                convertibleList.add(freeListNode);
            } else {
                // Otherwise, assume it's a Record
                System.out.println("record added!");
                Record record = new Record(recordBytes);
                convertibleList.add(record);
            }
        }
    }

    public ArrayList<FreeListNode> getFreeListNodes() {
        ArrayList<FreeListNode> freeListNodes = new ArrayList<>();

        for (Convertible convertible : convertibleList) {
            if (convertible instanceof FreeListNode) {
                freeListNodes.add((FreeListNode) convertible);
            }
        }

        return freeListNodes;
    }
    private boolean isFreeListNode(byte[] recordBytes) {
        // Check if the byte string starts with "!@#"
        byte[] markerBytes = "!@#".getBytes();
        for (int i = 0; i < markerBytes.length; i++) {
            if (i >= recordBytes.length || recordBytes[i] != markerBytes[i]) {
                return false;
            }
        }
        return true;
    }

    public void addConvertible(Convertible convertible) {
        convertibleList.add(convertible);
    }

    public byte[] getRaw() {
        int totalSize = convertibleList.size() * metadata.getRecordSize();
        byte[] raw = new byte[totalSize];
        int currentIndex = 0;

        for (Convertible convertible : convertibleList) {
            byte[] recordBytes = convertible.getRaw(); // Assuming Convertible has a method getRaw() to retrieve its raw byte representation
            System.arraycopy(recordBytes, 0, raw, currentIndex, recordBytes.length);
            currentIndex += recordBytes.length;
        }

        return raw;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void changeFreeListNode(Record newRecord, int i) {
        try {
            int freeListNodeCount = 0;

            // Iterate through the convertibleList
            Iterator<Convertible> iterator = convertibleList.iterator();
            while (iterator.hasNext()) {
                Convertible convertible = iterator.next();

                // Check if the convertible is a FreeListNode
                if (convertible instanceof FreeListNode) {
                    if (freeListNodeCount == i) {
                        // Replace the i-th occurrence of FreeListNode with newRecord
                        iterator.remove(); // Remove the FreeListNode
                        convertibleList.add(i, newRecord); // Add newRecord at the same index
                        return; // Exit the method after successful replacement
                    }
                    freeListNodeCount++; // Increment the count of FreeListNode occurrences
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
