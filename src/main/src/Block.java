import java.util.ArrayList;

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
            byte[] recordBytes = new byte[recordSize];
            System.arraycopy(raw, i, recordBytes, 0, recordSize);

            if (isFreeListNode(recordBytes)) {
                FreeListNode freeListNode = new FreeListNode(recordBytes);
                convertibleList.add(freeListNode);
            } else {
                // Otherwise, assume it's a Record
                Record record = new Record(recordBytes);
                convertibleList.add(record);
            }
        }
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
}
