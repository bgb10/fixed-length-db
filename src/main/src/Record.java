import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class Record implements Convertible {

    private byte[] raw;
    // arraylist
    public Record(Metadata metadata, HashMap<String, String> attributes) {
        this.raw = new byte[metadata.getRecordSize()];

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
                System.arraycopy(valueBytes, 0, this.raw, offset, columnSize);
            } else {
                // Pad the value with empty spaces if it's shorter than the column size
                System.arraycopy(valueBytes, 0, this.raw, offset, valueBytes.length);
                for (int i = valueBytes.length; i < columnSize; i++) {
                    this.raw[offset + i] = ' '; // Padding with empty spaces
                }
            }
            offset += columnSize; // Move the offset to the next column
        }
    }

    public Record(byte[] raw) {
        this.raw = raw;
    }

    @Override
    public byte[] getRaw() {
        return raw;
    }
}
