import java.util.HashMap;

public class Record implements Convertible {

    private byte[] raw;
    // arraylist
    public Record(Metadata metadata, HashMap<String, String> attributes) {

    }

    public Record(byte[] raw) {
        this.raw = raw;
    }


    @Override
    public byte[] getRaw() {
        return new byte[0];
    }
}
