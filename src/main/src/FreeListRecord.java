public class FreeListRecord {
    private final byte[] raw;
    private final Integer next;

    public FreeListRecord(byte[] raw) {
        StringBuilder freeIndexStr = new StringBuilder();
        for (int i = 3; i < raw.length; i++) {
            if (raw[i] == 0) {
                break;
            }
            freeIndexStr.append((char) raw[i]);
        }
        this.next = Integer.parseInt(freeIndexStr.toString());
        this.raw = raw;
    }

    public FreeListRecord(int recordSize, int next) {
        this.next = next;

        byte[] raw = new byte[recordSize];

        // convert int next to char[] next, and fill raw from char[3]
        // for example, if next = 420 then char[3] = '4', char[4] = '2', char[5] = '0'
        char[] nextChars = Integer.toString(next).toCharArray();
        raw = new byte[recordSize];
        for (int i = 0; i < nextChars.length; i++) {
            raw[3 + i] = (byte) nextChars[i];
        }

        this.raw = raw;
    }

    public byte[] getRaw() {
        return raw;
    }

    public Integer getNext() {
        return next;
    }
}
