public class FreeListNode implements Convertible {
    private final Integer recordSize;
    private final Integer next;

    public FreeListNode(byte[] raw) {
        StringBuilder freeIndexStr = new StringBuilder();
        for (int i = 3; i < raw.length; i++) {
            if (raw[i] == '#') {
                break;
            }
            freeIndexStr.append((char) raw[i]);
        }
        this.next = Integer.parseInt(freeIndexStr.toString());
        this.recordSize = raw.length;
    }

    public FreeListNode(int recordSize, int next) {
        this.recordSize = recordSize;
        this.next = next;
    }

    public byte[] getRaw() {
        // convert int next to char[] next, and fill raw from char[3]
        // for example, if next = 420 then char[3] = '4', char[4] = '2', char[5] = '0'
        char[] nextChars = Integer.toString(next).toCharArray();
        byte[] raw = new byte[recordSize];
        raw[0] = '!';
        raw[1] = '@';
        raw[2] = '#';
        for (int i = 0; i < nextChars.length; i++) {
            raw[3 + i] = (byte) nextChars[i];
        }
        for (int i = nextChars.length + 3; i < recordSize; i++) {
            raw[i] = '#';
        }

        return raw;
    }

    public Integer getNext() {
        return next;
    }
}
