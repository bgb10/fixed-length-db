public class Column {
    private final String name;
    private final Integer pos;
    private final Integer size;

    public Column(String columnName, Integer pos, Integer size) {
        this.name = columnName;
        this.pos = pos;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public Integer getPos() {
        return pos;
    }

    public Integer getSize() {
        return size;
    }
}
