import java.sql.*;
import java.util.HashMap;

public class MetadataManager {

    private static final String jdbcUrl = "jdbc:mysql://localhost:3306/fixed-length-dbms-metadata";
    // Database credentials
    private static final String user = "root";
    private static final String password = "1313";

    public MetadataManager() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createTable(Metadata metadata) {
        try {
            if (isTableExists(metadata.getTableName())) {
                System.out.println("Table already exists.");
                return; // Exit function if the table already exists
            }
        } catch (Exception e) {
            e.printStackTrace();
            return; // Exit function if an exception occurs
        }

        try {
            // Establish database connection
            try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password)) {
                // Insert metadata into relation_metadata table
                String insertRelationMetadataQuery = "INSERT INTO relation_metadata (relation_name, pk_column_name) VALUES (?, ?)";
                try (PreparedStatement relationMetadataStatement = connection.prepareStatement(insertRelationMetadataQuery)) {
                    relationMetadataStatement.setString(1, metadata.getTableName());
                    relationMetadataStatement.setString(2, metadata.getPrimaryKey());
                    relationMetadataStatement.executeUpdate();
                }

                // Insert metadata into attribute_metadata table for each column
                String insertAttributeMetadataQuery = "INSERT INTO attribute_metadata (relation_name, column_name, pos, size) VALUES (?, ?, ?, ?)";
                try (PreparedStatement attributeMetadataStatement = connection.prepareStatement(insertAttributeMetadataQuery)) {
                    for (Column column : metadata.getColumns()) {
                        attributeMetadataStatement.setString(1, metadata.getTableName());
                        attributeMetadataStatement.setString(2, column.getName());
                        attributeMetadataStatement.setInt(3, column.getPos());
                        attributeMetadataStatement.setInt(4, column.getSize());
                        attributeMetadataStatement.executeUpdate();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean isTableExists(String tableName) {
        try {
            try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password)) {
                // Check if the table exists in relation_metadata table
                String checkTableExistsQuery = "SELECT COUNT(*) AS count FROM relation_metadata WHERE relation_name = ?";
                try (PreparedStatement statement = connection.prepareStatement(checkTableExistsQuery)) {
                    statement.setString(1, tableName);
                    try (ResultSet resultSet = statement.executeQuery()) {
                        // Get the count of tables with the given name
                        int count = 0;
                        if (resultSet.next()) {
                            count = resultSet.getInt("count");
                        }
                        return count > 0;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public Metadata getTableMetadata(String tableName) {
        String primaryKey = "";
        HashMap<String, Integer> columns = new HashMap<>();
        try {
            if (!isTableExists(tableName)) {
                System.out.println("Table does not exist.");
                return null;
            }

            // Establish database connection
            try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password)) {
                // Get primary key column
                String primaryKeyQuery = "SELECT pk_column_name FROM relation_metadata WHERE relation_name = ?";
                try (PreparedStatement primaryKeyStatement = connection.prepareStatement(primaryKeyQuery)) {
                    primaryKeyStatement.setString(1, tableName);
                    try (ResultSet resultSet = primaryKeyStatement.executeQuery()) {
                        if (resultSet.next()) {
                            primaryKey = resultSet.getString("pk_column_name");
                        }
                    }
                }

                // Get columns and their sizes
                String columnsQuery = "SELECT column_name, pos, size FROM attribute_metadata WHERE relation_name = ?";
                try (PreparedStatement columnsStatement = connection.prepareStatement(columnsQuery)) {
                    columnsStatement.setString(1, tableName);
                    try (ResultSet resultSet = columnsStatement.executeQuery()) {
                        while (resultSet.next()) {
                            String columnName = resultSet.getString("column_name");
                            int pos = resultSet.getInt("pos");
                            int size = resultSet.getInt("size");
                            columns.put(columnName, size);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Metadata(tableName, columns, primaryKey);
    }
}