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

    public void createTable(String tableName, HashMap<String, Integer> columns, String primaryKey) {
        try {
            if (isTableExists(tableName)) {
                System.out.println("table exists.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        try {
            try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password)) {
                // Insert metadata into relation_metadata table
                String insertRelationMetadataQuery = "INSERT INTO relation_metadata (relation_name, pk_column_name) VALUES (?, ?)";
                try (PreparedStatement relationMetadataStatement = connection.prepareStatement(insertRelationMetadataQuery)) {
                    relationMetadataStatement.setString(1, tableName);
                    relationMetadataStatement.setString(2, primaryKey);
                    relationMetadataStatement.executeUpdate();
                }

                // Insert metadata into attribute_metadata table for each column
                String insertAttributeMetadataQuery = "INSERT INTO attribute_metadata (relation_name, column_name, size) VALUES (?, ?, ?)";
                try (PreparedStatement attributeMetadataStatement = connection.prepareStatement(insertAttributeMetadataQuery)) {
                    for (String columnName : columns.keySet()) {
                        attributeMetadataStatement.setString(1, tableName);
                        attributeMetadataStatement.setString(2, columnName);
                        attributeMetadataStatement.setInt(3, columns.get(columnName));
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
}