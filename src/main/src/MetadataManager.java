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
            if (isTableExists(metadata.tableName)) {
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
                    relationMetadataStatement.setString(1, metadata.tableName);
                    relationMetadataStatement.setString(2, metadata.primaryKey);
                    relationMetadataStatement.executeUpdate();
                }

                // Insert metadata into attribute_metadata table for each column
                String insertAttributeMetadataQuery = "INSERT INTO attribute_metadata (relation_name, column_name, size) VALUES (?, ?, ?)";
                try (PreparedStatement attributeMetadataStatement = connection.prepareStatement(insertAttributeMetadataQuery)) {
                    for (String columnName : metadata.columns.keySet()) {
                        attributeMetadataStatement.setString(1, metadata.tableName);
                        attributeMetadataStatement.setString(2, columnName);
                        attributeMetadataStatement.setInt(3, metadata.columns.get(columnName));
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
        Metadata metadata = new Metadata();
        try {
            if (!isTableExists(tableName)) {
                System.out.println("Table does not exist.");
                return metadata;
            }

            try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password)) {
                // Get primary key column
                String primaryKeyQuery = "SELECT pk_column_name FROM relation_metadata WHERE relation_name = ?";
                try (PreparedStatement primaryKeyStatement = connection.prepareStatement(primaryKeyQuery)) {
                    primaryKeyStatement.setString(1, tableName);
                    try (ResultSet resultSet = primaryKeyStatement.executeQuery()) {
                        if (resultSet.next()) {
                            metadata.primaryKey = resultSet.getString("pk_column_name");
                        }
                    }
                }

                // Get columns and their sizes
                String columnsQuery = "SELECT column_name, size FROM attribute_metadata WHERE relation_name = ?";
                try (PreparedStatement columnsStatement = connection.prepareStatement(columnsQuery)) {
                    columnsStatement.setString(1, tableName);
                    try (ResultSet resultSet = columnsStatement.executeQuery()) {
                        while (resultSet.next()) {
                            metadata.columns.put(resultSet.getString("column_name"), resultSet.getInt("size"));
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return metadata;
    }
}