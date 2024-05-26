-- Create relation_metadata table
CREATE TABLE relation_metadata (
                                   relation_name VARCHAR(255) PRIMARY KEY,
                                   pk_column_name VARCHAR(255)
);

-- Create attribute_metadata table
CREATE TABLE attribute_metadata (
                                    relation_name VARCHAR(255),
                                    column_name VARCHAR(255),
                                    pos INT,
                                    size INT,
                                    PRIMARY KEY (relation_name, column_name),
                                    FOREIGN KEY (relation_name) REFERENCES relation_metadata(relation_name)
);