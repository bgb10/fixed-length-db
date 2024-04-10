import javax.management.Query;
import java.util.HashMap;
import java.util.Scanner;

public class CLI {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        QueryEngine qe = new QueryEngine();

        while (true) {
            System.out.println("\n1. Create Table\n2. Insert Tuple\n3. Delete Tuple\n4. Select\n5. Select All\n6. Exit");
            System.out.print("Enter your choice: ");
            int choice = scanner.nextInt();

            String tableName;
            boolean tableExists;
            int numColumns;

            switch (choice) {
                case 1:
                    System.out.print("Enter table name: ");
                    tableName = scanner.next();
                    System.out.print("Enter number of columns: ");
                    numColumns = scanner.nextInt();
                    HashMap<String, Integer> columns = new HashMap<>();
                    for (int i = 0; i < numColumns; i++) {
                        System.out.print("Enter column name: ");
                        String columnName = scanner.next();
                        System.out.print("Enter column bytes: ");
                        Integer length = scanner.nextInt();
                        columns.put(columnName, length);
                    }
                    System.out.print("Enter PK column name: "); // Ask for primary key column name
                    String primaryKey = scanner.next();
                    qe.createTable(tableName, columns, primaryKey);
                    break;
                case 2:
                    System.out.print("Enter table name: ");
                    tableName = scanner.next();
                    System.out.print("Enter number of columns: ");
                    numColumns = scanner.nextInt();
                    HashMap<String, String> attributes = new HashMap<>();
                    for (int i = 0; i < numColumns; i++) {
                        System.out.print("Enter attribute name: ");
                        String name = scanner.next();
                        System.out.print("Enter attribute value: ");
                        String value = scanner.next();
                        attributes.put(name, value);
                    }
                    qe.insertTuple(tableName, attributes);
                    break;
                case 3:
                    break;
                case 4:
                    break;
                case 5:
                    System.out.print("Enter table name: ");
                    tableName = scanner.next();
                    qe.selectAllTuple(tableName);
                    break;
                case 6:
                    System.out.println("Exiting...");
                    scanner.close();
                    System.exit(0);
                    break;
                default:
                    System.out.println("Invalid choice. Please try again.");
                    break;
            }
        }
    }
}
