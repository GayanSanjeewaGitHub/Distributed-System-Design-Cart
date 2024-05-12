package utility;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DBTransaction {
    public static void sellerLogIn(String accountId, String password) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            // Get connection from singleton class
            connection = MySQLConnectionSingleton.getInstance();

            // Set auto-commit to false to start a transaction
            connection.setAutoCommit(false);

            // Check if user exists and password matches
            String sqlCheckUser = "SELECT id FROM seller WHERE id = ? AND password = ?";
            preparedStatement = connection.prepareStatement(sqlCheckUser);
            preparedStatement.setString(1, accountId);
            preparedStatement.setString(2, password);
            resultSet = preparedStatement.executeQuery();

            boolean userExists = resultSet.next();

            if (userExists) {
                // User exists and password matches, update status to logged in
                String sqlUpdateStatus = "UPDATE seller SET status = 1 WHERE id = ?";
                preparedStatement = connection.prepareStatement(sqlUpdateStatus);
                preparedStatement.setString(1, accountId);
                preparedStatement.executeUpdate();
                System.out.println("User logged in successfully.");
            } else {
                // User does not exist or password is incorrect, create new user and set status to logged in
                String sqlInsert = "INSERT INTO seller (id, password, status) VALUES (?, ?, 1)";
                preparedStatement = connection.prepareStatement(sqlInsert);
                preparedStatement.setString(1, accountId);
                preparedStatement.setString(2, password);
                preparedStatement.executeUpdate();
                System.out.println("User created and logged in successfully.");
            }

            // Commit the transaction
            connection.commit();
        } catch (SQLException e) {
            // Rollback the transaction in case of exception
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            // Close resources and set auto-commit back to true
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.setAutoCommit(true);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public static void sellerLogOut(String accountId) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            // Get connection from singleton class
            connection = MySQLConnectionSingleton.getInstance();

            // Set auto-commit to false to start a transaction
            connection.setAutoCommit(false);

            // Check if user exists
            String sqlCheckUser = "SELECT id FROM seller WHERE id = ?";
            preparedStatement = connection.prepareStatement(sqlCheckUser);
            preparedStatement.setString(1, accountId);
            resultSet = preparedStatement.executeQuery();

            boolean userExists = resultSet.next();

            if (userExists) {
                // User exists, update status to indicate logout
                String sqlUpdateStatus = "UPDATE seller SET status = 0 WHERE id = ?";
                preparedStatement = connection.prepareStatement(sqlUpdateStatus);
                preparedStatement.setString(1, accountId);
                preparedStatement.executeUpdate();
                System.out.println("User logged out successfully.");
            } else {
                // User does not exist
                System.out.println("User with ID " + accountId + " does not exist.");
            }

            // Commit the transaction
            connection.commit();
        } catch (SQLException e) {
            // Rollback the transaction in case of exception
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            // Close resources and set auto-commit back to true
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.setAutoCommit(true);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void addOrUpdateItems(String accountId, HashMap<String, String> map) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            // Get connection from singleton class
            connection = MySQLConnectionSingleton.getInstance();

            // Set auto-commit to false to start a transaction
            connection.setAutoCommit(false);

            // Check if seller account exists
            String sqlCheckSeller = "SELECT id FROM seller WHERE id = ?";
            preparedStatement = connection.prepareStatement(sqlCheckSeller);
            preparedStatement.setString(1, accountId);
            resultSet = preparedStatement.executeQuery();
            boolean sellerExists = resultSet.next();

            if (sellerExists) {
                // Seller account exists, check if item exists for the seller
                String itemId = map.get("itemId");
                String sqlCheckItem = "SELECT id FROM selleritems WHERE itemname = ? AND seller_id = ?";
                preparedStatement = connection.prepareStatement(sqlCheckItem);
                preparedStatement.setString(1, itemId);
                preparedStatement.setString(2, accountId);
                resultSet = preparedStatement.executeQuery();
                boolean itemExists = resultSet.next();

                if (itemExists) {
                    // Item exists, update the record
                    String sqlUpdateItem = "UPDATE selleritems SET amount = ? WHERE itemname = ? AND seller_id = ?";
                    preparedStatement = connection.prepareStatement(sqlUpdateItem);
                    preparedStatement.setInt(1, Integer.parseInt(map.get("amount")));
                    preparedStatement.setString(2, itemId);
                    preparedStatement.setString(3, accountId);
                    preparedStatement.executeUpdate();
                    System.out.println("Item updated successfully.");
                } else {
                    // Item does not exist, add a new record
                    String sqlInsertItem = "INSERT INTO selleritems (itemname, cellorrent, amount, seller_id) VALUES (?, ?, ?, ?)";
                    preparedStatement = connection.prepareStatement(sqlInsertItem);
                    preparedStatement.setString(1, itemId);
                    preparedStatement.setString(2, map.get("sellorbuy"));
                    preparedStatement.setInt(3, Integer.parseInt(map.get("amount")));
                    preparedStatement.setString(4, accountId);
                    preparedStatement.executeUpdate();
                    System.out.println("Item added successfully.");
                }
            } else {
                // Seller account does not exist
                System.out.println("Seller account with ID " + accountId + " does not exist.");
            }

            // Commit the transaction
            connection.commit();
        } catch (SQLException e) {
            // Rollback the transaction in case of exception
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            // Close resources and set auto-commit back to true
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.setAutoCommit(true);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void DeleteItems(String accountId, HashMap<String, String> map) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            // Get connection from singleton class
            connection = MySQLConnectionSingleton.getInstance();

            // Set auto-commit to false to start a transaction
            connection.setAutoCommit(false);

            // Check if seller account exists
            String sqlCheckSeller = "SELECT id FROM seller WHERE id = ?";
            preparedStatement = connection.prepareStatement(sqlCheckSeller);
            preparedStatement.setString(1, accountId);
            resultSet = preparedStatement.executeQuery();
            boolean sellerExists = resultSet.next();

            if (sellerExists) {
                // Seller account exists, check if item exists for the seller
                String itemId = map.get("itemId");
                String sqlCheckItem = "SELECT id FROM selleritems WHERE id = ? AND seller_id = ?";
                preparedStatement = connection.prepareStatement(sqlCheckItem);
                preparedStatement.setString(1, itemId);
                preparedStatement.setString(2, accountId);
                resultSet = preparedStatement.executeQuery();
                boolean itemExists = resultSet.next();

                if (itemExists) {
                    // Item exists, update the status to 0
                    String sqlUpdateStatus = "UPDATE selleritems SET status = 0 WHERE id = ?";
                    preparedStatement = connection.prepareStatement(sqlUpdateStatus);
                    preparedStatement.setString(1, itemId);
                    preparedStatement.executeUpdate();
                    System.out.println("Item status updated to 0 successfully.");
                } else {
                    // Item does not exist
                    System.out.println("Item with ID " + itemId + " does not exist.");
                }
            } else {
                // Seller account does not exist
                System.out.println("Seller account with ID " + accountId + " does not exist.");
            }

            // Commit the transaction
            connection.commit();
        } catch (SQLException e) {
            // Rollback the transaction in case of exception
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            // Close resources and set auto-commit back to true
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.setAutoCommit(true);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public static void addOrderGoodsItems(String accountId, String goodsId) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            // Get connection from singleton class
            connection = MySQLConnectionSingleton.getInstance();

            // Set auto-commit to false to start a transaction
            connection.setAutoCommit(false);

            // Insert order for goods
            String sqlInsertOrder = "INSERT INTO goodsorder (managers_id, orderItem, date) VALUES (?, ?, ?)";
            preparedStatement = connection.prepareStatement(sqlInsertOrder);
            preparedStatement.setString(1, accountId);
            preparedStatement.setString(2, goodsId);
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            preparedStatement.setTimestamp(3, timestamp);
            preparedStatement.executeUpdate();
            System.out.println("Order for goods added successfully.");

            // Commit the transaction
            connection.commit();
        } catch (SQLException e) {
            // Rollback the transaction in case of exception
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            // Close resources and set auto-commit back to true
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.setAutoCommit(true);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void addInventoryItems(String accountId, String itemId) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            // Get connection from singleton class
            connection = MySQLConnectionSingleton.getInstance();

            // Set auto-commit to false to start a transaction
            connection.setAutoCommit(false);

            // Insert inventory item for the seller
            String sqlInsertItem = "INSERT INTO inventory (clarkid, itemid) VALUES (?, ?)";
            preparedStatement = connection.prepareStatement(sqlInsertItem);
            preparedStatement.setString(1, accountId);
            preparedStatement.setString(2, itemId);
            preparedStatement.executeUpdate();
            System.out.println("Inventory item added successfully.");

            // Commit the transaction
            connection.commit();
        } catch (SQLException e) {
            // Rollback the transaction in case of exception
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            // Close resources and set auto-commit back to true
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.setAutoCommit(true);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized static void addCustomerOrderItems(String accountId, HashMap<String, String> map) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            // Establish database connection
            connection = MySQLConnectionSingleton.getInstance();

            // Begin transaction
            connection.setAutoCommit(false);

            // Check if inventory_id exists
            String itemId = map.get("itemid");
            String checkInventoryQuery = "SELECT id FROM inventory WHERE itemid = ?";
            preparedStatement = connection.prepareStatement(checkInventoryQuery);
            preparedStatement.setString(1, itemId);
            ResultSet resultSet = preparedStatement.executeQuery();

            int inventoryId = -1; // Initialize with a negative value
            if (resultSet.next()) {
                inventoryId = resultSet.getInt("id");
            } else {
                // If inventory_id doesn't exist, handle accordingly
                System.out.println("Inventory item with id " + itemId + " does not exist.");
                return;
            }

            // Lock the inventory record
            String lockInventoryQuery = "SELECT * FROM inventory WHERE id = ? FOR UPDATE";
            preparedStatement = connection.prepareStatement(lockInventoryQuery);
            preparedStatement.setInt(1, inventoryId);
            preparedStatement.executeQuery(); // Execute query just to lock the row


            // Lock the order table
            String lockOrderTableQuery = "LOCK TABLES `order` WRITE";
            preparedStatement = connection.prepareStatement(lockOrderTableQuery);
            preparedStatement.executeUpdate();

            // Update the inventory record to set reserved = 1
            String updateInventoryQuery = "UPDATE inventory SET reserved = 1 WHERE id = ?";
            preparedStatement = connection.prepareStatement(updateInventoryQuery);
            preparedStatement.setInt(1, inventoryId);
            preparedStatement.executeUpdate();


            // Insert order record
            String insertOrderQuery = "INSERT INTO `order` (inventory_id, customerid, date, paymentmethod) VALUES (?, ?, NOW(), ?)";
            preparedStatement = connection.prepareStatement(insertOrderQuery);
            preparedStatement.setInt(1, inventoryId);
            preparedStatement.setString(2, accountId);
            preparedStatement.setString(3, map.get("paymentMethod"));
            preparedStatement.executeUpdate();

            // Commit transaction
            connection.commit();

            // Release locks
            connection.setAutoCommit(true);
            String releaseLocksQuery = "UNLOCK TABLES";
            preparedStatement = connection.prepareStatement(releaseLocksQuery);
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            // Handle any SQL errors
            if (connection != null) {
                try {
                    connection.rollback(); // Rollback transaction if any error occurs
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
            e.printStackTrace();
        } finally {
            // Close PreparedStatement and Connection
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static List<String> AllAvailableItems() {
        List<String> availableItems = new ArrayList<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {

            connection = MySQLConnectionSingleton.getInstance();

            // Prepare and execute query to retrieve available items
            String query = "SELECT itemid FROM inventory WHERE reserved = 0";
            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();

            // Iterate through the result set and add item IDs to the list
            while (resultSet.next()) {
                String itemId = resultSet.getString("itemid");
                availableItems.add(itemId);
            }

        } catch (SQLException e) {
            // Handle any SQL errors
            e.printStackTrace();
        } finally {
            // Close ResultSet, PreparedStatement, and Connection
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        // Return the list of available item IDs
        return availableItems;
    }
}
