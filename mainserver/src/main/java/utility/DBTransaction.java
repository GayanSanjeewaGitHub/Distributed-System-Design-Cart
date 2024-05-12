package utility;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.sql.SQLException;
import java.util.HashMap;

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
}
