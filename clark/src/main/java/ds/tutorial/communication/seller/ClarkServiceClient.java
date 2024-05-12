package ds.tutorial.communication.seller;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

import ds.utility.DatabaseManager;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import ds.tutorial.communication.grpc.generated.*;


import java.sql.Statement;

class ClarkServiceClient {
    InventryAddServiceGrpc.InventryAddServiceBlockingStub inventryAddServiceBlockingStub = null;

    String host = null;
    int port = -1;
    String mode;
    private ManagedChannel channel = null;

    public ClarkServiceClient(String host, int port, String mode) {
        this.host = host;
        this.port = port;
        this.mode = mode;
    }

    private static void loggingProcess(String userId, String password) {
        try {

            Connection con = DatabaseManager.getInstance().getConnection();
            Statement stmt = con.createStatement();


            String sqlCheckUser = "SELECT id FROM seller WHERE id = '" + userId + "'";
            boolean userExists = stmt.executeQuery(sqlCheckUser).next();

            if (userExists) {
                // User exists, update password and status
                String sqlUpdate = "UPDATE seller SET password = '" + password + "', status = 1 WHERE id = '" + userId + "'";
                stmt.executeUpdate(sqlUpdate);
                System.out.println("User updated successfully.");

            } else {
                // User does not exist, create new user
                String sqlInsert = "INSERT INTO seller (id, password, status) VALUES ('" + userId + "', '" + password + "', 1)";
                stmt.executeUpdate(sqlInsert);
                System.out.println("User created successfully.");
            }

            stmt.close();
            con.close();


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void logoutProcess(String userId) {
        try {
            Connection con = DatabaseManager.getInstance().getConnection();
            Statement stmt = con.createStatement();

            String sqlCheckUser = "SELECT id FROM seller WHERE id = '" + userId + "'";
            boolean userExists = stmt.executeQuery(sqlCheckUser).next();

            if (userExists) {
                // User exists, update status to 0 (logged out)
                String sqlUpdate = "UPDATE seller SET status = 0 WHERE id = '" + userId + "'";
                stmt.executeUpdate(sqlUpdate);
                System.out.println("User logged out successfully.");
            } else {

                System.out.println("User does not exist.");
            }

            stmt.close();
            con.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean isloggedin(String userId) {
        try {
            Connection con = DatabaseManager.getInstance().getConnection();
            Statement stmt = con.createStatement();

            String sqlCheckUser = "SELECT id, status FROM seller WHERE id = '" + userId + "'";
            ResultSet rs = stmt.executeQuery(sqlCheckUser);

            if (rs.next()) {
                int status = rs.getInt("status");
                if (status == 1) {
                    System.out.println("User session is available .");
                    return true;
                } else {
                    System.out.println("User is not logged in.");
                    return false;
                }
            } else {
                System.out.println("User does not exist.");
                return false;
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }


    private static void startSellingProcess(String sellerId, String itemId, String sellOrBuy, int amount) {
        try {
            Connection con = DatabaseManager.getInstance().getConnection();
            Statement stmt = con.createStatement();

            // Check if the record already exists
            String sqlCheckRecord = "SELECT id FROM selleritems WHERE seller_id = '" + sellerId + "' AND itemname = '" + itemId + "'";
            boolean recordExists = stmt.executeQuery(sqlCheckRecord).next();

            if (recordExists) {
                // Record exists, update it
                String sqlUpdateRecord = "UPDATE selleritems SET amount = amount + " + amount + ", cellorrent = '" + sellOrBuy + "' WHERE seller_id = '" + sellerId + "' AND itemname = '" + itemId + "' AND amount + " + amount + " >= 0";
                int rowsUpdated = stmt.executeUpdate(sqlUpdateRecord);
                if (rowsUpdated > 0) {
                    System.out.println("Record updated successfully.");
                } else {
                    System.out.println("Failed to update record. Minus value not allowed.");
                }
            } else {
                // Record does not exist, insert a new one
                String sqlInsertRecord = "INSERT INTO selleritems (itemname, seller_id, cellorrent, amount) VALUES ('" + itemId + "', '" + sellerId + "', '" + sellOrBuy + "', " + amount + ")";
                stmt.executeUpdate(sqlInsertRecord);
                System.out.println("New record inserted successfully.");
            }

            stmt.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws InterruptedException {
        // if (args[0].equals("login")) {
        //     loggingProcess(args[1], args[2]);
        // } else {
        //     if (args[0].equals("logout")) {
        //         logoutProcess(args[1]);
        //     } else {
        //         boolean isloggedin = isloggedin(args[0]);
        //         if (args.length == 4) {
        //             startSellingProcess(args[0], args[1], args[2], Integer.parseInt(args[3]));
        //         } else {
        //             System.out.println("not enough data to sell ot buy items");
        //         }

        //     }


        // }

        String host = null;
        int port = -1;
        String mode;
        if (args.length != 3) {
            System.out.println("Usage customer ServiceClient <host> <port>");
            System.exit(1);
        }
        host = args[0];
        port = Integer.parseInt(args[1].trim());
        mode = args[2].trim();


        ClarkServiceClient client = new ClarkServiceClient(host, port, mode);
        client.initializeConnection();
        client.processUserRequests();
        client.closeConnection();

    }


    private void initializeConnection() {
        System.out.println("Initializing Connecting to server at " + host + ":" + port);
        channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();

        inventryAddServiceBlockingStub = InventryAddServiceGrpc.newBlockingStub(channel);


    }

    private void closeConnection() {
        channel.shutdown();
    }

    private void processUserRequests() throws InterruptedException {

        while (true) {
            if (mode.equals("add")) {
                Scanner userInput = new Scanner(System.in);
                System.out.println("\nEnter ClarkID , ItemID , Item ID that available   :");
                String clarkid = userInput.nextLine().trim();
                String itemid = userInput.nextLine().trim();
                System.out.println("Requesting server add items  " + itemid);
                InventryAddRequest invenrtyaddrequest = InventryAddRequest.newBuilder().setClarkId(clarkid).setItemId(itemid).build();
                InventryAddResponse inventryAddResponse = inventryAddServiceBlockingStub.addInventry(invenrtyaddrequest);
                if (inventryAddResponse.getStatus()) {
                    System.out.printf(" Inventry added Success for  ItemId " + itemid);
                } else {
                    System.out.printf(" Inventry added  not Success for  ItemId " + itemid);
                }
                Thread.sleep(1000);

            }

        }
    }
}

