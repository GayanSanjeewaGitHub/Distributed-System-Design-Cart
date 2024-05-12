package ds.tutorial.communication.seller;

import com.google.protobuf.ProtocolStringList;
import ds.tutorial.communication.grpc.generated.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Scanner;

class CustomerServiceClient {

    ReservationServiceGrpc.ReservationServiceBlockingStub reservationServiceBlockingStub = null;

    AllAvailableItemServiceGrpc.AllAvailableItemServiceBlockingStub allAvailableItemServiceBlockingStub = null;

    String host = null;
    int port = -1;
    String mode;
    private ManagedChannel channel = null;

    public CustomerServiceClient(String host, int port, String mode) {
        this.host = host;
        this.port = port;
        this.mode = mode;
    }


    public static void main(String[] args) throws InterruptedException {

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


        CustomerServiceClient client = new CustomerServiceClient(host, port, mode);
        client.initializeConnection();
        client.processUserRequests();
        client.closeConnection();

    }


    private void initializeConnection() {
        System.out.println("Initializing Connecting to server at " + host + ":" + port);
        channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        reservationServiceBlockingStub = ReservationServiceGrpc.newBlockingStub(channel);
        allAvailableItemServiceBlockingStub = AllAvailableItemServiceGrpc.newBlockingStub(channel);


    }

    private void closeConnection() {
        channel.shutdown();
    }

    private void processUserRequests() throws InterruptedException {

        while (true) {
            if (mode.equals("order")) {
                Scanner userInput = new Scanner(System.in);
                System.out.println("\nEnter customer Account ID , orderItem  , and the payment method  'card' or 'cash' (when order recieved) :");
                String customerId = userInput.nextLine().trim();
                String orderItem = userInput.nextLine().trim();
                System.out.println("Requesting server to reserve a item  " + orderItem);

                ReserveRequest request = ReserveRequest.newBuilder().setCustomerId(customerId).setItemId(orderItem).build();

                ReserveResponse reserve = reservationServiceBlockingStub.reserve(request);

                if (reserve.getStatus()) {
                    System.out.printf(" Reservations Success for  ItemId " + orderItem);
                } else {
                    System.out.printf(" Reservations not Success for  ItemId " + orderItem);
                }
                Thread.sleep(1000);

            } else if (mode.equals("view")) {
                Scanner userInput = new Scanner(System.in);
                System.out.println("\nBelow are the currently aviable Items for you   :");

                Empty request = Empty.newBuilder().build();
                AllAvailableItemResponse response = allAvailableItemServiceBlockingStub.getAllAvailableItems(request);

                if (response.getItemsCount() > 0) {
                    ProtocolStringList itemsList = response.getItemsList();
                    for (String item : itemsList) {
                        System.out.println(item);
                    }
                } else {
                    System.out.print(" No items are availble for now to order sorry! ");
                }
                Thread.sleep(1000);

            }

        }
    }
}

