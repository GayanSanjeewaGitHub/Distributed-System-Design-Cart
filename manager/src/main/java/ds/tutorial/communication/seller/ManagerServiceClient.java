package ds.tutorial.communication.seller;


import java.util.Scanner;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import ds.tutorial.communication.grpc.generated.*;


import java.sql.Statement;

class ManagerServiceClient {

    FactoryGoodsServiceGrpc.FactoryGoodsServiceBlockingStub factoryGoodsServiceBlockingStub = null;

    String host = null;
    int port = -1;
    String mode;
    private ManagedChannel channel = null;

    public ManagerServiceClient(String host, int port, String mode) {
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


        ManagerServiceClient client = new ManagerServiceClient(host, port, mode);
        client.initializeConnection();
        client.processUserRequests();
        client.closeConnection();

    }


    private void initializeConnection() {
        System.out.println("Initializing Connecting to server at " + host + ":" + port);
        channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        factoryGoodsServiceBlockingStub = FactoryGoodsServiceGrpc.newBlockingStub(channel);


    }

    private void closeConnection() {
        channel.shutdown();
    }

    private void processUserRequests() throws InterruptedException {

        while (true) {
            if (mode.equals("order")) {
                Scanner userInput = new Scanner(System.in);
                System.out.println("\nEnter Manager Id , OrderItem Id(goodsid) ,amount that need to order   :");
                String managerId = userInput.nextLine().trim();
                String goodsid = userInput.nextLine().trim();
                System.out.println("Requesting server to place order    " + goodsid);
                FactoryGoodsRequest order = FactoryGoodsRequest.newBuilder().setManagerId(managerId).setGoodsId(goodsid).build();

                FactoryGoodsResponse factoryGoodsResponse = factoryGoodsServiceBlockingStub.orderGoods(order);


                if (factoryGoodsResponse.getStatus()) {
                    System.out.printf(" Order placed   Success for  Goods Id " + goodsid);
                } else {
                    System.out.printf(" Order placed  not  Success for  Goods Id " + goodsid);
                }
                Thread.sleep(1000);

            }

        }
    }
}

