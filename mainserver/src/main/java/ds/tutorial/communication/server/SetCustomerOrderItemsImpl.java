package ds.tutorial.communication.server;


import ds.tutorial.communication.grpc.generated.*;
import ds.tutorials.sycnhronization.DistributedTxCoordinator;
import ds.tutorials.sycnhronization.DistributedTxListener;
import ds.tutorials.sycnhronization.DistributedTxParticipant;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;


public class SetCustomerOrderItemsImpl extends ReservationServiceGrpc.ReservationServiceImplBase implements DistributedTxListener {
    private final MainServer mainServer;

    ReservationServiceGrpc.ReservationServiceBlockingStub  reservationServiceBlockingStub = null;

    private ManagedChannel channel = null;
    private volatile Pair<String, HashMap<String , String>> tempDataHolderForItem;
    private boolean transactionStatus = false;

    public SetCustomerOrderItemsImpl(MainServer mainServer) {
        this.mainServer = mainServer;
    }


    @Override
    public void reserve(ReserveRequest request, io.grpc.stub.StreamObserver<ReserveResponse> responseObserver) {


        String customerId = request.getCustomerId();
        String itemid = request.getItemId();
        String paymentMethod = request.getPaymentMethod();

        HashMap<String, String> orderdetailsmap = new HashMap<>();
        orderdetailsmap.put("itemid" , itemid);
        orderdetailsmap.put("paymentMethod" , paymentMethod);



        if (mainServer.isLeader()) {
            // Act as primary
            try {
                System.out.println("placing customer order items as  Primary");
                startDistributedTx(customerId, orderdetailsmap);
                updateSecondaryServers(customerId, orderdetailsmap);
                System.out.println("going to perform");

                ((DistributedTxCoordinator) mainServer.getTransaction()).perform();
                //    } else {
                //    ((DistributedTxCoordinator) server.getTransaction()).sendGlobalAbort();
                //    }
            } catch (Exception e) {
                System.out.println("Error while updating  customer order items   Items" + e.getMessage());
                e.printStackTrace();
            }
        } else {
//             Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("confirming adding  customer order items    details  on secondary, on Primary's command");
                startDistributedTx(customerId, orderdetailsmap);
                if (orderdetailsmap.get("itemid") != null) {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteAbort();
                }
            } else {
                ReserveResponse response = callPrimary(customerId, orderdetailsmap);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }
        ReserveResponse response = ReserveResponse.newBuilder().setStatus(transactionStatus).build();



        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    private ReserveResponse callPrimary(String accountId, HashMap<String , String> map) {
        System.out.println("Calling Primary server ");
        String[] currentLeaderData = mainServer.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(accountId, map, false, IPAddress, port);
    }


    @Override
    public void onGlobalCommit() {
        updateAddItems();

    }

    private void updateAddItems() {
        if (tempDataHolderForItem != null) {
            String accountId = tempDataHolderForItem.getKey();
            HashMap<String, String> map = tempDataHolderForItem.getValue();
            mainServer.setCustomerOrderItemStatus(accountId, map);
            System.out.println("Account " + accountId + " updated customer orders  status  ");
            tempDataHolderForItem = null;
        }
    }


    @Override
    public void onGlobalAbort() {
        tempDataHolderForItem = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    private void startDistributedTx(String accountId, HashMap<String, String> ordermap) {
        try {
            mainServer.getTransaction().start(accountId, String.valueOf(UUID.randomUUID()));
//            DBTransaction.sellerLogIn(accountId, password);
            tempDataHolderForItem = new Pair<>(accountId, ordermap);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateSecondaryServers(String accountId, HashMap<String, String> ordermap) throws KeeperException, InterruptedException {
        System.out.println("Updating deletion for secondary servers");
        List<String[]> othersData = mainServer.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(accountId, ordermap, true, IPAddress, port);
        }
    }

    private ReserveResponse callServer(String accountId, HashMap<String, String> ordermap, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();

        reservationServiceBlockingStub = ReservationServiceGrpc.newBlockingStub(channel);
        ReserveRequest request = ReserveRequest.newBuilder()
                .setCustomerId(accountId)
                .setItemId(ordermap.get(""))
                .setPaymentMethod(ordermap.get(""))
                .build();
        ReserveResponse response = reservationServiceBlockingStub.reserve(request);
        return response;

    }


}
