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


public class DeleteSellerItemsImpl extends DeleteItemServiceGrpc.DeleteItemServiceImplBase implements DistributedTxListener {
    private final MainServer mainServer;

    DeleteItemServiceGrpc.DeleteItemServiceBlockingStub deleteItemServiceBlockingStub = null;
    private ManagedChannel channel = null;
    private volatile Pair<String, HashMap<String, String>> tempDataHolderForItem;
    private boolean transactionStatus = false;

    public DeleteSellerItemsImpl(MainServer mainServer) {
        this.mainServer = mainServer;
    }


    @Override
    public void deleteItem(DeleteItemRequest request, io.grpc.stub.StreamObserver<DeleteItemResponse> responseObserver) {

        String sellerId = request.getSellerId();
        String itemId = request.getItemId();
        String status = Boolean.toString(request.getStatus());

        HashMap<String, String> itemsdetailsfordelete = new HashMap<String, String>();
        itemsdetailsfordelete.put("sellerId", sellerId);
        itemsdetailsfordelete.put("amount", itemId);
        itemsdetailsfordelete.put("status", status);


        if (mainServer.isLeader()) {
            // Act as primary
            try {
                System.out.println("deleting  items as  Primary");
                startDistributedTx(sellerId, itemsdetailsfordelete);
                updateSecondaryServers(sellerId, itemsdetailsfordelete);
                System.out.println("going to perform deleting");

                ((DistributedTxCoordinator) mainServer.getTransaction()).perform();
                //    } else {
                //    ((DistributedTxCoordinator) server.getTransaction()).sendGlobalAbort();
                //    }
            } catch (Exception e) {
                System.out.println("Error while deleting the Items" + e.getMessage());
                e.printStackTrace();
            }
        } else {
//             Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("confirming  deleting details  on secondary, on Primary's command");
                startDistributedTx(sellerId, itemsdetailsfordelete);
                if (true) {//Boolean.getBoolean(status)
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteAbort();
                }
            } else {

                DeleteItemResponse response = callPrimaryForDeletion(sellerId, itemsdetailsfordelete);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }

        DeleteItemResponse responseitem = DeleteItemResponse.newBuilder().setStatus(transactionStatus).build();


        responseObserver.onNext(responseitem);
        responseObserver.onCompleted();
    }




    private DeleteItemResponse callPrimaryForDeletion(String accountId, HashMap<String, String> map) {
        System.out.println("Calling Primary server ");
        String[] currentLeaderData = mainServer.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServerForDelete(accountId, map, false, IPAddress, port);
    }


    @Override
    public void onGlobalCommit() {
        updateAddItems();

    }

    private void updateAddItems() {
        if (tempDataHolderForItem != null) {
            String accountId = tempDataHolderForItem.getKey();
            HashMap<String, String> value = tempDataHolderForItem.getValue();
            mainServer.setDeleteItemStatus(accountId, value);
            System.out.println("Account " + accountId + " updated Item status  ");
            tempDataHolderForItem = null;
        }
    }


    @Override
    public void onGlobalAbort() {
        tempDataHolderForItem = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    private void startDistributedTx(String accountId, HashMap<String, String> map) {
        try {
            mainServer.getTransaction().start(accountId, String.valueOf(UUID.randomUUID()));
//            DBTransaction.sellerLogIn(accountId, password);
            tempDataHolderForItem = new Pair<>(accountId, map);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateSecondaryServers(String accountId, HashMap<String, String> map) throws KeeperException, InterruptedException {
        System.out.println("Updating deletion for secondary servers");
        List<String[]> othersData = mainServer.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServerForDelete(accountId, map, true, IPAddress, port);
        }
    }



    private DeleteItemResponse callServerForDelete(String accountId, HashMap<String, String> map, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();

        deleteItemServiceBlockingStub = DeleteItemServiceGrpc.newBlockingStub(channel);

        DeleteItemRequest deleteItemRequest = DeleteItemRequest.newBuilder().setSellerId(accountId).setItemId(map.get("itemId"))
//                .setStatus(map.get("status"))
                .build();
        DeleteItemResponse deleteItemResponse = deleteItemServiceBlockingStub.deleteItem(deleteItemRequest);
        return deleteItemResponse;
    }


}
