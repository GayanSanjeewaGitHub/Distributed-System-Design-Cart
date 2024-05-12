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

public class SetSellerItemsImpl extends AddItemServiceGrpc.AddItemServiceImplBase implements DistributedTxListener {
    private final MainServer mainServer;
    AddItemServiceGrpc.AddItemServiceBlockingStub addItemServiceBlockingStub = null;
    DeleteItemServiceGrpc.DeleteItemServiceBlockingStub deleteItemServiceBlockingStub = null;
    private ManagedChannel channel = null;
    private volatile Pair<String, HashMap<String, String>> tempDataHolderForItem;
    private boolean transactionStatus = false;

    public SetSellerItemsImpl(MainServer mainServer) {
        this.mainServer = mainServer;
    }


    @Override
    public void addItem(AddItemRequest request, io.grpc.stub.StreamObserver<AddItemResponse> responseObserver) {


        String sellerId = request.getSellerId();
        String itemId = request.getItemId();
        int amount = (int) request.getAmount();
        String sellorbuy = request.getSellOrBuy();

        HashMap<String, String> itemsdetails = new HashMap<String, String>();
        itemsdetails.put("sellerId", sellerId);
        itemsdetails.put("amount", itemId);
        itemsdetails.put("sellorbuy", sellorbuy);
        itemsdetails.put("sellerId", Integer.toString(amount));


        if (mainServer.isLeader()) {
            // Act as primary
            try {
                System.out.println("adding items as  Primary");
                startDistributedTx(sellerId, itemsdetails);
                updateSecondaryServers(sellerId, itemsdetails);
                System.out.println("going to perform");

                ((DistributedTxCoordinator) mainServer.getTransaction()).perform();
                //    } else {
                //    ((DistributedTxCoordinator) server.getTransaction()).sendGlobalAbort();
                //    }
            } catch (Exception e) {
                System.out.println("Error while updating the Items" + e.getMessage());
                e.printStackTrace();
            }
        } else {
//             Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("confirming  loggin details  on secondary, on Primary's command");
                startDistributedTx(sellerId, itemsdetails);
                if (amount >= 0) {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteAbort();
                }
            } else {
                AddItemResponse response = callPrimary(sellerId, itemsdetails);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }

        AddItemResponse responseitem = AddItemResponse.newBuilder().setStatus(transactionStatus).build();


        responseObserver.onNext(responseitem);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteItem(DeleteItemRequest request, io.grpc.stub.StreamObserver<DeleteItemResponse> responseObserver) {

        String sellerId = request.getSellerId();
        String itemId = request.getItemId();
        String status = request.getStatus();

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
                startDistributedTx(sellerId, itemsdetails);
                if (Boolean.getBoolean(status) == True) {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteAbort();
                }
            } else {

                AddItemResponse response = callPrimaryForDeletion(sellerId, itemsdetails);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }

        AddItemResponse responseitem = AddItemResponse.newBuilder().setStatus(transactionStatus).build();


        responseObserver.onNext(responseitem);
        responseObserver.onCompleted();
    }


    private AddItemResponse callPrimary(String accountId, HashMap<String, String> map) {
        System.out.println("Calling Primary server ");
        String[] currentLeaderData = mainServer.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(accountId, map, false, IPAddress, port);
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
            mainServer.setItemStatus(accountId, value);
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
            callServer(accountId, map, true, IPAddress, port);
        }
    }

    private AddItemResponse callServer(String accountId, HashMap<String, String> map, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();

        if (map.get("status") == null) {
            addItemServiceBlockingStub = AddItemServiceGrpc.newBlockingStub(channel);

            AddItemRequest addItemRequest = AddItemRequest.newBuilder()
                    .setSellerId(accountId)
                    .setItemId(map.get("itemId "))
                    .setAmount(Long.parseLong(map.get("amount")))
                    .setSellOrBuy(map.get("sellorbuy"))
                    .build();

            AddItemResponse addItemResponse = addItemServiceBlockingStub.addItem(addItemRequest);
            return addItemResponse;
        }else{
            deleteItemServiceBlockingStub = DeleteItemServiceGrpc.newBlockingStub(channel);

            DeleteItemRequest deleteItemRequest = DeleteItemRequest.newBuilder()
                    .setSellerId(accountId)
                    .setItemId(map.get("itemId"))
                    .setStatus(map.get("status"))
                    .build();
           DeleteItemResponse deleteItemResponse = deleteItemServiceBlockingStub.deleteItem(deleteItemRequest);
           deleteItemParser(deleteItemResponse);

        }

        return null;
    }

    private DeleteItemResponse   callServerForDelete(String accountId, HashMap<String, String> map, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();

            deleteItemServiceBlockingStub = DeleteItemServiceGrpc.newBlockingStub(channel);

            DeleteItemRequest deleteItemRequest = DeleteItemRequest.newBuilder()
                    .setSellerId(accountId)
                    .setItemId(map.get("itemId"))
                    .setStatus(map.get("status"))
                    .build();
            DeleteItemResponse deleteItemResponse = deleteItemServiceBlockingStub.deleteItem(deleteItemRequest);
        return deleteItemResponse;
    }




}
