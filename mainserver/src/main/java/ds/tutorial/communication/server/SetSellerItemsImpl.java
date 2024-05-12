package ds.tutorial.communication.server;


import ds.tutorial.communication.grpc.generated.*;
import ds.tutorials.sycnhronization.DistributedTxCoordinator;
import ds.tutorials.sycnhronization.DistributedTxListener;
import ds.tutorials.sycnhronization.DistributedTxParticipant;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class SetSellerItemsImpl extends AddItemServiceGrpc.AddItemServiceImplBase implements DistributedTxListener {
    private final MainServer mainServer;
    AddItemServiceGrpc.AddItemServiceBlockingStub addItemServiceBlockingStub = null;
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
        itemsdetails.put("sellerId" ,sellerId);
        itemsdetails.put("amount" ,itemId);
        itemsdetails.put("sellorbuy" ,sellorbuy);
        itemsdetails.put("sellerId" ,Integer.toString(amount));


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
                startDistributedTx(sellerId, itemId);
                if (amount >= 0) {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteAbort();
                }
            } else {
                AddItemResponse response = callPrimary(sellerId, itemId);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }

        AddItemResponse responseitem = AddItemResponse.newBuilder().setStatus(transactionStatus).build();


        responseObserver.onNext(responseitem);
        responseObserver.onCompleted();
    }


    private AddItemResponse callPrimary(String accountId, String Itemid) {
        System.out.println("Calling Primary server ");
        String[] currentLeaderData = mainServer.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(accountId, Itemid, false, IPAddress, port);
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

    private void startDistributedTx(String accountId, HashMap<String ,String> map) {
        try {
            mainServer.getTransaction().start(accountId, String.valueOf(UUID.randomUUID()));
//            DBTransaction.sellerLogIn(accountId, password);
            tempDataHolderForItem = new Pair<>(accountId, map);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateSecondaryServers(String accountId, String password) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = mainServer.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(accountId, password, true, IPAddress, port);
        }
    }

    private AddItemResponse callServer(String accountId, String itemid, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();
        addItemServiceBlockingStub = AddItemServiceGrpc.newBlockingStub(channel);

        AddItemRequest addItemRequest = AddItemRequest.newBuilder()
                .setSellerId(accountId)
                .setItemId(itemid)
                .build();

        AddItemResponse addItemResponse = addItemServiceBlockingStub.addItem(addItemRequest);
        return addItemResponse;
    }

}
