package ds.tutorial.communication.server;


import ds.tutorial.communication.grpc.generated.AddItemServiceGrpc;
import ds.tutorial.communication.grpc.generated.InventryAddRequest;
import ds.tutorial.communication.grpc.generated.InventryAddResponse;
import ds.tutorial.communication.grpc.generated.InventryAddServiceGrpc;
import ds.tutorials.sycnhronization.DistributedTxCoordinator;
import ds.tutorials.sycnhronization.DistributedTxListener;
import ds.tutorials.sycnhronization.DistributedTxParticipant;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.util.List;
import java.util.UUID;


public class SetClarkItemsImpl extends InventryAddServiceGrpc.InventryAddServiceImplBase implements DistributedTxListener {
    private final MainServer mainServer;

    InventryAddServiceGrpc.InventryAddServiceBlockingStub inventryAddServiceBlockingStub = null;
    AddItemServiceGrpc.AddItemServiceBlockingStub addItemServiceBlockingStub = null;

    private ManagedChannel channel = null;
    private volatile Pair<String, String> tempDataHolderForItem;
    private boolean transactionStatus = false;

    public SetClarkItemsImpl(MainServer mainServer) {
        this.mainServer = mainServer;
    }


    @Override
    public void addInventry(InventryAddRequest request, io.grpc.stub.StreamObserver<InventryAddResponse> responseObserver) {


        String clarkId = request.getClarkId();
        String itemId = request.getItemId();


        if (mainServer.isLeader()) {
            // Act as primary
            try {
                System.out.println("adding inventry items as  Primary");
                startDistributedTx(clarkId, itemId);
                updateSecondaryServers(clarkId, itemId);
                System.out.println("going to perform");

                ((DistributedTxCoordinator) mainServer.getTransaction()).perform();
                //    } else {
                //    ((DistributedTxCoordinator) server.getTransaction()).sendGlobalAbort();
                //    }
            } catch (Exception e) {
                System.out.println("Error while updating the Inventory Items" + e.getMessage());
                e.printStackTrace();
            }
        } else {
//             Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("confirming adding inventry details  on secondary, on Primary's command");
                startDistributedTx(clarkId, itemId);
                if (itemId != null) {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteAbort();
                }
            } else {
                InventryAddResponse response = callPrimary(clarkId, itemId);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }

        InventryAddResponse responseitem = InventryAddResponse.newBuilder().setStatus(transactionStatus).build();


        responseObserver.onNext(responseitem);
        responseObserver.onCompleted();
    }


    private InventryAddResponse callPrimary(String accountId, String itemid) {
        System.out.println("Calling Primary server ");
        String[] currentLeaderData = mainServer.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(accountId, itemid, false, IPAddress, port);
    }


    @Override
    public void onGlobalCommit() {
        updateAddItems();

    }

    private void updateAddItems() {
        if (tempDataHolderForItem != null) {
            String accountId = tempDataHolderForItem.getKey();
            String itemid = tempDataHolderForItem.getValue();
            mainServer.setInventryItemStatus(accountId, itemid);
            System.out.println("Account " + accountId + " updated Item status  ");
            tempDataHolderForItem = null;
        }
    }


    @Override
    public void onGlobalAbort() {
        tempDataHolderForItem = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    private void startDistributedTx(String accountId, String itemid) {
        try {
            mainServer.getTransaction().start(accountId, String.valueOf(UUID.randomUUID()));
//            DBTransaction.sellerLogIn(accountId, password);
            tempDataHolderForItem = new Pair<>(accountId, itemid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateSecondaryServers(String accountId, String itemid) throws KeeperException, InterruptedException {
        System.out.println("Updating deletion for secondary servers");
        List<String[]> othersData = mainServer.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(accountId, itemid, true, IPAddress, port);
        }
    }

    private InventryAddResponse callServer(String accountId, String itemid, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();

        inventryAddServiceBlockingStub = InventryAddServiceGrpc.newBlockingStub(channel);
        InventryAddRequest inventryItemRequest = InventryAddRequest.newBuilder().setItemId(itemid)
                .setClarkId(accountId)
                .build();

        InventryAddResponse inventryAddResponse = inventryAddServiceBlockingStub.addInventry(inventryItemRequest);


        return inventryAddResponse;

    }


}
