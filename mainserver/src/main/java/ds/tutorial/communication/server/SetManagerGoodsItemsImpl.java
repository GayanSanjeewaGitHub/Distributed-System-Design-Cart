package ds.tutorial.communication.server;


import ds.tutorial.communication.grpc.generated.*;
import ds.tutorials.sycnhronization.DistributedTxCoordinator;
import ds.tutorials.sycnhronization.DistributedTxListener;
import ds.tutorials.sycnhronization.DistributedTxParticipant;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.util.List;
import java.util.UUID;


public class SetManagerGoodsItemsImpl extends FactoryGoodsServiceGrpc.FactoryGoodsServiceImplBase implements DistributedTxListener {
    private final MainServer mainServer;

    FactoryGoodsServiceGrpc.FactoryGoodsServiceBlockingStub  factoryGoodsServiceBlockingStub = null;

    private ManagedChannel channel = null;
    private volatile Pair<String, String> tempDataHolderForItem;
    private boolean transactionStatus = false;

    public SetManagerGoodsItemsImpl(MainServer mainServer) {
        this.mainServer = mainServer;
    }


    @Override
    public void orderGoods(FactoryGoodsRequest request, io.grpc.stub.StreamObserver<FactoryGoodsResponse> responseObserver) {


        String managerId = request.getManagerId();
        String goodsId = request.getGoodsId();


        if (mainServer.isLeader()) {
            // Act as primary
            try {
                System.out.println("adding goods items as  Primary");
                startDistributedTx(managerId, goodsId);
                updateSecondaryServers(managerId, goodsId);
                System.out.println("going to perform");

                ((DistributedTxCoordinator) mainServer.getTransaction()).perform();
                //    } else {
                //    ((DistributedTxCoordinator) server.getTransaction()).sendGlobalAbort();
                //    }
            } catch (Exception e) {
                System.out.println("Error while updating manger order   goods Items" + e.getMessage());
                e.printStackTrace();
            }
        } else {
//             Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("confirming adding manager order goods details  on secondary, on Primary's command");
                startDistributedTx(managerId, goodsId);
                if (goodsId != null) {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteAbort();
                }
            } else {
                FactoryGoodsResponse response = callPrimary(managerId, goodsId);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }

        FactoryGoodsResponse responseitem = FactoryGoodsResponse.newBuilder().setStatus(transactionStatus).build();


        responseObserver.onNext(responseitem);
        responseObserver.onCompleted();
    }


    private FactoryGoodsResponse callPrimary(String accountId, String goodsId) {
        System.out.println("Calling Primary server ");
        String[] currentLeaderData = mainServer.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(accountId, goodsId, false, IPAddress, port);
    }


    @Override
    public void onGlobalCommit() {
        updateAddItems();

    }

    private void updateAddItems() {
        if (tempDataHolderForItem != null) {
            String accountId = tempDataHolderForItem.getKey();
            String ordergoods = tempDataHolderForItem.getValue();
            mainServer.setManagerOrderItemStatus(accountId, ordergoods);
            System.out.println("Account " + accountId + " updated customer order status  ");
            tempDataHolderForItem = null;
        }
    }


    @Override
    public void onGlobalAbort() {
        tempDataHolderForItem = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    private void startDistributedTx(String accountId, String goodsid) {
        try {
            mainServer.getTransaction().start(accountId, String.valueOf(UUID.randomUUID()));
//            DBTransaction.sellerLogIn(accountId, password);
            tempDataHolderForItem = new Pair<>(accountId, goodsid);
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

    private FactoryGoodsResponse callServer(String accountId, String goodsid, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port).usePlaintext().build();

        factoryGoodsServiceBlockingStub= FactoryGoodsServiceGrpc.newBlockingStub(channel);
        FactoryGoodsRequest request = FactoryGoodsRequest.newBuilder()
                .setManagerId(accountId)
                .setGoodsId(goodsid)
                .build();
        FactoryGoodsResponse factoryGoodsResponse = factoryGoodsServiceBlockingStub.orderGoods(request);
        return factoryGoodsResponse;

    }


}
