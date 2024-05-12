package ds.tutorial.communication.server;


import ds.tutorial.communication.grpc.generated.LoggingRequest;
import ds.tutorial.communication.grpc.generated.LoggingResponse;
import ds.tutorial.communication.grpc.generated.LoginServiceGrpc;
import ds.tutorials.sycnhronization.DistributedTxCoordinator;
import ds.tutorials.sycnhronization.DistributedTxListener;
import ds.tutorials.sycnhronization.DistributedTxParticipant;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.util.List;
import java.util.UUID;

public class SetSellerLoginImpl extends LoginServiceGrpc.LoginServiceImplBase implements DistributedTxListener {
    LoginServiceGrpc.LoginServiceBlockingStub loginServiceBlockingStub = null;
    private ManagedChannel channel = null;
    private final MainServer mainServer;
    private volatile Pair<String, String> tempDataHolder;
    private boolean transactionStatus = false;

    public SetSellerLoginImpl(MainServer mainServer) {
        this.mainServer = mainServer;
    }


    @Override
    public void login(ds.tutorial.communication.grpc.generated.LoggingRequest request,
                      io.grpc.stub.StreamObserver<ds.tutorial.communication.grpc.generated.LoggingResponse> responseObserver) {

        String accountId = request.getAccountId();
        String password = request.getPassword();


        if (mainServer.isLeader()) {
            // Act as primary
            try {
                System.out.println("Checking loging details as  Primary");
                startDistributedTx(accountId, password);
                updateSecondaryServers(accountId, password);
                System.out.println("going to perform");

                ((DistributedTxCoordinator) mainServer.getTransaction()).perform();
                //    } else {
                //    ((DistributedTxCoordinator) server.getTransaction()).sendGlobalAbort();
                //    }
            } catch (Exception e) {
                System.out.println("Error while updating the account balance" + e.getMessage());
                e.printStackTrace();
            }
        } else {
//             Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("confirming  loggin details  on secondary, on Primary's command");
                startDistributedTx(accountId, password);
                if (password != null) {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant) mainServer.getTransaction()).voteAbort();
                }
            } else {
                LoggingResponse response = callPrimary(accountId, password);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }

        LoggingResponse response = LoggingResponse.newBuilder()
                .setStatus(transactionStatus)
                .build();


        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    private LoggingResponse callPrimary(String accountId, String password) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = mainServer.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(accountId, password, false, IPAddress, port);
    }


    @Override
    public void onGlobalCommit() {
        updateLoggin();

    }

    private void updateLoggin() {
        if (tempDataHolder != null) {
            String accountId = tempDataHolder.getKey();
            String password = tempDataHolder.getValue();
            mainServer.setLoginStatus(accountId, password);
            System.out.println("Account " + accountId + " updated login Status  ");
            tempDataHolder = null;
        }
    }


    @Override
    public void onGlobalAbort() {
        tempDataHolder = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    private void startDistributedTx(String accountId, String password) {
        try {
            mainServer.getTransaction().start(accountId, String.valueOf(UUID.randomUUID()));
//            DBTransaction.sellerLogIn(accountId, password);
            tempDataHolder = new Pair<>(accountId, password);
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

    private LoggingResponse callServer(String accountId, String password, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        loginServiceBlockingStub = LoginServiceGrpc.newBlockingStub(channel);
        LoggingRequest request = LoggingRequest
                .newBuilder()
                .setAccountId(accountId)
                .setPassword(password)
                .setIsSentByPrimary(isSentByPrimary)
                .build();

        LoggingResponse response = loginServiceBlockingStub.login(request);
        return response;
    }

}
