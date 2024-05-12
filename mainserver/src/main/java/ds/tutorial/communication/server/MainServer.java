package ds.tutorial.communication.server;

import ds.tutorials.sycnhronization.DistributedLock;
import ds.tutorials.sycnhronization.DistributedTx;
import ds.tutorials.sycnhronization.DistributedTxCoordinator;
import ds.tutorials.sycnhronization.DistributedTxParticipant;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.KeeperException;
import utility.DBTransaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MainServer {
    DistributedTx transaction;
    //   SetBalanceServiceImpl setBalanceService;
//   BalanceServiceImpl checkBalanceService;
    SetSellerLoginImpl setSellerLoginService;
    private final DistributedLock leaderLock;
    private final int serverPort;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private byte[] leaderData;
    private final Map<String, Double> accounts = new HashMap();

    public MainServer(String host, int port) throws InterruptedException, IOException, KeeperException {
        this.serverPort = port;
        leaderLock = new DistributedLock("MainServerTestCluster", buildServerData(host, port));
        setSellerLoginService = new SetSellerLoginImpl(this);

        transaction = new DistributedTxParticipant(setSellerLoginService);
    }

    public static void main(String[] args) throws Exception {
        DistributedLock.setZooKeeperURL("localhost:2181");
        DistributedTx.setZooKeeperURL("localhost:2181");
        int serverPort;
        if (args.length != 1) {
            System.out.println("Usage MainServer <port>");
            System.exit(1);
        }
        serverPort = Integer.parseInt(args[0].trim());
        MainServer mainServer = new MainServer("localhost", serverPort);
        mainServer.startServer();
    }

    public static String buildServerData(String IP, int port) {
        return IP + ":" + port;
    }

    public synchronized String[] getCurrentLeaderData() {
        return new String(leaderData).split(":");
    }

    private synchronized void setCurrentLeaderData(byte[] leaderData) {
        this.leaderData = leaderData;
    }

    public void startServer() throws IOException, InterruptedException, KeeperException {
        Server server = ServerBuilder
                .forPort(serverPort)
                .addService(setSellerLoginService)
                .build();
        server.start();
        System.out.println("Main Started and ready to accept requests on port " + serverPort);
        tryToBeLeader();
        server.awaitTermination();
    }

    private void tryToBeLeader() throws KeeperException, InterruptedException {
        Thread leaderCampaignThread = new Thread(new LeaderCampaignThread());
        leaderCampaignThread.start();
    }

    public void setLoginStatus(String accountId, String password) {
        DBTransaction.sellerLogIn(accountId, password);
    }

    private void beTheLeader() {
        System.out.println("I got the leader lock. Now acting as primary");
        isLeader.set(true);
        transaction = new DistributedTxCoordinator(setSellerLoginService);
    }

    public boolean isLeader() {
        return isLeader.get();
    }

    public DistributedTx getTransaction() {
        return transaction;
    }

    public List<String[]> getOthersData() throws KeeperException, InterruptedException {
        List<String[]> result = new ArrayList<>();
        List<byte[]> othersData = leaderLock.getOthersData();

        for (byte[] data : othersData) {
            String[] dataStrings = new String(data).split(":");
            result.add(dataStrings);
        }
        return result;
    }

    public void setItemStatus(String accountId, HashMap<String, String> map) {
        DBTransaction.addOrUpdateItems(accountId, map);
    }

    public void setDeleteItemStatus(String accountId, HashMap<String, String> map) {
        DBTransaction.DeleteItems(accountId, map);
    }

    //
    class LeaderCampaignThread implements Runnable {
        private byte[] currentLeaderData = null;

        @Override
        public void run() {
            System.out.println("Starting the leader Campaign");

            try {
                boolean leader = leaderLock.tryAcquireLock();

                while (!leader) {
                    byte[] leaderData = leaderLock.getLockHolderData();
                    if (currentLeaderData != leaderData) {
                        currentLeaderData = leaderData;
                        setCurrentLeaderData(currentLeaderData);
                    }
                    Thread.sleep(10000);
                    leader = leaderLock.tryAcquireLock();
                }

                currentLeaderData = null;
                beTheLeader();
            } catch (Exception e) {
            }
        }
    }
}

