package ds.tutorial.communication.server;


import ds.tutorial.communication.grpc.generated.AllAvailableItemResponse;
import ds.tutorial.communication.grpc.generated.AllAvailableItemServiceGrpc;
import ds.tutorial.communication.grpc.generated.Empty;
import io.grpc.ManagedChannel;
import utility.DBTransaction;

import java.util.List;


public class GetAvailableItemsForCustomerImpl extends AllAvailableItemServiceGrpc.AllAvailableItemServiceImplBase {
    private final MainServer mainServer;


    private final ManagedChannel channel = null;

    private final boolean transactionStatus = false;

    public GetAvailableItemsForCustomerImpl(MainServer mainServer) {
        this.mainServer = mainServer;
    }


    @Override
    public void getAllAvailableItems(Empty request, io.grpc.stub.StreamObserver<AllAvailableItemResponse> responseObserver) {

        List<String> list = DBTransaction.AllAvailableItems();
        AllAvailableItemResponse response = AllAvailableItemResponse.newBuilder()
                .addAllItems(list)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
