package dlt.load.balancer.model;

import dlt.client.tangle.hornet.enums.TransactionType;
import dlt.client.tangle.hornet.model.transactions.LBMultiDevice;
import dlt.client.tangle.hornet.model.transactions.LBMultiDeviceResponse;
import dlt.client.tangle.hornet.model.transactions.Reply;
import dlt.client.tangle.hornet.model.transactions.Request;
import dlt.client.tangle.hornet.model.transactions.TargetedTransaction;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Uellington Damasceno
 */
public class WaitingLBRequestState extends AbstractBalancerState {

    private static final Logger logger = Logger.getLogger(WaitingLBRequestState.class.getName());

    public WaitingLBRequestState(Balancer balancer) {
        super(balancer);
    }
    
    @Override
    public void onEnter() {
        Long LBRequestTimeWaiting = this.balancer.getLBRequestTimeWaiting();
        this.balancer.scheduleTimeout(LBRequestTimeWaiting);
    }

    @Override
    protected boolean isValidTransaction(Transaction transaction) {
        return transaction.is(TransactionType.LB_REQUEST) 
                || transaction.is(TransactionType.LB_MULTI_DEVICE_REQUEST);
    }

    @Override
    protected void handleInvalidTransaction(Transaction trans) {
        logger.info("Acceptable trans: LB_REQUEST or LB_MULTI_DEVICE_REQUEST.");
    }

    @Override
    protected void handleValidTransaction(Transaction transaction) {
        if (!((TargetedTransaction) transaction).isSameTarget(source)) {
            return;
        }
        
        String device = transaction.isMultiLayerTransaction() 
                ? ((LBMultiDevice) transaction).getDevice()
                : ((Request) transaction).getDevice();
        
        this.balancer.receiveNewDevice(device);
        String transactionSender = transaction.getSource();
        
        Transaction reply = transaction.isMultiLayerTransaction()
                ? new LBMultiDeviceResponse(source, group, device, transactionSender)
                : new Reply(source, group, transactionSender);
        
        this.balancer.sendTransaction(reply);
        logger.log(Level.INFO, "{0} Sended.", reply.getType());
        this.balancer.transitionTo(new IdleState(balancer));
    }
    
    
}
