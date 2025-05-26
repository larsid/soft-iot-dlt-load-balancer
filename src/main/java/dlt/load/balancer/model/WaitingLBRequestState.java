package dlt.load.balancer.model;

import dlt.client.tangle.hornet.enums.TransactionType;
import dlt.client.tangle.hornet.model.transactions.LBMultiDeviceResponse;
import dlt.client.tangle.hornet.model.transactions.Reply;
import dlt.client.tangle.hornet.model.transactions.Request;
import dlt.client.tangle.hornet.model.transactions.TargetedTransaction;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import java.util.logging.Logger;

/**
 *
 * @author Uellington Damasceno
 */
public class WaitingLBRequestState extends AbstractBalancerState {

    private static final Logger logger = Logger.getLogger(WaitingLBRequestState.class.getName());
    private boolean isMultiLayerState;
    
    public WaitingLBRequestState(Balancer balancer, boolean isMultiLayerState) {
        super(balancer);
        this.isMultiLayerState = isMultiLayerState;
    }
    
    @Override
    public void onEnter() {
        Long LBRequestTimeWaiting = this.balancer.getLBRequestTimeWaiting();
        this.balancer.scheduleTimeout(LBRequestTimeWaiting);
    }

    @Override
    protected boolean isValidTransaction(Transaction transaction) {
        return transaction.is(TransactionType.LB_REQUEST) 
                || (this.isMultiLayerState && transaction.is(TransactionType.LB_MULTI_DEVICE_REQUEST));
    }

    @Override
    protected void handleInvalidTransaction(Transaction trans) {
        logger.info("Transação ignorada. Esperado LB*_REQUEST.");
    }

    @Override
    protected void handleValidTransaction(Transaction transaction) {
        if (!((TargetedTransaction) transaction).isSameTarget(source)) {
            //logger.info("LB_REQUEST com target errado. Timeout reiniciado."); 
            // Verificar a necessidade de reiniciar timer
            return;
        }
        String device = ((Request) transaction).getDevice();
        this.balancer.receiveNewDevice(device);
        String transactionSender = transaction.getSource();
        
        Transaction reply = this.isMultiLayerState 
                ? new LBMultiDeviceResponse(source, group, device, transactionSender)
                : new Reply(source, group, transactionSender);
        
        this.balancer.sendTransaction(reply);
        logger.info("LB_REPLY enviado.");
        this.balancer.transitionTo(new IdleState(balancer));
    }
    
    
}
