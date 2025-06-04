package dlt.load.balancer.model;

import dlt.client.tangle.hornet.enums.TransactionType;
import dlt.client.tangle.hornet.model.transactions.TargetedTransaction;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Uellington Damasceno
 */
public class WaitingLBReplyState extends AbstractBalancerState {

    private static final Logger logger = Logger.getLogger(WaitingLBReplyState.class.getName());
    
    public WaitingLBReplyState(Balancer balancer) {
        super(balancer);
    }

    @Override
    public void onEnter() {
        Long waitingTime = this.balancer.getLBStartReplyTimeWaiting();
        this.balancer.scheduleTimeout(waitingTime);
    }

    @Override
    protected boolean isValidTransaction(Transaction transaction) {
        return transaction.is(TransactionType.LB_ENTRY_REPLY) 
                || (transaction.isMultiLayerTransaction() 
                && transaction.is(TransactionType.LB_MULTI_RESPONSE));
    }

    @Override
    protected void handleInvalidTransaction(Transaction trans) {
        logger.log(Level.INFO, "Acceptable trans: LB_ENTRY_REPLY or LB_MULTI_RESPONSE.");
    }

    @Override
    protected void handleValidTransaction(Transaction transaction) {
        if (!((TargetedTransaction) transaction).isSameTarget(source)) {
            return;
        }
        BalancerState nextState = transaction.isMultiLayerTransaction() 
                ? new ProcessMultiLayerSendDeviceState(balancer, transaction)
                : new ProcessSingleLayerSendDeviceState(balancer, transaction);
        
        this.balancer.transitionTo(nextState);
    }

}
