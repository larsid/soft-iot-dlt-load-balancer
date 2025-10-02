package dlt.load.balancer.model;

import dlt.client.tangle.hornet.enums.TransactionType;
import dlt.client.tangle.hornet.model.transactions.LBMultiResponse;
import dlt.client.tangle.hornet.model.transactions.LBReply;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Uellington Damasceno
 */
public class IdleState extends AbstractBalancerState {

    private static final Logger logger = Logger.getLogger(IdleState.class.getName());

    public IdleState(Balancer balancer) {
        super(balancer);
    }

    @Override
    public void onEnter() {
        this.cancelTimeout();
    }

    @Override
    protected boolean isValidTransactionForThisState(Transaction transaction) {
        return transaction.is(TransactionType.LB_ENTRY)
                || transaction.is(TransactionType.LB_MULTI_REQUEST);
    }

    @Override
    protected void handleInvalidTransaction(Transaction trans) {
        logger.log(Level.INFO, "Acceptable trans: LB_ENTRY or LB_MULTI_REQUEST.");
    }

    @Override
    protected void handleValidTransaction(Transaction transaction, String currentGatewayId) {
        String transactionSender = transaction.getSource();
        
        Transaction reply = transaction.isMultiLayerTransaction()
                ? new LBMultiResponse(currentGatewayId, group, transactionSender)
                : new LBReply(currentGatewayId, group, transactionSender);

        this.balancer.sendTransaction(reply);
        this.hookTransitionTo(new WaitingLBRequestState(balancer, transactionSender));
    }

    @Override
    public boolean canProcessLoopback(Transaction transaction) {
        return this.isValidTransactionForThisState(transaction);
    }
    
    public void hookTransitionTo(AbstractBalancerState state){
        this.transitionTo(state);
    }

}
