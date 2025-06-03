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
        this.balancer.cancelTimeout();
    }

    @Override
    public boolean isBalancing() {
        return false;
    }

    @Override
    protected boolean isValidTransaction(Transaction transaction) {
        return transaction.is(TransactionType.LB_ENTRY)
                || transaction.is(TransactionType.LB_MULTI_REQUEST);
    }

    @Override
    protected void handleInvalidTransaction(Transaction trans) {
        logger.log(Level.INFO, "Transação ignorada. Tipo não é LB_ENTRY nem LB_MULTI_DEVICE_REQUEST.");
    }

    @Override
    public void handleValidTransaction(Transaction transaction) {

        if (transaction.isLoopback(source)) {
            logger.info("Solicitação interna de balancemaento iniciada.");
            this.balancer.transitionTo(new WaitingLBReplyState(balancer));
            return;
        }

        if (!this.balancer.canReciveNewDevice()) {
            logger.info("Gateway indisponível para receber novos devices.");
            return;
        }
        boolean isMultiLayer = this.balancer.isMultiLayerBalancer();

        String transactionSender = transaction.getSource();

        Transaction reply = isMultiLayer
                ? new LBMultiResponse(source, group, transactionSender)
                : new LBReply(source, group, transactionSender);

        this.balancer.sendTransaction(reply);
        this.balancer.transitionTo(new WaitingLBRequestState(balancer, isMultiLayer));
    }

}
