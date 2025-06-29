package dlt.load.balancer.model;

import dlt.client.tangle.hornet.enums.TransactionType;
import dlt.client.tangle.hornet.model.transactions.Status;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Uellington Damasceno
 */
public abstract class AbstractBalancerState implements BalancerState {

    private static final Logger logger = Logger.getLogger(AbstractBalancerState.class.getName());

    protected final Balancer balancer;

    protected String source;
    protected String group;

    protected AbstractBalancerState(Balancer balancer) {
        this.balancer = balancer;
        this.group = balancer.getGatewayGroup();
    }

    @Override
    public abstract void onEnter();

    protected abstract boolean isValidTransaction(Transaction transaction);

    protected abstract void handleInvalidTransaction(Transaction trans);

    protected abstract void handleValidTransaction(Transaction transaction);

    @Override
    public final void handle(Transaction transaction) {
        if (!this.balancer.isValidPublishMessageInterval(transaction.getPublishedAt())) {
            logger.log(Level.WARNING, "{0} foi é considerada antiga.", transaction);
            return;
        }

        if (transaction.isMultiLayerTransaction() && !this.balancer.isMultiLayerBalancer()) {
            logger.info("Load balancer - Multilayer message type not allowed.");
            return;
        }

        this.source = balancer.buildSource();
        boolean isLoopback = transaction.isLoopback(source);

        if (transaction.is(TransactionType.LB_STATUS)) {
            if (!isLoopback) {
                return;
            }
            this.balancer.updateInternalStatus((Status) transaction);
            return;
        }

        if (isLoopback && !this.canProcessLoopback(transaction)) {
            return;
        }

        if (!this.isValidTransaction(transaction)) {
            logger.log(Level.WARNING, "Recived trans type: {0} from {1}",
                    new Object[]{transaction.getType(), transaction.getSource()});
            this.handleInvalidTransaction(transaction);
            return;
        }

        if (this.balancer.shouldDisplayPastTimeTransPublication()) {
            String time = TimeFormatter.formatTimeElapsed(transaction.getPublishedAt());
            logger.log(Level.INFO, "{0} - {1}", new Object[]{transaction.getType(), time});
        }

        this.handleValidTransaction(transaction);
    }

    @Override
    public void onTimeout() {
        logger.log(Level.WARNING,
                "Timeout in state {0}, transitioning to IdleState.",
                this.getClass().getSimpleName());

        balancer.transitionTo(new IdleState(balancer));
    }

    @Override
    public boolean canProcessLoopback(Transaction transaction) {
        return false;
    }
}
