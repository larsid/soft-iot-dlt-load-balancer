package dlt.load.balancer.model;

import dlt.client.tangle.hornet.model.transactions.Transaction;
import java.util.concurrent.ScheduledFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Uellington Damasceno
 */
public abstract class AbstractBalancerState implements BalancerState {

    private static final Logger logger = Logger.getLogger(AbstractBalancerState.class.getName());

    protected final Balancer balancer;

    private ScheduledFuture<?> internalTimeout;

    private String gatewayTarget;
    
    protected String group;

    protected AbstractBalancerState(Balancer balancer) {
        this.balancer = balancer;
        this.group = balancer.getGatewayGroup();
    }

    @Override
    public abstract void onEnter();

    protected abstract boolean isValidTransactionForThisState(Transaction transaction);

    protected abstract void handleInvalidTransaction(Transaction trans);

    protected abstract void handleValidTransaction(Transaction transaction, String currentGatewayId);

    @Override
    public final void handle(Transaction transaction, String currentGatewayId) {
        
        if (!this.isValidTransactionForThisState(transaction)) {
            logger.log(Level.WARNING, "Recived trans type: {0} from {1}",
                    new Object[]{transaction.getType(), transaction.getSource()});
            this.handleInvalidTransaction(transaction);
            return;
        }

        if (this.balancer.shouldDisplayPastTimeTransPublication()) {
            String time = TimeFormatter.formatTimeElapsed(transaction.getPublishedAt());
            logger.log(Level.INFO, "{0} - {1}", new Object[]{transaction.getType(), time});
        }

        this.handleValidTransaction(transaction, currentGatewayId);
    }

    @Override
    public void onTimeout() {
        logger.log(Level.WARNING,
                "Timeout in state {0}, transitioning to IdleState.",
                this.getClass().getSimpleName());

        this.transitionTo(new IdleState(balancer));
    }

    @Override
    public boolean canProcessLoopback(Transaction transaction) {
        return false;
    }

    public void scheduleTimeout(long delayMillis) {
        this.cancelTimeout();
        this.internalTimeout = this.balancer.scheduleTimeout(this, delayMillis);
    }

    public void cancelTimeout() {
        if (internalTimeout != null) {
            internalTimeout.cancel(true);
        }
    }
    
    protected final void transitionTo(AbstractBalancerState state){
        this.cancelTimeout();
        this.balancer.transitionTo(gatewayTarget, state);
    }
    
    protected final void transiteOverloadedStateTo(AbstractBalancerState state){
        this.cancelTimeout();
        this.balancer.transiteOverloadedStateTo(state, true);
    }
    
    protected final void sendTransaction(Transaction transaction){
        this.balancer.sendTransaction(transaction);
    }
}
