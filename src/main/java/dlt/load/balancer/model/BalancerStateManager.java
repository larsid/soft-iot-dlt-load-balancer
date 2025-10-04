package dlt.load.balancer.model;

import dlt.client.tangle.hornet.enums.TransactionType;
import dlt.client.tangle.hornet.model.transactions.Status;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Uellington Damasceno
 */
public class BalancerStateManager {

    private static final Logger logger = Logger.getLogger(BalancerStateManager.class.getName());

    private final Map<String, AbstractBalancerState> balancerRequestsState;
   
    private AbstractBalancerState overloadedGatewayState;

    private final Predicate<AbstractBalancerState> isStateBalancing;
    private Status internalStatus;

    public BalancerStateManager(Balancer balancer) {
        this.balancerRequestsState = new HashMap<>();
        this.isStateBalancing = (state) -> !(state instanceof IdleState);
        this.overloadedGatewayState = new OverloadIdleState(balancer);
    }

    public void transitionTo(String gatewayTarget, AbstractBalancerState newState) {
        Optional<BalancerState> state = this.getBalancerByTransactionSender(gatewayTarget);
        this.logStateTransition(state.get(), newState, false);
        this.updateBalanceStateById(gatewayTarget, newState);
        newState.onEnter();
    }

    public void transiteOverloadedStateTo(AbstractBalancerState newState, boolean shouldInitializeState) {
        this.logStateTransition(this.overloadedGatewayState, newState, true);
        if (this.overloadedGatewayState != null) {
            this.overloadedGatewayState.cancelTimeout();
        }
        this.overloadedGatewayState = newState;
        if (shouldInitializeState) {
            this.overloadedGatewayState.onEnter();
        }
    }
    
    public boolean isGatewayOverloadIdleState() {
        return this.overloadedGatewayState instanceof OverloadIdleState;
    }

    private void logStateTransition(BalancerState oldState, BalancerState newState, boolean isOverload) {
        String oldStateName = this.getStateName(oldState);
        String newStateName = this.getStateName(newState);
        String message = isOverload ? 
                "Overload State transaction from {0} to {1}" :
                "State transaction from {0} to {1}";
        logger.log(Level.INFO, message, new Object[]{oldStateName, newStateName});
    }

    private String getStateName(BalancerState state) {
        return (state != null)
                ? state.getClass().getSimpleName()
                : "NOT_FOUND_STATE";
    }

    public void addBalancerRequestHandle(String gatewayId, AbstractBalancerState newState) {
        if (!this.balancerRequestsState.containsKey(gatewayId)) {
             this.balancerRequestsState.put(gatewayId, newState);
             newState.onEnter();
             return;
        }

        BalancerState currentState = this.balancerRequestsState.get(gatewayId);
        
        if (this.willNewCancelGatewayBalancing(currentState, newState)) {
            logger.log(Level.INFO, 
                    "Was requested the state transaction from {0} to {1} but are't not change because will interrupt balancing.", 
                    new Object[]{currentState.toString(), newState.toString()});
            return;
        }
        
        this.updateBalanceStateById(gatewayId, newState);
        newState.onEnter();
    }
    
    private boolean willNewCancelGatewayBalancing(BalancerState currentState, BalancerState newState){
        return !(currentState instanceof IdleState);
    }

    public void updateBalanceStateById(String gatewayId, AbstractBalancerState newState) {
        if (!this.balancerRequestsState.containsKey(gatewayId)) {
            logger.log(Level.WARNING, "Gateway id: {0} not found to update.", gatewayId);
            return;
        }
        this.balancerRequestsState.replace(gatewayId, newState);
    }

    public boolean canReciveNewDevice(long maxNumberConnectedDevices) {
        return this.currentGatewayLoad()
                + this.qtySimultaneosBalancings()
                + 1 <= maxNumberConnectedDevices;
    }

    private double currentGatewayLoad() {
        return this.internalStatus != null
                ? this.internalStatus.getLastLoad()
                : 0;
    }

    private long qtySimultaneosBalancings() {
        return balancerRequestsState.values()
                .stream()
                .filter(this.isStateBalancing)
                .count();
    }

    public Optional<BalancerState> getBalancerByTransaction(Transaction transaction) {
        if(this.isBalancingStartResponseTransaction(transaction) && !this.isGatewayOverloadIdleState()){
            return Optional.ofNullable(this.overloadedGatewayState);
        }
        String transactionSender = transaction.getSource();
        return this.getBalancerByTransactionSender(transactionSender);
    }

    private Optional<BalancerState> getBalancerByTransactionSender(String transactionSender) {
        if (!this.balancerRequestsState.containsKey(transactionSender)) {
            return Optional.empty();
        }
        return Optional.of(this.balancerRequestsState.get(transactionSender));
    }

    private boolean isBalancingStartResponseTransaction(Transaction transaction) {
        return transaction.is(TransactionType.LB_ENTRY_REPLY)
                || transaction.is(TransactionType.LB_MULTI_RESPONSE);
    }

    public void updateInternalStatus(Status internalStatus) {
        this.internalStatus = internalStatus;
    }

    public Status currentInternalStatus() {
        return this.internalStatus;
    }

}
