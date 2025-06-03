package dlt.load.balancer.model;

import dlt.client.tangle.hornet.model.transactions.Transaction;

/**
 *
 * @author Uellington Damasceno
 */
public interface BalancerState {

    void onEnter();

    boolean isBalancing();

    void handle(Transaction transaction);

    void onTimeout();
}
