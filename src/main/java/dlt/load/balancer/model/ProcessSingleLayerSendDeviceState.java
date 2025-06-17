package dlt.load.balancer.model;

import dlt.client.tangle.hornet.enums.TransactionType;
import dlt.client.tangle.hornet.model.transactions.Request;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Uellington Damasceno
 */
public class ProcessSingleLayerSendDeviceState extends AbstractProcessSendDeviceState {

    private static final Logger logger = Logger.getLogger(ProcessSingleLayerSendDeviceState.class.getName());

    public ProcessSingleLayerSendDeviceState(Balancer balancer) {
        super(balancer);
    }

    @Override
    protected Transaction buildTransaction(String deviceJson, String sender) {
        return new Request(source, group, deviceJson, sender);
    }

    @Override
    protected boolean isValidTransaction(Transaction transaction) {
        return transaction.is(TransactionType.LB_ENTRY_REPLY);
    }

    @Override
    protected void handleInvalidTransaction(Transaction trans) {
        logger.log(Level.INFO, "Acceptable trans: LB_ENTRY_REPLY.");
    }

    @Override
    protected void handlePostSendTransaction() {
        logger.info("LB_ENTRY_REPLY Sent successfully");
    }

}
