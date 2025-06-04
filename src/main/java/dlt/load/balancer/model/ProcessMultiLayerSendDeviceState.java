package dlt.load.balancer.model;

import dlt.client.tangle.hornet.enums.TransactionType;
import dlt.client.tangle.hornet.model.transactions.LBMultiDevice;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Uellington Damasceno
 */
public class ProcessMultiLayerSendDeviceState extends AbstractProcessSendDeviceState {
    private static final Logger logger = Logger.getLogger(ProcessMultiLayerSendDeviceState.class.getName());

    public ProcessMultiLayerSendDeviceState(Balancer balancer, Transaction transaction) {
        super(balancer, transaction);
    }

    @Override
    protected Transaction buildTransaction(String deviceJson, String sender) {
        return new LBMultiDevice(source, group, deviceJson, sender);
    }

    @Override
    protected boolean isValidTransaction(Transaction transaction) {
        return transaction.isMultiLayerTransaction()
                && transaction.is(TransactionType.LB_MULTI_RESPONSE);
    }

    @Override
    protected void handleInvalidTransaction(Transaction trans) {
        logger.log(Level.INFO, "Acceptable trans: LB_MULTI_RESPONSE.");
    }

}
