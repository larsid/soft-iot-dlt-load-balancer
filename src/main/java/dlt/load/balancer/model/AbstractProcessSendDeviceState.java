package dlt.load.balancer.model;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import br.uefs.larsid.extended.mapping.devices.tatu.DeviceWrapper;
import br.ufba.dcc.wiser.soft_iot.entities.Device;
import dlt.client.tangle.hornet.model.transactions.LBMultiRequest;
import dlt.client.tangle.hornet.model.transactions.Status;
import dlt.client.tangle.hornet.model.transactions.TargetedTransaction;
import dlt.client.tangle.hornet.model.transactions.Transaction;

/**
 *
 * @author Uellington Damasceno
 */
public abstract class AbstractProcessSendDeviceState extends AbstractBalancerState {

    private static final Logger logger = Logger.getLogger(AbstractProcessSendDeviceState.class.getName());

    private Long qtyMaxResendTansaction;
    private Device deviceToRemove;
    private AbstractBalancerState waitingLBDeviceRecivedReplyState;
    private String sender;
    private String currentGatewayId;
    private Transaction startBalancingTransaction;

    public AbstractProcessSendDeviceState(Balancer balancer, Transaction referenceTransaction) {
        super(balancer, null);
        this.qtyMaxResendTansaction = this.balancer.qtyMaxTimeResendTransaction();
        this.currentGatewayId = balancer.getGatewayId();
        this.startBalancingTransaction = referenceTransaction;
    }

    @Override
    public void onEnter() {
        Long waitingTime = this.balancer.getLBStartReplyTimeWaiting();
        this.scheduleTimeout(waitingTime);
        this.startBalancingTransaction = this.refreshTransaction(startBalancingTransaction);
        this.balancer.sendTransaction(startBalancingTransaction);
    }

    private Transaction refreshTransaction(Transaction transaction) {
        String targetGroup = this.balancer.getGatewayGroup();

        if (!transaction.isMultiLayerTransaction()) {
            Status status = (Status) transaction;
            return new Status(currentGatewayId, targetGroup, true, status.getLastLoad(), status.getAvgLoad(), false);
        }
        return new LBMultiRequest(currentGatewayId, targetGroup);
    }

    @Override
    protected void handleValidTransaction(Transaction transaction, String currentGatewayId) {
        if (transaction == null) {
            logger.warning("VIXE");
            return;
        }
        if (!(transaction instanceof TargetedTransaction)) {
            logger.log(Level.WARNING, "Transaction {0} recived but is not a targeted transaction", transaction.getType());
            return;
        }
        TargetedTransaction targetedTransaction = ((TargetedTransaction) transaction);

        if (!targetedTransaction.isSameTarget(currentGatewayId)) {
            return;
        }
        logger.info("LB_*RESPONSE recebido com target correto. Iniciando envio de LB_*REQUEST.");

        this.sender = transaction.getSource();
        this.currentGatewayId = currentGatewayId;
        this.sendPreConfirmSendDevice();
    }

    private void sendPreConfirmSendDevice() {
        Transaction transactionRequest;

        try {
            this.deviceToRemove = this.selectWhichDeviceToRemove();
            String deviceJson = DeviceWrapper.toJSON(deviceToRemove);
            transactionRequest = this.buildTransaction(deviceJson, currentGatewayId, sender);
            this.balancer.sendTransaction(transactionRequest);
            this.handlePostSendTransaction();
            this.transiteToNextState();
            this.updateQtyResendTransaction();
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        } catch (NoSuchElementException ex) {
            logger.warning("AbstractProcessSendDeviceState - Device list is empty.");
        }
    }

    private void transiteToNextState() {
        if (qtyMaxResendTansaction == 0) {
            this.transiteOverloadedStateTo(new OverloadIdleState(balancer));
            return;
        }

        waitingLBDeviceRecivedReplyState = this.waitingLBDeviceRecivedReplyState != null
                ? this.waitingLBDeviceRecivedReplyState
                : new WaitingLBDeviceRecivedReplyState(balancer, this, this.deviceToRemove);

        this.transiteOverloadedStateTo(waitingLBDeviceRecivedReplyState);
    }

    private Device selectWhichDeviceToRemove() throws IOException {
        return this.deviceToRemove != null
                ? this.deviceToRemove
                : this.balancer.getFirstDevice().orElseThrow();
    }

    private void updateQtyResendTransaction() {
        this.qtyMaxResendTansaction = this.qtyMaxResendTansaction - 1;
        logger.log(Level.INFO, "Amount of resent attempts remaining: {0}", this.qtyMaxResendTansaction);
    }

    protected abstract Transaction buildTransaction(String deviceJson, String currentGatewayId, String sender);

    protected abstract void handlePostSendTransaction();

    @Override
    public void onTimeout() {
        logger.log(Level.WARNING,
                "Timeout in state {0}, transitioning to OverloadIdleState.",
                this.getClass().getSimpleName());

        this.transiteOverloadedStateTo(new OverloadIdleState(balancer));
    }

}
