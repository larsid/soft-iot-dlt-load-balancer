package dlt.load.balancer.model;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import br.uefs.larsid.extended.mapping.devices.tatu.DeviceWrapper;
import br.ufba.dcc.wiser.soft_iot.entities.Device;
import dlt.client.tangle.hornet.model.transactions.Transaction;

/**
 *
 * @author Uellington Damasceno
 */
public abstract class AbstractProcessSendDeviceState extends AbstractBalancerState {

    private static final Logger logger = Logger.getLogger(AbstractProcessSendDeviceState.class.getName());

    private Long qtyMaxResendTansaction;
    private final Transaction transBeingProcessed;
    private Device deviceToRemove;
    private BalancerState waitingLBDeviceRecivedReplyState;

    public AbstractProcessSendDeviceState(Balancer balancer, Transaction transaction) {
        super(balancer);
        this.transBeingProcessed = transaction;
        this.qtyMaxResendTansaction = this.balancer.qtyMaxTimeResendTransaction();
    }

    @Override
    public void onEnter() {
        this.handle(transBeingProcessed);
    }

    @Override
    protected void handleValidTransaction(Transaction transaction) {
        logger.info("LB_*DEVICE*RESPONSE recebido com target correto. Iniciando envio de LB_REQUEST.");
        Transaction transactionRequest;
        String sender = transaction.getSource();
        try {
            this.deviceToRemove = this.selectWhichDeviceToRemove();
            String deviceJson = DeviceWrapper.toJSON(deviceToRemove);
            transactionRequest = this.buildTransaction(deviceJson, sender);
            this.balancer.sendTransaction(transactionRequest);

            logger.log(Level.INFO, "Device selecionado para envio: {0}", deviceJson);
            logger.info("LB_REQUEST enviado com sucesso.");
            this.transiteToNextState();
        } catch (IOException | NoSuchElementException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        this.updateQtyResendTransaction();
    }

    private void transiteToNextState() {
        if (qtyMaxResendTansaction == 0) {
            this.balancer.transitionTo(new IdleState(balancer));
            return;
        }

        waitingLBDeviceRecivedReplyState = this.waitingLBDeviceRecivedReplyState != null
                ? this.waitingLBDeviceRecivedReplyState
                : new WaitingLBDeviceRecivedReplyState(balancer, this, this.deviceToRemove);

        this.balancer.transitionTo(waitingLBDeviceRecivedReplyState);
    }

    private Device selectWhichDeviceToRemove() throws IOException {
        return this.deviceToRemove != null
                ? this.deviceToRemove
                : this.balancer.getFirstDevice().orElseThrow();
    }

    private void updateQtyResendTransaction() {
        logger.log(Level.INFO, "Amount of resent attempts remaining:  ", --this.qtyMaxResendTansaction);
    }

    protected abstract Transaction buildTransaction(String deviceJson, String sender);
}
