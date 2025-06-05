package dlt.load.balancer.model;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import br.uefs.larsid.extended.mapping.devices.tatu.DeviceWrapper;
import br.ufba.dcc.wiser.soft_iot.entities.Device;
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
    private BalancerState waitingLBDeviceRecivedReplyState;

    public AbstractProcessSendDeviceState(Balancer balancer) {
        super(balancer);
        this.qtyMaxResendTansaction = this.balancer.qtyMaxTimeResendTransaction();
    }

    @Override
    public void onEnter() {
        Long waitingTime = this.balancer.getLBStartReplyTimeWaiting();
        this.balancer.scheduleTimeout(waitingTime);
    }

    @Override
    protected void handleValidTransaction(Transaction transaction) {
        if(transaction == null){
            logger.warning("VIXE");
            return;
        }
        if (!(transaction instanceof TargetedTransaction)){
            logger.log(Level.WARNING, "Transaction {0} recived but is not a targeted transaction", transaction.getType());
            return;
        }
        String transTarget = ((TargetedTransaction) transaction).getTarget();
        if (!this.source.equals(transTarget)){
            return;
        }
        logger.info("LB_*RESPONSE recebido com target correto. Iniciando envio de LB_*REQUEST.");
        
        Transaction transactionRequest;
        String sender = transaction.getSource();
        try {
            this.deviceToRemove = this.selectWhichDeviceToRemove();
            String deviceJson = DeviceWrapper.toJSON(deviceToRemove);
            transactionRequest = this.buildTransaction(deviceJson, sender);
            this.balancer.sendTransaction(transactionRequest);
            this.handlePostSendTransaction();
            this.transiteToNextState();
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        } catch (NoSuchElementException ex) {
            logger.warning("AbstractProcessSendDeviceState - Device list is empty.");
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
        logger.log(Level.INFO, "Amount of resent attempts remaining: {0}", --this.qtyMaxResendTansaction);
    }

    protected abstract Transaction buildTransaction(String deviceJson, String sender);
    
    protected abstract void handlePostSendTransaction();

}
