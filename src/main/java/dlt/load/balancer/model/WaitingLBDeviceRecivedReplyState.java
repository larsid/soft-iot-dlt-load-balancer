package dlt.load.balancer.model;

import br.ufba.dcc.wiser.soft_iot.entities.Device;
import dlt.client.tangle.hornet.enums.TransactionType;
import dlt.client.tangle.hornet.model.transactions.TargetedTransaction;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.paho.client.mqttv3.MqttException;

/**
 *
 * @author Uellington Damasceno
 */
public class WaitingLBDeviceRecivedReplyState extends AbstractBalancerState {

    private static final Logger logger = Logger.getLogger(WaitingLBDeviceRecivedReplyState.class.getName());

    private final BalancerState nextState;
    private final Device deviceToSend;

    public WaitingLBDeviceRecivedReplyState(Balancer balancer, BalancerState nextState, Device device) {
        super(balancer);
        this.nextState = nextState;
        this.deviceToSend = device;
    }

    @Override
    public void onEnter() {
        Long deviceRecivedReplyTime = this.balancer.getLBDeviceRecivedReplyTimeWaiting();
        this.balancer.scheduleTimeout(deviceRecivedReplyTime);
    }

    @Override
    protected boolean isValidTransaction(Transaction transaction) {
        return transaction.is(TransactionType.LB_REPLY) 
                || (transaction.isMultiLayerTransaction() 
                && transaction.is(TransactionType.LB_MULTI_DEVICE_RESPONSE));
    }

    @Override
    protected void handleInvalidTransaction(Transaction trans) {
        logger.info("Transação ignorada. Esperado LB_REPLY.");
    }

    @Override
    protected void handleValidTransaction(Transaction transaction) {
        if (!((TargetedTransaction) transaction).isSameTarget(source)) {
            //CONSIDERAR NECESSIDADE DE REINICIAR O TIMEOUT
            return;
        }
        this.balancer.cancelTimeout();
        String ip = transaction.getSource().split("/")[2];
        String port = transaction.getSource().split("/")[3];
        try {
            this.balancer.sendDevice(deviceToSend, ip, port);
          /*  Transaction transactionDevice = new LBDevice(source, group, this.lastRemovedDevice, trans.getSource());
            this.sendTransaction(transactionDevice);*/
          this.balancer.transitionTo(new IdleState(balancer));
        } catch (MqttException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void onTimeout() {
        this.balancer.transitionTo(nextState);
    }
}
