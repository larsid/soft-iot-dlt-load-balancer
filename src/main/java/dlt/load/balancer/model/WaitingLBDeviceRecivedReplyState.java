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

    private final AbstractBalancerState nextState;
    private final Device deviceToSend;

    public WaitingLBDeviceRecivedReplyState(Balancer balancer, AbstractBalancerState nextState, Device device) {
        super(balancer, null);
        this.nextState = nextState;
        this.deviceToSend = device;
    }

    @Override
    public void onEnter() {
        Long deviceRecivedReplyTime = this.balancer.getLBDeviceRecivedReplyTimeWaiting();
        this.scheduleTimeout(deviceRecivedReplyTime);
    }

    @Override
    protected boolean isValidTransactionForThisState(Transaction transaction) {
        return transaction.is(TransactionType.LB_REPLY) 
                || transaction.is(TransactionType.LB_MULTI_DEVICE_RESPONSE);
    }

    @Override
    protected void handleInvalidTransaction(Transaction trans) {
        logger.info("Acceptable trans: LB_REPLY or LB_MULTI_DEVICE_RESPONSE.");
    }

    @Override
    protected void handleValidTransaction(Transaction transaction, String currentGatewayId) {
        if (!((TargetedTransaction) transaction).isSameTarget(currentGatewayId)) {
            return;
        }
        this.cancelTimeout();
        String ip = transaction.getSource().split("/")[2];
        String port = transaction.getSource().split("/")[3];
        try {
            this.balancer.sendDevice(deviceToSend, ip, port);
          /*  Transaction transactionDevice = new LBDevice(currentGatewayId, group, this.lastRemovedDevice, trans.getSource());
            this.sendTransaction(transactionDevice);*/
          this.transiteOverloadedStateTo(new OverloadIdleState(balancer));
        } catch (MqttException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void onTimeout() {
        logger.log(Level.WARNING,
                "Timeout in state {0}, transitioning to {1}.",
                new Object[]{this.getClass().getSimpleName(),
                    nextState.getClass().getSimpleName()});
        
        this.transiteOverloadedStateTo(nextState);
    }
    
}
