package dlt.load.balancer.model;

import static java.util.stream.Collectors.toList;

import br.uefs.larsid.extended.mapping.devices.services.IDevicePropertiesManager;
import br.uefs.larsid.extended.mapping.devices.tatu.DeviceWrapper;
import br.ufba.dcc.wiser.soft_iot.entities.Device;
import com.google.gson.JsonObject;
import dlt.auth.services.IPublisher;
import dlt.client.tangle.enums.TransactionType;
import dlt.client.tangle.model.transactions.LBDevice;
import dlt.client.tangle.model.transactions.LBReply;
import dlt.client.tangle.model.transactions.Reply;
import dlt.client.tangle.model.transactions.Request;
import dlt.client.tangle.model.transactions.Status;
import dlt.client.tangle.model.transactions.TargetedTransaction;
import dlt.client.tangle.model.transactions.Transaction;
import dlt.client.tangle.services.ILedgerSubscriber;
import dlt.id.manager.services.IDLTGroupManager;
import dlt.id.manager.services.IIDManagerService;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.json.JSONArray;
import java.util.logging.Logger;

/**
 *
 * @author Allan Capistrano, Antonio Crispim, Uellington Damasceno
 * @version 0.0.2
 */
public class Balancer implements ILedgerSubscriber, Runnable {

    private final ScheduledExecutorService executor;
    private TimerTask timerTaskLb, timerTaskGateWay;

    private LedgerConnector connector;

    private final Long TIMEOUT_LB_REPLY, TIMEOUT_GATEWAY;
    private Transaction lastTransaction;
    private List<String> subscribedTopics;
    
    private IDevicePropertiesManager deviceManager;
    private IIDManagerService idManager;
    private IDLTGroupManager groupManager;
    private IPublisher publisher;
    
    private int timerPass, resent;
    private Status lastStatus;
    private String lastRemovedDevice;
    private boolean flagSubscribe;
    private static final Logger logger = Logger.getLogger(Balancer.class.getName());

    public Balancer(long timeoutLB, long timeoutGateway) {
        this.TIMEOUT_LB_REPLY = timeoutLB;
        this.TIMEOUT_GATEWAY = timeoutGateway;

        this.buildTimerResendTransaction();
        this.resent = 0;
        this.lastStatus = null;
        this.flagSubscribe = true;

        this.executor = Executors.newSingleThreadScheduledExecutor();
    }

    public void buildTimerResendTransaction() {
        timerTaskGateWay
                = new TimerTask() {
            @Override
            public void run() {
                Logger log = Logger.getLogger(Balancer.class.getName());

                timerPass = timerPass + 1;
                log.info(String.valueOf(timerPass));

                if ((timerPass * 1000) >= TIMEOUT_GATEWAY) {
                    resendTransaction();
                    timerTaskGateWay.cancel();
                    log.info("TIME'S UP");
                }
            }
        };
    }

    public void setPublisher(IPublisher iPublisher) {
        this.publisher = iPublisher;
    }

    public void setDeviceManager(IDevicePropertiesManager deviceManager) {
        this.deviceManager = deviceManager;
    }

    public void setConnector(LedgerConnector connector) {
        this.connector = connector;
    }

    public void setIdManager(IIDManagerService idManager) {
        this.idManager = idManager;
    }

    public void setGroupManager(IDLTGroupManager groupManager) {
        this.groupManager = groupManager;
    }

    public void setSubscribedTopics(String topicsJSON) {
        logger.log(Level.INFO, "Set topics: {0}", topicsJSON);

        this.subscribedTopics
                = new JSONArray(topicsJSON)
                        .toList()
                        .stream()
                        .filter(String.class::isInstance)
                        .map(String.class::cast)
                        .collect(toList());
    }

    public void start() {
        this.executor.scheduleAtFixedRate(this, 0, 5, TimeUnit.SECONDS);
    }

    public void stop() {
        this.flagSubscribe = true;

        this.subscribedTopics
                .forEach(topic -> this.connector.unsubscribe(topic, this));

        this.executor.shutdown();

        try {
            if (this.executor.awaitTermination(500, TimeUnit.MILLISECONDS)
                    && !this.executor.isShutdown()) {
                this.executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            this.executor.shutdownNow();
        }
    }

    private boolean currentGatewayIsTarget(Transaction transaction) {
        if (transaction == null) {
            logger.warning("The transaction cannot be null.");
            return false;
        }
        if (!(transaction instanceof TargetedTransaction)) {
            String message = "The new transaction {0} is not a targeted transaction. Therefore it's impossible evaluate the target";
            logger.log(Level.WARNING, message, transaction.getClass().getName());
            return false;
        }
        return ((TargetedTransaction) transaction).getTarget().equals(this.buildSource());
    }

    private void messageArrived(Transaction transaction) {
        boolean isStatusTransaction = transaction.getType().equals(TransactionType.LB_STATUS);
        boolean isLoopbackMessage = transaction.getSource().equals(this.buildSource());
        if (isStatusTransaction && isLoopbackMessage) {
            logger.info("Load balancer - The new message is a status loopback.");
            this.lastStatus = (Status) transaction;
        }
        logger.info("Waiting for LB_ENTRY");
        TransactionType transactionType = transaction.getType();
        if (!transactionType.equals(TransactionType.LB_ENTRY)) {
            logger.log(Level.INFO, "Load balancer - The transaction type is {0}", transactionType);
            return;
        }
        if (isLoopbackMessage) {// Definindo minha última transação enviada.
            logger.info("Load balancer - The type message is LB_ENTRY and it's a loopback.");
            this.lastTransaction = transaction;
            executeTimeOutLB();
            return;
        }
        logger.info("Load balancer - The type of new message is LB_ENTRY and is from another gateway");
        // Verificando a própria disponibilidade.
        if (!this.lastStatus.getAvaible()) { //Considerar recuperar essa informação direto do monitor
            logger.info("Load balancer - This gateway is not availabe to receive new devices.");
            return;
        }
        String source = this.buildSource();
        String group = this.groupManager.getGroup();
        String newTarget = transaction.getSource();

        Transaction transactionReply = new LBReply(source, group, newTarget);
        this.sendTransaction(transactionReply);
    }

    private void processWhenLastTransactionIsLBEntry(Transaction transaction) {
        boolean isEntryReply = transaction.getType().equals(TransactionType.LB_ENTRY_REPLY);
        if (!isEntryReply) {
            String message = "The new transaction [{0}] will not be processed. Because is not LB_ENTRY_REPLY";
            logger.log(Level.WARNING, message, transaction.getType());
            return;
        }
        if (this.currentGatewayIsTarget(transaction)) {
            logger.info("The new transaction will not be processed. Because the current gateway is not the target.");
            return;
        }
        logger.log(Level.INFO, "Processing the new transaction [{0}].", transaction.getType());
        timerTaskLb.cancel();
        // Enviar LB_REQUEST.
        Device deviceToSend;
        try {
            deviceToSend = this.deviceManager.getAllDevices().get(0);
        } catch (IOException ioe) {
            logger.info("Load Balancer - Error! Unable to retrieve device list.");
            logger.log(Level.SEVERE, null, ioe);
            return;
        }

        String source = this.buildSource();
        String group = this.groupManager.getGroup();
        String newTarget = transaction.getSource();
        String deviceStringToSend = DeviceWrapper.toJSON(deviceToSend);
        this.lastRemovedDevice = deviceStringToSend;

        Transaction transactionRequest = new Request(
                source,
                group,
                deviceStringToSend,
                newTarget
        );
        this.sendTransaction(transactionRequest);
        this.startLBReplyTimeout();

    }

    private void processWhenLastTransactionIsLBEntryReply(Transaction transaction) {
        boolean isLBRequest = transaction.getType().equals(TransactionType.LB_REQUEST);
        if (!isLBRequest) {
            String message = "The new transaction [{0}] will not be processed. Because is not LB_REQUEST";
            logger.log(Level.WARNING, message, transaction.getType());
            return;
        }
        if (!this.currentGatewayIsTarget(transaction)) {
            logger.info("The new transaction will not be processed. Because the current gateway is not the target.");
            return;
        }
        logger.log(Level.INFO, "Processing the new transaction [{0}].", transaction.getType());
        timerTaskGateWay.cancel();

        // Carregar dispositivo na lista.
        String device = ((Request) transaction).getDevice();
        this.loadSwapReceberDispositivo(device);

        String source = this.buildSource();
        String group = this.groupManager.getGroup();
        String newTarget = transaction.getSource();
        // Enviar LB_REPLY.
        Transaction transactionReply = new Reply(source, group, newTarget);
        this.sendTransaction(transactionReply);

        // Colocar para a última transação ser nula.
        this.lastTransaction = null;
    }

    public void processWhenLastTransactionIsLBRequest(Transaction transaction) {
        boolean isLBReply = transaction.getType().equals(TransactionType.LB_REPLY);
        if (!isLBReply) {
            String message = "The new transaction [{0}] will not be processed. Because is not LB_REQUEST";
            logger.log(Level.WARNING, message, transaction.getType());
            return;
        }
        if (!this.currentGatewayIsTarget(transaction)) {
            logger.info("The new transaction will not be processed. Because the current gateway is not the target.");
            return;
        }
        logger.log(Level.INFO, "Processing the new transaction [{0}].", transaction.getType());
        timerTaskGateWay.cancel();

        try {
            this.removeFirstDevice(transaction.getSource().split("/")[2]);
        } catch (MqttException me) {
            logger.info("Load Balancer - Error! Unable to remove the first device.");
            logger.log(Level.SEVERE, null, me);
            return;
        }
        String source = this.buildSource();
        String group = this.groupManager.getGroup();
        String newTarget = transaction.getSource();

        // Enviar LBDevice.
        Transaction transactionDevice = new LBDevice(
                source,
                group,
                this.lastRemovedDevice,
                newTarget
        );

        this.sendTransaction(transactionDevice);

        // Colocar para a última transação ser nula.
        this.lastTransaction = null;

    }

    private void processTransactions(Transaction transaction) {
        logger.info("Load balancer - Processing the new transaction");
        logger.log(Level.INFO, "Load balancer - The last transaction type is {0}", lastTransaction.getType());
        switch (lastTransaction.getType()) {
            case LB_ENTRY:
                this.processWhenLastTransactionIsLBEntry(transaction);
                break;
            case LB_ENTRY_REPLY:
                this.processWhenLastTransactionIsLBEntryReply(transaction);
                break;
            case LB_REQUEST:
                this.processWhenLastTransactionIsLBRequest(transaction);
                break;
            default:
                logger.info("Error! Something went wrong. - Default case");
                break;
        }
    }

    private void startLBReplyTimeout() {
        timerTaskGateWay
                = new TimerTask() {
            @Override
            public void run() {
                Logger log = Logger.getLogger(Balancer.class.getName());

                timerPass = timerPass + 1;
                log.info(String.valueOf(timerPass));

                if ((timerPass * 1000) >= TIMEOUT_LB_REPLY) {
                    lastTransaction = null;
                    timerTaskGateWay.cancel();
                    log.info("TIME'S UP");
                }
            }
        };

        timerPass = 0;
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(timerTaskGateWay, 1000, 1000);
    }

    private void executeTimeOutLB() {
        timerTaskLb
                = new TimerTask() {
            @Override
            public void run() {
                Logger log = Logger.getLogger(Balancer.class.getName());

                timerPass = timerPass + 1;
                log.info(String.valueOf(timerPass));

                if ((timerPass * 1000) >= TIMEOUT_LB_REPLY) {
                    lastTransaction = null;
                    timerTaskLb.cancel();
                    log.info("TIME'S UP");
                }
            }
        };

        timerPass = 0;
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(timerTaskLb, 1000, 1000);
    }

    private void sendTransaction(Transaction transaction) {
        try {
            this.connector.put(transaction);

            this.lastTransaction = transaction;
        } catch (InterruptedException ie) {
            logger.info("Load Balancer - Error commit transaction.");
            logger.log(Level.SEVERE, null, ie);
        }
    }

    private void resendTransaction() {
        try {
            this.resent += 1;
            if (this.resent == 3) {
                this.lastTransaction = null;
                this.resent = 0;
                return;
            }

            this.connector.put(lastTransaction);
            Timer timer = new Timer();
            timerPass = 0;
            timer.scheduleAtFixedRate(timerTaskLb, 1000, 1000);
        } catch (InterruptedException ie) {
            logger.info("Load Balancer - Error commit transaction.");
            logger.log(Level.SEVERE, null, ie);
        }
    }

    private String buildSource() {
        return new StringBuilder(this.groupManager.getGroup())
                .append("/")
                .append(this.idManager.getIP())
                .toString();
    }

    public void removeFirstDevice(String targetIp) throws MqttException {
        try {
            List<Device> allDevices = deviceManager.getAllDevices();

            if (allDevices.isEmpty()) {
                logger.warning("Load balancer - It's not possible to remove device. Because there is note devices in this gateway.");
                return;
            }
            Device deviceARemover = allDevices.get(0);
            JsonObject jsonPublish = new JsonObject();
            jsonPublish.addProperty("id", deviceARemover.getId());
            jsonPublish.addProperty("url", "tcp://" + targetIp);
            jsonPublish.addProperty("port", deviceARemover.getId());
            jsonPublish.addProperty("user", "karaf");
            jsonPublish.addProperty("password", "karaf");

            String response = "SET VALUE brokerMqtt{" + jsonPublish.toString() + "}";
            publisher.publish(deviceARemover.getId(), response);

            deviceManager.removeDevice(allDevices.get(0).getId());
        } catch (IOException ioe) {
            logger.info("Error! To retrieve device list or to remove the first device.");
            logger.log(Level.SEVERE, null, ioe);
        }
    }

    public void loadSwapReceberDispositivo(String deviceJSON) {
        logger.info("Load balancer - Receiving new device");
        logger.log(Level.INFO, "DeviceJSON: {0}", deviceJSON);
        Device device = DeviceWrapper.toDevice(deviceJSON);
        logger.log(Level.INFO, "Device after convert: {0}", DeviceWrapper.toJSON(device));
        try {
            deviceManager.addDevice(device);
            logger.info("Load balancer - New device successfully added.");
        } catch (IOException ioe) {
            logger.info("Error! To add a new device to the list.");
            logger.log(Level.SEVERE, null, ioe);
        }
    }

    @Override
    public void update(Object object) {
        if (object == null) {
            logger.warning("Load balancer - New message - The message is null.");
            return;
        }
        if (!(object instanceof String)) {
            logger.warning("Load balancer - New message - Is not String type");
            String messageType = object.getClass().getName();
            logger.log(Level.WARNING, "Load balancer - New message - message type: {0}", messageType);
            return;
        }
        String hashTransaction = (String) object;

        Transaction transaction = this.connector.getTransactionByHash(hashTransaction);
        if (!this.isRecentTransaction(transaction)) {
            logger.warning("Load balancer - The received message is old");
            return;
        }

        logger.info("Load balancer - New message - Valid");
        logger.log(Level.INFO, "HashTransaction: {0}", hashTransaction);

        if (lastTransaction == null) {
            logger.info("Load balancer - Last transaction is Null");
            this.messageArrived(transaction);
            return;
        }
        boolean isLoopbackMessage = transaction.getSource().equals(this.buildSource());
        if (isLoopbackMessage) {
            logger.info("The new message will not be processed. because is a loopback.");
            return;
        }
        // Apenas processar mensagens de outros gateways
        this.processTransactions(transaction);
    }

    private boolean isRecentTransaction(Transaction transaction) {
        return System.currentTimeMillis() - transaction.getPublishedAt() < 15000;
    }

    /* Thread para verificar o IP do client tangle e se inscrever nos tópicos
    de interesse.*/
    @Override
    public void run() {
        try {
            String url = this.connector.getLedgerWriter().getUrl();
            InetAddress inet = InetAddress.getByName(url);

            if (!inet.isReachable(3000)) {
                this.flagSubscribe = true;
                return;
            }
            if (!this.flagSubscribe) {
                return;
            }
            this.subscribedTopics
                    .forEach(topic -> this.connector.subscribe(topic, this));
            this.flagSubscribe = false;

        } catch (UnknownHostException ex) {
            logger.info("Error! Unknown Host.");
            logger.log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            logger.info("Error! Can't connect to InetAddress.");
            logger.log(Level.SEVERE, null, ex);
        }
    }
}
