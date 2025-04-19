package dlt.load.balancer.model;

import br.uefs.larsid.extended.mapping.devices.services.IDevicePropertiesManager;
import br.uefs.larsid.extended.mapping.devices.tatu.DeviceWrapper;
import br.ufba.dcc.wiser.soft_iot.entities.Device;
import com.google.gson.JsonObject;
import dlt.auth.services.IPublisher;
import dlt.client.tangle.hornet.enums.TransactionType;
import static dlt.client.tangle.hornet.enums.TransactionType.LB_ENTRY;
import static dlt.client.tangle.hornet.enums.TransactionType.LB_ENTRY_REPLY;
import static dlt.client.tangle.hornet.enums.TransactionType.LB_REQUEST;
import dlt.client.tangle.hornet.model.transactions.LBDevice;
import dlt.client.tangle.hornet.model.transactions.LBMultiDevice;
import dlt.client.tangle.hornet.model.transactions.LBMultiDeviceResponse;
import dlt.client.tangle.hornet.model.transactions.LBMultiRequest;
import dlt.client.tangle.hornet.model.transactions.LBMultiResponse;
import dlt.client.tangle.hornet.model.transactions.LBReply;
import dlt.client.tangle.hornet.model.transactions.Reply;
import dlt.client.tangle.hornet.model.transactions.Request;
import dlt.client.tangle.hornet.model.transactions.Status;
import dlt.client.tangle.hornet.model.transactions.TargetedTransaction;
import dlt.client.tangle.hornet.model.transactions.Transaction;
import dlt.client.tangle.hornet.services.ILedgerSubscriber;
import dlt.id.manager.services.IDLTGroupManager;
import dlt.id.manager.services.IIDManagerService;
import java.io.IOException;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.paho.client.mqttv3.MqttException;

/**
 *
 * @author Allan Capistrano, Antonio Crispim, Uellington Damasceno
 * @version 1.2.4
 */
public class Balancer implements ILedgerSubscriber, Runnable {

    private final ScheduledExecutorService executor;
    private LedgerConnector connector;

    private Transaction lastThirdPartyTrans;
    private IDevicePropertiesManager deviceManager;
    private IIDManagerService idManager;
    private IDLTGroupManager groupManager;
    private IPublisher iPublisher;

    private TimerTask timerTaskLB;
    private TimerTask timerTaskGateway;

    private int timerPass;
    private int currentResendAttempt;

    private Status internalStatus;
    private String lastRemovedDevice;
    private boolean isSubscribed;

    private final BalancerConfigs configs;
    private static final Logger logger = Logger.getLogger(Balancer.class.getName());

    public Balancer() {

        this.buildTimerResendTransaction();

        this.currentResendAttempt = 0;
        this.isSubscribed = false;

        this.configs = new BalancerConfigs();

        this.executor = Executors.newSingleThreadScheduledExecutor();
    }

    public void buildTimerTaskLB() {
        timerTaskLB = new TimerTask() {
            @Override
            public void run() {
                Logger log = Logger.getLogger(Balancer.class.getName());

                timerPass = timerPass + 1;
                log.info(String.valueOf(timerPass));

                if ((timerPass * 1000) >= configs.getTIMEOUT_LB_REPLY()) {
                    lastThirdPartyTrans = null;
                    timerTaskLB.cancel();
                    log.info("TIME'S UP buildTimerTaskLB");
                }
            }
        };
    }

    public void buildTimerResendTransaction() {
        timerTaskGateway = new TimerTask() {
            @Override
            public void run() {
                Logger log = Logger.getLogger(Balancer.class.getName());

                timerPass = timerPass + 1;
                log.info(String.valueOf(timerPass));

                if ((timerPass * 1000) >= configs.getTimeoutGateway()) {
                    tryResendLastTransaction();
                    timerTaskGateway.cancel();
                    log.info("TIME'S UP buildTimerResendTransaction");
                }
            }
        };
    }

    private void startTimeoutToCleanLastTransaction() {
        timerTaskGateway = new TimerTask() {
            @Override
            public void run() {
                Logger log = Logger.getLogger(Balancer.class.getName());

                timerPass = timerPass + 1;
                log.info(String.valueOf(timerPass));

                if ((timerPass * 1000) >= configs.getTIMEOUT_LB_REPLY()) {
                    lastThirdPartyTrans = null;
                    timerTaskGateway.cancel();
                    log.info("TIME'S UP executeTimeOutGateWay");
                }
            }
        };

        timerPass = 0;
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(timerTaskGateway, 1000, 1000);
    }

    private void startLoadBalancerOrderTimeout() {
        timerTaskLB = new TimerTask() {
            @Override
            public void run() {
                Logger log = Logger.getLogger(Balancer.class.getName());

                timerPass = timerPass + 1;
                log.info(String.valueOf(timerPass));

                if ((timerPass * 1000) >= configs.getTIMEOUT_LB_REPLY()) {
                    log.info("TIME'S UP executeTimeOutLB");
                    if (configs.isMultiLayerBalancing()) {
                        log.info("Send multi-layer balancing request.");
                        Transaction trans = new LBMultiRequest(buildSource(), groupManager.getGroup());
                        sendTransaction(trans);
                    } else {
                        lastThirdPartyTrans = null;
                    }
                    timerTaskLB.cancel();
                }
            }
        };

        timerPass = 0;
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(timerTaskLB, 1000, 1000);
    }

    public void setPublisher(IPublisher iPublisher) {
        this.iPublisher = iPublisher;
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

    public void start() {
        logger.info(this.configs.toString());
        this.executor.scheduleAtFixedRate(this, 0, 5, TimeUnit.SECONDS); 
    }

    public void stop() {
        this.isSubscribed = false;

        this.configs
                .getInterestedTopics()
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

    private void processBalancerStartTransactions(Transaction transaction, String source) {

        if (!transaction.is(TransactionType.LB_ENTRY)
                && !transaction.is(TransactionType.LB_MULTI_DEVICE_REQUEST)) {
            logger.log(Level.INFO, "Transação ignorada. Tipo não é LB_ENTRY nem LB_MULTI_DEVICE_REQUEST.");
            return;
        }

        if (transaction.isLoopback(source)) {
            logger.info("Solicitação interna de balancemaento iniciada.");
            this.lastThirdPartyTrans = transaction;
            startLoadBalancerOrderTimeout();
            return;
        }

        if (!this.internalStatus.getAvailable()) {
            logger.info("Gateway indisponível para receber novos devices.");
            return;
        }

        TransactionType type = transaction.getType();
        String group = this.groupManager.getGroup();

        String newTarget = transaction.getSource();

        Transaction transactionReply = type.isMultiLayer()
                ? new LBMultiResponse(source, group, newTarget)
                : new LBReply(source, group, newTarget);

        this.sendTransaction(transactionReply);
    }

    private void handleLBEntryReply(Transaction trans, String reciverAddress, String group) {
        if (!trans.is(TransactionType.LB_ENTRY_REPLY)) {
            logger.info("Transação ignorada. Esperado LB_ENTRY_REPLY.");
            return;
        }

        if (!((TargetedTransaction) trans).isSameTarget(reciverAddress)) {
            logger.info("LB_ENTRY_REPLY com target diferente. Timeout reiniciado.");
            timerTaskGateway.cancel();
            startTimeoutToCleanLastTransaction();
            return;
        }

        logger.info("LB_ENTRY_REPLY recebido com target correto. Iniciando envio de LB_REQUEST.");

        timerTaskLB.cancel();
        timerTaskGateway.cancel();

        try {
            Device deviceToSend = this.deviceManager.getAllDevices().get(0);
            String deviceStringToSend = DeviceWrapper.toJSON(deviceToSend);
            this.lastRemovedDevice = deviceStringToSend;

            logger.log(Level.INFO, "Device selecionado para envio: {0}", deviceStringToSend);

            Transaction transactionRequest = new Request(reciverAddress, group, deviceStringToSend, trans.getSource());
            this.sendTransaction(transactionRequest);

            logger.info("LB_REQUEST enviado com sucesso.");
            timerTaskGateway.cancel();
            startTimeoutToCleanLastTransaction();
        } catch (IOException ioe) {
            logger.info("Erro ao obter lista de devices.");
            ioe.printStackTrace();
        }
    }

    private void handleLBRequest(Transaction trans, String reciverAddress, String group) {
        if (!trans.is(TransactionType.LB_REQUEST)) {
            logger.info("Transação ignorada. Esperado LB_REQUEST.");
            return;
        }

        if (!((TargetedTransaction) trans).isSameTarget(reciverAddress)) {
            logger.info("LB_REQUEST com target errado. Timeout reiniciado.");
            timerTaskGateway.cancel();
            startTimeoutToCleanLastTransaction();
            return;
        }
        logger.info("LB_REQUEST recebido corretamente. Adicionando device local.");

        timerTaskGateway.cancel();

        String device = ((Request) trans).getDevice();
        this.receiveNewDevice(device);

        Transaction transactionReply = new Reply(reciverAddress, group, trans.getSource());
        this.sendTransaction(transactionReply);

        logger.info("LB_REPLY enviado. Resetando lastTransaction.");
        this.lastThirdPartyTrans = null;
    }

    private void handleLBReply(Transaction trans, String revicerAddress, String group) {
        if (!trans.is(TransactionType.LB_REPLY)) {
            logger.info("Transação ignorada. Esperado LB_REPLY.");
            return;
        }

        if (!((TargetedTransaction) trans).isSameTarget(revicerAddress)) {
            logger.info("LB_REPLY com target errado. Timeout reiniciado.");
            timerTaskGateway.cancel();
            startTimeoutToCleanLastTransaction();
            return;
        }

        logger.info("LB_REPLY recebido. Removendo device do gateway local.");

        timerTaskGateway.cancel();
        String ip = trans.getSource().split("/")[2];
        String port = trans.getSource().split("/")[3];

        logger.log(Level.INFO, "IP destino: {0}, Porta destino: {1}", new Object[]{ip, port});

        try {
            this.sendDevice(ip, port);

            Transaction transactionDevice = new LBDevice(revicerAddress, group, this.lastRemovedDevice, trans.getSource());
            this.sendTransaction(transactionDevice);

            logger.info("LB_DEVICE enviado. Resetando lastTransaction.");
            this.lastThirdPartyTrans = null;
        } catch (MqttException me) {
            logger.info("Erro ao remover device.");
            me.printStackTrace();
        }
    }

    private void handleLBMultiResponse(Transaction trans, String reciverAddress, String group) {
        if (!trans.is(TransactionType.LB_MULTI_RESPONSE)) {
            logger.info("Transação ignorada. Esperado LB_MULTI_RESPONSE.");
            return;
        }

        if (!((TargetedTransaction) trans).isSameTarget(reciverAddress)) {
            logger.info("LB_MULTI_RESPONSE com target errado. Timeout reiniciado.");
            timerTaskGateway.cancel();
            startTimeoutToCleanLastTransaction();
        }

        logger.info("Preparando envio de LB_MULTI_DEVICE.");

        timerTaskLB.cancel();
        timerTaskGateway.cancel();

        try {
            Device deviceToSend = this.deviceManager.getAllDevices().get(0);
            String deviceStringToSend = DeviceWrapper.toJSON(deviceToSend);
            this.lastRemovedDevice = deviceStringToSend;

            logger.log(Level.INFO, "Device selecionado: {0}", deviceStringToSend);

            Transaction transactionRequest = new LBMultiDevice(reciverAddress, group, deviceStringToSend, trans.getSource());
            this.sendTransaction(transactionRequest);

            logger.info("LB_MULTI_DEVICE enviado com sucesso.");
            timerTaskGateway.cancel();
            startTimeoutToCleanLastTransaction();
        } catch (IOException ioe) {
            logger.info("Erro ao acessar device list.");
            ioe.printStackTrace();
        }
    }

    private void handleLBMultiDeviceRequest(Transaction trans, String reciverAddres, String group) {
        if (!trans.is(TransactionType.LB_MULTI_DEVICE_REQUEST)) {
            logger.info("Transação ignorada. Esperado LB_MULTI_DEVICE_REQUEST.");
            return;
        }

        if (!((TargetedTransaction) trans).isSameTarget(reciverAddres)) {
            logger.info("LB_MULTI_DEVICE_REQUEST com target errado. Timeout reiniciado.");
            timerTaskGateway.cancel();
            startTimeoutToCleanLastTransaction();
            return;
        }
        logger.info("Adicionando device local.");

        timerTaskGateway.cancel();

        String device = ((LBMultiDevice) trans).getDevice();
        this.receiveNewDevice(device);

        Transaction response = new LBMultiDeviceResponse(reciverAddres, group, device, trans.getSource());
        this.sendTransaction(response);

        logger.info("LB_MULTI_DEVICE_RESPONSE enviado. Resetando lastTransaction.");
        this.lastThirdPartyTrans = null;

    }

    private void handleLBMultiDeviceResponse(Transaction trans, String reciverAddress) {
        if (!trans.is(TransactionType.LB_MULTI_DEVICE_RESPONSE)) {
            logger.info("Transação ignorada. Esperado LB_MULTI_DEVICE_RESPONSE.");
            return;
        }

        if (!((TargetedTransaction) trans).isSameTarget(reciverAddress)) {
            logger.info("LB_MULTI_DEVICE_RESPONSE com target errado. Timeout reiniciado.");
            timerTaskGateway.cancel();
            startTimeoutToCleanLastTransaction();
            return;
        }

        logger.info("Removendo device do gateway local.");

        timerTaskGateway.cancel();

        String ip = trans.getSource().split("/")[2];
        String port = trans.getSource().split("/")[3];

        logger.log(Level.INFO, "IP destino: {0}, Porta destino: {1}", new Object[]{ip, port});
        try {

            this.sendDevice(ip, port);

            this.lastThirdPartyTrans = null;
        } catch (MqttException me) {
            logger.info("Erro ao remover device.");
            me.printStackTrace();
        }
    }

    private void processThirdPartyTransactions(Transaction trans, String source) {

        String group = this.groupManager.getGroup();
        TransactionType lastType = lastThirdPartyTrans.getType();
        logger.log(Level.INFO, "Última transação registrada: {0}", lastType);

        switch (lastType) {
            case LB_ENTRY:
                handleLBEntryReply(trans, source, group);
                break;

            case LB_ENTRY_REPLY:
                this.handleLBRequest(trans, source, group);
                break;

            case LB_REQUEST:
                this.handleLBReply(trans, source, group);
                break;

            case LB_MULTI_REQUEST:
                this.handleLBMultiResponse(trans, source, group);
                break;

            case LB_MULTI_RESPONSE:
                this.handleLBMultiDeviceRequest(trans, source, group);
                break;

            case LB_MULTI_DEVICE_REQUEST:
                this.handleLBMultiDeviceResponse(trans, source);
                break;

            case LB_MULTI_DEVICE_RESPONSE:
                logger.info("Transação LB_MULTI_DEVICE_RESPONSE recebida. Nada a fazer.");
                break;

            default:
                logger.info("Tipo de transação não reconhecido ou não tratado.");
                break;
        }
    }

    private void sendTransaction(Transaction transaction) {
        try {
            this.connector.put(transaction);
            this.lastThirdPartyTrans = transaction;
        } catch (InterruptedException ex) {
            logger.info("Load Balancer - Error commit transaction.");
            Logger.getLogger(Balancer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void tryResendLastTransaction() {
        this.currentResendAttempt++;

        if (this.currentResendAttempt == this.configs.getMaxTryResendTransaction()) {
            this.lastThirdPartyTrans = null;
            this.currentResendAttempt = 0;
            return;
        }

        try {
            this.connector.put(lastThirdPartyTrans);
            Timer timer = new Timer();
            timerPass = 0;
            timer.scheduleAtFixedRate(timerTaskLB, 1000, 1000);
        } catch (InterruptedException ie) {
            logger.info("Load Balancer - Error commit transaction.");
            Logger.getLogger(Balancer.class.getName()).log(Level.SEVERE, null, ie);

        }
    }

    public void sendDevice(String targetIp, String targetMqttPort) throws MqttException {
        try {
            List<Device> allDevices = deviceManager.getAllDevices();

            if (allDevices.isEmpty()) {
                logger.warning("Foi solicitado a remoção de um dispositivo mas a lista está vazia.");
                return;
            }

            Device device = allDevices.get(0);
            JsonObject jsonPublish = new JsonObject();
            jsonPublish.addProperty("id", device.getId());
            jsonPublish.addProperty("url", "tcp://" + targetIp);
            jsonPublish.addProperty("port", targetMqttPort);
            jsonPublish.addProperty("user", "karaf");
            jsonPublish.addProperty("password", "karaf");

            iPublisher.publish(
                    device.getId(),
                    "SET VALUE brokerMqtt{" + jsonPublish.toString() + "}"
            );

            deviceManager.removeDevice(device.getId());

        } catch (IOException ioe) {
            logger.info("Error! To retrieve device list or to remove the first device.");
            ioe.printStackTrace();
        }
    }

    public void receiveNewDevice(String deviceJSON) {
        try {
            Device device = DeviceWrapper.toDevice(deviceJSON);
            logger.log(Level.INFO, "Device after convert: {0}", DeviceWrapper.toJSON(device));

            deviceManager.addDevice(device);
        } catch (IOException ioe) {
            logger.info("Error! To add a new device to the list.");
            ioe.printStackTrace();
        }
    }

    @Override
    public void update(Object trans, Object messageId) {

        if (!this.configs.isBalanceable()) {
            logger.log(Level.INFO, "Load balancer - New message but will not processed because is not balanceable.");
            return;
        }

        if (!(trans instanceof Transaction)) {
            logger.log(Level.INFO, "Load balancer - New message with id: {0} is Invalid", messageId);
            return;
        }

        Transaction transaction = (Transaction) trans;

        if (transaction.isMultiLayerTransaction() && !this.configs.isMultiLayerBalancing()) {
            logger.info("Load balancer - Multilayer message type not allowed.");
            return;
        }

        String source = this.buildSource();
        boolean isLoopback = transaction.isLoopback(source);
        boolean isStatusTransaction = transaction.is(TransactionType.LB_STATUS);

        // Mensagem muito antiga, ignora
        if (!this.configs.validPublishMessageInterval(transaction.getPublishedAt())) {
            logger.log(Level.WARNING, "{0} foi é considerada antiga.", transaction);
            return;
        }

        if (!isLoopback) {
            logger.log(Level.INFO, "\n{0}\n", transaction);
        }

        if (lastThirdPartyTrans == null) {
            if (isStatusTransaction && transaction.isLoopback(source)) {
                logger.info("Atualizando status interno.");
                this.internalStatus = (Status) transaction;
                return;
            }
            this.processBalancerStartTransactions(transaction, source);
            return;
        }

        if (isLoopback || isStatusTransaction) {
            return;
        }

        this.processThirdPartyTransactions(transaction, source);
    }

    /* Thread para verificar o IP do client tangle e se inscrever nos tópicos
    de interesse.*/
    @Override
    public void run() {
        try {
            String ledgerUrl = this.connector.getLedgerWriter().getUrl();

            URL urlObj = new URL(ledgerUrl);
            String host = urlObj.getHost();
            InetAddress inet = InetAddress.getByName(host);

            if (!inet.isReachable(5000)) {
                logger.log(Level.WARNING, "Host inacessível: {0}. flagSubscribe setado para true.", host);
                this.isSubscribed = false;
            }

            if (this.isSubscribed) {
                return;
            }

            this.configs
                    .getInterestedTopics()
                    .forEach(topic -> {
                        logger.log(Level.INFO, "Inscrevendo-se no tópico: {0}", topic);
                        this.connector.subscribe(topic, this);
                    });

            this.isSubscribed = true;
            logger.info("Inscrição concluída. flagSubscribe setado para false.");

        } catch (UnknownHostException uhe) {
            logger.log(Level.SEVERE, "Erro: Host desconhecido.", uhe);
            this.isSubscribed = false;
        } catch (IOException ioe) {
            logger.log(Level.SEVERE, "Erro: Falha ao conectar com o endereço.", ioe);
            this.isSubscribed = false;
        }
    }

    private String buildSource() {
        return new StringBuilder(this.groupManager.getGroup())
                .append("/")
                .append(this.idManager.getIP())
                .append("/")
                .append(this.configs.getMqttPort())
                .toString();
    }
}