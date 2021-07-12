package dlt.load.balancer.model;

import br.uefs.larsid.extended.mapping.devices.services.IDevicePropertiesManager;
import br.uefs.larsid.extended.mapping.devices.tatu.DeviceWrapper;
import br.ufba.dcc.wiser.soft_iot.entities.Device;
import dlt.client.tangle.enums.TransactionType;
import dlt.client.tangle.model.Transaction;
import dlt.client.tangle.services.ILedgerSubscriber;
import dlt.id.manager.services.IIDManagerService;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import static java.util.stream.Collectors.toList;
import org.json.JSONArray;

/**
 *
 * @author Uellington Damasceno
 * @version 0.0.1
 */
public class Balancer implements ILedgerSubscriber {

    private LedgerConnector connector;
    
    private final Long TIMEOUT_LB_REPLY; //Pode ser usando como parametro no arq .cfg?
    private final Long TIMEOUT_GATEWAY;
    
    private Transaction lastTransaction;
    private List<String> subscribedTopics;
    
    private IDevicePropertiesManager deviceManager;
    private IIDManagerService idManager;
    
    public Balancer(long timeoutLB, long timeoutGateway) {
        this.TIMEOUT_LB_REPLY = timeoutLB;
        this.TIMEOUT_GATEWAY = timeoutGateway;
    }

    public void setDeviceManager(IDevicePropertiesManager deviceManager) {
        this.deviceManager = deviceManager;
    }

    public void setConnector(LedgerConnector connector) {
        this.connector = connector;
    }
    
    public void setIdManager(IIDManagerService idManager){
        this.idManager = idManager;
    }

    public void setSubscribedTopics(String topicsJSON){
        this.subscribedTopics = new JSONArray(topicsJSON)
                .toList()
                .stream()
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .collect(toList());
    }

    public void start() {
        this.subscribedTopics
                .forEach(topic -> this.connector.subscribe(topic, this));
    }

    public void stop() {
        this.subscribedTopics
                .forEach(topic -> this.connector.unsubscribe(topic, this));
    }
    /**
     * LB_REQUEST -> swap !3Status
     * LB_ENTRY_REPLY -> !3Status !device swap
     * LB_REPLAY = LB_ENTRY_REPLY
     */
    private void messageArrived(String hashTransaction) {
        Transaction transaction = this.connector.getTransactionByHash(hashTransaction); //Pode refatorar

        if (lastTransaction != null) {
            processTransactions(transaction);
        } else {
            System.out.println("AGUARDANDO ENTRY");
            System.out.println("Load  - Last transaction is Null");
            if (transaction.getType().equals(TransactionType.LB_ENTRY)) {
                System.out.println("RECEBI LB_ENTRY");
                if (transaction.getSource().equals(this.idManager.getID())) { //Verificar viabilidade de inserir um serviço de atualização para o monitor.
                    this.lastTransaction = transaction;
                } else {
                        Transaction transactionReply = new Transaction();
                        
                        transactionReply.setSource(this.idManager.getID());
                        transactionReply.setTarget(transaction.getSource());
                        transactionReply.setTimestamp(System.currentTimeMillis());
                        transactionReply.setType(TransactionType.LB_ENTRY_REPLY);
                        
                        this.sendTransaction(transactionReply);
                }
            }
        }
    }

    private void processTransactions(Transaction transaction) {
        System.out.println("PROCESSANDO MENSAGEM.....");
        System.out.println("Tipo da Mensagem que chegou: "+ transaction.getType());
        System.out.println("Tipo da utima mensagem: "+ lastTransaction.getType());
        switch (lastTransaction.getType()) {
            case LB_ENTRY:{
                /*PEDI E CONSEGUI RESPOSTA -> ENVIAR*/
                if (transaction.getType() == TransactionType.LB_ENTRY_REPLY 
                        && transaction.getTarget().equals(this.idManager.getID())) {
                    System.out.println("RECEBI LB_ENTRY_REPLY");
                    try {
                        Transaction transactionReply = new Transaction();
                        
                        transactionReply.setSource(this.idManager.getID());
                        transactionReply.setTarget(transaction.getSource());
                        transactionReply.setTimestamp(System.currentTimeMillis());
                        transactionReply.setType(TransactionType.LB_REQUEST);
                        
                        Device device = this.deviceManager.getAllDevices().get(0);
                        String deviceString = DeviceWrapper.toJSON(device);
                        
                        System.out.println("DEVICE STRING");
                        System.out.println(deviceString);
                        transactionReply.setDeviceSwap(deviceString);
                        
                        this.sendTransaction(transactionReply);
                    } catch (IOException ex) {
                        System.out.println("Load Balancer - Não foi possível recuperar a lista de dispositivos.");
                    }
                    
                } else { //Thread -> Timeout
                    Long timeAtual = System.currentTimeMillis();
                    if ((timeAtual - lastTransaction.getTimestamp()) >= TIMEOUT_LB_REPLY) {
                        resendTransaction();
                    } 
                }  
                break;
            }
            case LB_ENTRY_REPLY:{
                /*Recebi o device*/
                if (transaction.getType() == TransactionType.LB_REQUEST
                        && transaction.getTarget().equals(this.idManager.getID())) {
                        System.out.println("RECEBI LB_REQUEST");
                        
                        Transaction transactionReply = new Transaction();
                        transactionReply.setSource(this.idManager.getID());
                        transactionReply.setTarget(transaction.getSource());
                        transactionReply.setTimestamp(System.currentTimeMillis());
                        transactionReply.setType(TransactionType.LB_REPLY);
                        
                        this.loadSwapReceberDispositivo(transaction.getDeviceSwap());
                        this.sendTransaction(transactionReply); // Iniciar meu contador de timeout
                    
                } else {
                    Long timeAtual = System.currentTimeMillis();
                    if ((timeAtual - lastTransaction.getTimestamp()) >= TIMEOUT_GATEWAY) {
                        System.out.println("ANULOOU >>>>>>>>>>>");
                        lastTransaction = null;
                    } 
                }   break;
            }
            /**/
            case LB_REQUEST:{
                if (transaction.getType() == TransactionType.LB_REPLY 
                        && transaction.getTarget().equals(this.idManager.getID())) {
                    System.out.println("RECEBI LB_REPLY");
                    this.removeFirstDevice();
                    this.lastTransaction = transaction;
                } else {
                    Long timeAtual = System.currentTimeMillis();
                    if ((timeAtual - lastTransaction.getTimestamp()) >= TIMEOUT_GATEWAY) {
                        resendTransaction();
                    } 
                } 
                break;
            }
            default:{
                System.out.println("Chegou algo que não deveria!");
                break;
            }
        }
    }

    private void sendTransaction(Transaction transaction){
        try {
            this.connector.put(transaction);
            this.lastTransaction = transaction;
        } catch (InterruptedException ex) {
            System.out.println("Load Balancer - Error commit transaction - InterruptedException");
        }
    }
    
    private void resendTransaction() {
        this.lastTransaction.setTimestamp(System.currentTimeMillis());
        try {
            this.connector.put(lastTransaction);
        } catch (InterruptedException ex) {
            System.out.println("Load Balancer - Error commit transaction - InterruptedException");
        }
    }

    public void removeFirstDevice() { //Refatorar/ pegar primeiro da lista.
        try {
            List<Device> allDevices = deviceManager.getAllDevices();
            if(!allDevices.isEmpty()){
                deviceManager.removeDevice(allDevices.get(0).getId());
            }
        } catch (IOException ex) {
            Logger.getLogger(Balancer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void loadSwapReceberDispositivo(String deviceJSON) {
        try {
            System.out.println("DeviceJSON: "+ deviceJSON);
            Device device = DeviceWrapper.toDevice(deviceJSON);
            System.out.println("Device after convert: "+DeviceWrapper.toJSON(device));
            deviceManager.addDevice(device);
        } catch (IOException ex) {
            Logger.getLogger(Balancer.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Override
    public void update(Object object) {
        if(object instanceof String){
            System.out.println("Load balancer - New message - Válid");
            String hashTransaction = (String) object;
            System.out.println("HashTransaction: "+ hashTransaction);
            this.messageArrived(hashTransaction);
        }else{
            System.out.println("Load balancer - New message - Invalid");
        }
    }
}
