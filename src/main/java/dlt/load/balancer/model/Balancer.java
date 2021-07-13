package dlt.load.balancer.model;

import br.uefs.larsid.extended.mapping.devices.services.IDevicePropertiesManager;
import br.uefs.larsid.extended.mapping.devices.tatu.DeviceWrapper;
import br.ufba.dcc.wiser.soft_iot.entities.Device;
import dlt.client.tangle.enums.TransactionType;
import static dlt.client.tangle.enums.TransactionType.LB_ENTRY;
import static dlt.client.tangle.enums.TransactionType.LB_ENTRY_REPLY;
import static dlt.client.tangle.enums.TransactionType.LB_REQUEST;
import dlt.client.tangle.model.transactions.LBReply;
import dlt.client.tangle.model.transactions.Request;
import dlt.client.tangle.model.transactions.TargetedTransaction;
import dlt.client.tangle.model.transactions.Transaction;
import dlt.client.tangle.services.ILedgerSubscriber;
import dlt.id.manager.services.IDLTGroupManager;
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
    
    private final Long TIMEOUT_LB_REPLY;
    private final Long TIMEOUT_GATEWAY;
    
    private Transaction lastTransaction;
    private List<String> subscribedTopics;
    
    private IDevicePropertiesManager deviceManager;
    private IIDManagerService idManager;
    private IDLTGroupManager groupManager;
    
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
    
    public void setGroupManager(IDLTGroupManager groupManager){
        this.groupManager = groupManager;
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

    private void messageArrived(Transaction transaction) {
            System.out.println("AGUARDANDO ENTRY");
            System.out.println("Load  - Last transaction is Null");
            if (!transaction.getType().equals(TransactionType.LB_ENTRY)) {
                System.out.println("RECEBI LB_ENTRY");
                if (transaction.getSource().equals(this.idManager.getIP())) { //Verificar viabilidade de inserir um serviço de atualização para o monitor.
                    this.lastTransaction = transaction;
                } else {
                        String source = this.idManager.getIP();
                        String group = this.groupManager.getGroup();
                        String newTarget = transaction.getSource();
                        
                        Transaction transactionReply = new LBReply(source, group, newTarget);
                        this.sendTransaction(transactionReply);
                }
            }
    }

    private void processTransactions(Transaction transaction) {
        System.out.println("PROCESSANDO MENSAGEM.....");
        System.out.println("Tipo da Mensagem que chegou: "+ transaction.getType());
        System.out.println("Tipo da utima mensagem: "+ lastTransaction.getType());
        switch (lastTransaction.getType()) {
            case LB_ENTRY:{
                String target = ((TargetedTransaction)transaction).getTarget();
                if (transaction.getType() == TransactionType.LB_ENTRY_REPLY 
                        && target.equals(this.idManager.getIP())) {
                    try {
                        String source = this.idManager.getIP();
                        String group = this.groupManager.getGroup();
                        String newTarget = transaction.getSource();

                        Device device = this.deviceManager.getAllDevices().get(0);
                        String deviceString = DeviceWrapper.toJSON(device);
                       
                        Transaction transactionReply = new Request(source, group,deviceString, newTarget);
                        this.sendTransaction(transactionReply);
                    } catch (IOException ex) {
                        System.out.println("Load Balancer - Não foi possível recuperar a lista de dispositivos.");
                    }
                    
                } else { //Thread -> Timeout
                    Long timeAtual = System.currentTimeMillis();
                    if ((timeAtual - lastTransaction.getPublishedAt()) >= TIMEOUT_LB_REPLY) {
                        resendTransaction();
                    } 
                }  
                break;
            }
            case LB_ENTRY_REPLY:{
                String target = ((TargetedTransaction)transaction).getTarget();
                String device = ((Request)transaction).getDevice();
                if (transaction.getType() == TransactionType.LB_REQUEST
                        && target.equals(this.idManager.getIP())) {

                        String source = this.idManager.getIP();
                        String group = this.groupManager.getGroup();
                        String newTarget = transaction.getSource();
                        
                        Transaction transactionReply = new LBReply(source, group, newTarget);
                        this.sendTransaction(transactionReply); // Iniciar meu contador de timeout
                        this.loadSwapReceberDispositivo(device);
                    
                } else {
                    Long timeAtual = System.currentTimeMillis();
                    if ((timeAtual - lastTransaction.getPublishedAt()) >= TIMEOUT_GATEWAY) {
                        System.out.println("ANULOOU >>>>>>>>>>>");
                        lastTransaction = null;
                    } 
                }   break;
            }
            /**/
            case LB_REQUEST:{
                String target = ((TargetedTransaction)transaction).getTarget();
                if (transaction.getType() == TransactionType.LB_REPLY 
                        && target.equals(this.idManager.getIP())) {
                    System.out.println("RECEBI LB_REPLY");
                    this.removeFirstDevice();
                    this.lastTransaction = transaction;
                } else {
                    Long timeAtual = System.currentTimeMillis();
                    if ((timeAtual - lastTransaction.getPublishedAt()) >= TIMEOUT_GATEWAY) {
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
        try {
            this.connector.put(lastTransaction);
        } catch (InterruptedException ex) {
            System.out.println("Load Balancer - Error commit transaction - InterruptedException");
        }
    }

    private String buildSource(){
        return new StringBuilder(this.groupManager.getGroup())
                .append("/")
                .append(this.idManager.getIP())
                .toString();
    }
    
    public void removeFirstDevice() {
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
            
            Transaction transaction = this.connector.getTransactionByHash(hashTransaction);
            if (lastTransaction != null) {
                this.processTransactions(transaction);
            }else{
                this.messageArrived(transaction);
            }
        }else{
            System.out.println("Load balancer - New message - Invalid");
        }
    }
}
