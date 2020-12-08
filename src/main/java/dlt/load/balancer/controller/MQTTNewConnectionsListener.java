package dlt.load.balancer.controller;

import dlt.load.monitor.model.BrokerStatus;
import dlt.load.monitor.services.IProcessor;
import extended.tatu.wrapper.model.ExtendedTATUMethods;
import extended.tatu.wrapper.model.TATUMessage;
import extended.tatu.wrapper.util.ExtendedTATUWrapper;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 *
 * @author Uellington Damasceno
 */
public class MQTTNewConnectionsListener implements MqttCallback {

    private final String uri;
    private final String serverId;
    private final String username;
    private final String password;
    private MqttClient client;
    private IProcessor<BrokerStatus> processor;

    public MQTTNewConnectionsListener(String url, String port, String id, String username, String password) {
        this.serverId = id;
        this.username = username;
        this.password = password;
        this.uri = new StringBuilder()
                .append(url)
                .append(":")
                .append(port)
                .toString();
        System.out.println("MQTTNewConnectionsListener is build.");
        System.out.println("======Broker info=======");
        System.out.println("URL:" + url);
        System.out.println("PORT:" + port);
        System.out.println("id:" + id);
        System.out.println("USERNAME:" + username);
        System.out.println("PASSWORD:" + password);

    }

    public void initialize() throws MqttException {
        this.client = new MqttClient(this.uri, serverId);
        MqttConnectOptions connection = new MqttConnectOptions();
        if (!this.username.isEmpty()) {
            connection.setUserName(this.username);
        }
        if (!this.password.isEmpty()) {
            connection.setPassword(this.password.toCharArray());
        }
        this.client.connect(connection);
        this.client.setCallback(this);
        this.client.subscribe(ExtendedTATUWrapper.getConnectionTopic());
        System.out.println("MQTTNewConnectionsListener has been inicialized.");
    }

    public void disconnect() {
        if (this.client != null && client.isConnected()) {
            try {
                client.disconnect();
            } catch (MqttException ex) {
                Logger.getLogger(MQTTNewConnectionsListener.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        System.out.println("MQTTNewConnectionsListener has been disconnected.");
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("Connection Lost");
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        TATUMessage tatuMessage = new TATUMessage(message.getPayload());
        if (tatuMessage.getMethod().equals(ExtendedTATUMethods.CONNECT)) {
            String deviceName = tatuMessage.getTarget();
            BrokerStatus status = this.processor.getLastFitness();

            String connackMessage = ExtendedTATUWrapper
                    .buildConnackMessage(deviceName, deviceName, !status.isFull());

            MqttMessage response = new MqttMessage(connackMessage.getBytes());
            this.client.publish(ExtendedTATUWrapper.getConnectionTopicResponse(), response);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {

    }

    public void setProcessor(IProcessor processor) {
        this.processor = processor;
    }

}
