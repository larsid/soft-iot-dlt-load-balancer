package dlt.load.balancer.controller;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 *
 * @author Uellington Damasceno
 */
public class MQTTNewConnectionsListener implements MqttCallback{
    
    private MqttClient client;
    
    public MQTTNewConnectionsListener(){
        
    }
    
    public void initialize(){
        this.client.setCallback(this);
    }
    
    public void disconnect(){
        try {
            this.client.disconnect();
        } catch (MqttException ex) {
            Logger.getLogger(MQTTNewConnectionsListener.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    public void connectionLost(Throwable cause) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
