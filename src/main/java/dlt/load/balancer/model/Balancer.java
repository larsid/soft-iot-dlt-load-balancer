package dlt.load.balancer.model;


/**
 *
 * @author Uellington Damasceno
 * @version 0.0.1
 */
public class Balancer {
    private LedgerConnector ledgerConnector;
    
    public void setLedgerConnector(LedgerConnector ledgerConnector){
        this.ledgerConnector = ledgerConnector;
    }
}
