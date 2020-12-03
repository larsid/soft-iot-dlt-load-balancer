package dlt.load.balancer.model;

import dlt.client.tangle.services.ILedgerReader;

/**
 *
 * @author Uellington Damasceno
 * @version 0.0.1
 */
public class LedgerConnector {
    private ILedgerReader ledgerReader;
    
    public void setLedgerReader(ILedgerReader ledgerReader){
        this.ledgerReader = ledgerReader;
    }
}
