package dlt.load.balancer.model;

import dlt.client.tangle.services.ILedgerReader;
import dlt.client.tangle.services.ILedgerWriter;

/**
 *
 * @author Uellington Damasceno
 * @version 0.0.1
 */
public class LedgerConnector {
    private ILedgerReader ledgerReader;
    private ILedgerWriter ledgerWriter;
    
    public void setLedgerWriter(ILedgerWriter ledgerWriter){
        this.ledgerWriter = ledgerWriter;
    }
    
    public void setLedgerReader(ILedgerReader ledgerReader){
        this.ledgerReader = ledgerReader;
    }
}
