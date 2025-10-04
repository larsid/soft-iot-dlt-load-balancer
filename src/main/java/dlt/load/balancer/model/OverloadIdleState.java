package dlt.load.balancer.model;

/**
 *
 * @author Uellington Damasceno
 */
public class OverloadIdleState extends IdleState{
    
    public OverloadIdleState(Balancer balancer) {
        super(balancer, null);
    }
    
    @Override
    public void hookTransitionTo(AbstractBalancerState state){
        this.transiteOverloadedStateTo(state);
    }
}
