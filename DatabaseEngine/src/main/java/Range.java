public class Range implements java.io.Serializable{

    private Object lowerBound;
    private Object upperBound;

    public Range(Object lb, Object ub){
        lowerBound = lb;
        upperBound = ub;
    }

    public Object getlb(){
        return lowerBound;
    }

    public Object getub(){
        return upperBound;
    }
    
    public String toString() {
    	return "lb"+lowerBound+"__"+"ub:"+upperBound; 
    }
}
