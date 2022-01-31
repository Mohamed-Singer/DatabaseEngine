import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.Vector;

public class Page implements java.io.Serializable{
	
	private String PageName;
	private int maxTuples;
	private Vector<Tuple> tuples;
	private Vector<String> Overflow;
	private Object maxOverflowIndices;

    
	
	public Page(String pageName)
	{
		PageName = pageName;
		
		Properties prop = new Properties();
		String fileName = "src/main/resources/DBApp.config";
		InputStream is = null;
		try {
		    is = new FileInputStream(fileName);
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		    
		}
		try {
		    prop.load(is);
		} catch (IOException ex) {
		    
		}
		maxTuples=Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		
		tuples= new Vector<>();

		Overflow = null;

		maxOverflowIndices = null;

		
		
	}

	public Boolean isFull(){
		if(tuples.size() == maxTuples)
		  return true;
		  
		else return false;
	}

	public String toString(){
		return (tuples.toString());
	}
	

	public String getPageName(){

		return PageName;
	}

	public Vector<Tuple> getTuples() {
		return tuples;
	}
	public void setTuples(Vector<Tuple> tuples) {
		this.tuples = tuples;
	}
	public int getMaxTuples() {
		return maxTuples;
	}

	public Vector<String> getOverflow(){
		return Overflow;
	}

	public Object getMaxOverflowIndices(){
		return maxOverflowIndices;
	}

	






	

	
}

