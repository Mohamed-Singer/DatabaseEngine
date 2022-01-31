import java.io.Serializable;
import java.util.Vector;

public class Tuple implements Serializable{
	

	Vector <String> colNames;
	Vector <Object> data;

	public Tuple() {

		data = new Vector <Object>();
		colNames = new Vector<String>(); 
	}
	
	
	public Vector<Object> getData() {
		return data;
	}

	public Vector<String> getColNames() {
		return colNames;
	}

	public String toString(){
		return this.getData().toString();
	}


	public void add(Object e){
		this.getData().add(e);

	}

	
} 

