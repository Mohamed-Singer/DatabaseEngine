
import java.io.Serializable;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.util.Date;
import java.util.Vector;

public class Table implements java.io.Serializable{

	private static final long serialVersionUID = 6019447327243878673L;
	
	private String tablename;
	private String primarykey;
	private Vector<String> Pages;
	private Vector<GridIndex> GridIndices;
	private Vector<String> maxIndices;
	private Class clustType;
	
	
	public Table(String t , String p , Class c)
	{
		clustType = c;
		
		tablename=t;
		primarykey=p;
		Pages=new Vector<>();
		
		String DataType = c.getName();

	    maxIndices = new Vector<>();

		GridIndices = new Vector<>();

	}

	

	public String toString(){
		return("Table name: "+tablename+"\n"
		       +"key: "+primarykey+"\n"
			   +Pages );
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public String getPrimarykey() {
		return primarykey;
	}

	public void setPrimarykey(String primarykey) {
		this.primarykey = primarykey;
	}

	public Vector<String> getPages(){
		return Pages;
	}

	public Vector<GridIndex> getGridIndices(){
		return GridIndices;
	}

	public Vector<String> getMaxIndices(){
		return maxIndices;
	}

	public Class getClustKeyType(){
		return clustType;
	}

	

	

	

}
