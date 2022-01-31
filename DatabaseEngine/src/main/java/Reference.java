import java.io.Serializable;

public class Reference implements Serializable{

    private String pageName;
    private int index;
    private Object clustKey;
    
    public Reference(String pgname , int i , Object key)
    {
        pageName = pgname;
        index = i;
        clustKey = key;

    }


    public String getPageName(){

        return pageName;

    }

    public int getIndexOf(){

        return index;

    }
    

    public Object getClustKey(){

        return clustKey;

    }


    
}
