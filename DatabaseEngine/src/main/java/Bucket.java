
    import java.io.FileInputStream;
    import java.io.FileNotFoundException;
    import java.io.FileWriter;
    import java.io.IOException;
    import java.io.InputStream;
    import java.util.Collections;
    import java.util.Properties;
    import java.util.Vector;
    
    public class Bucket  implements java.io.Serializable{
        
        private String bucketName;
        private Vector<Reference> refs;
    
        
        
        public Bucket(String bucketName)
        {
            this.bucketName = bucketName;
            
            refs= new Vector<>();
      
        }
    
        
    
        public String toString(){
            return (refs.toString());
        }
        
    
        public String getBucketName(){
    
            return bucketName;
        }
    
        public Vector<Reference> getRefs() {
            return refs;
        }
        
    
        
    
    
    
    
    
    
        
    
        
    }
    
      

