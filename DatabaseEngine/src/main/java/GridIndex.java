import java.util.Vector;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map.Entry;

public class GridIndex implements java.io.Serializable{
    
    
    private int dimensions;
    private Vector<String> colNames;
    private Vector<Bucket> Grid;
    private Vector<Vector> ranges;
    




    public GridIndex(String tablename, String[] colNames) throws IOException, ParseException 
    {
        
        this.Grid = new Vector<>();
        dimensions = colNames.length;
        this.colNames= new Vector<>();

        for(int i=0 ; i<colNames.length ; i++)
        this.colNames.add(colNames[i]);

        

    
       for(int i=0; i <((int) Math.pow(10, dimensions)) ; i++)
       Grid.add(new Bucket(tablename+"bucket"+i)); 

       Vector<String> MinValues = new Vector<>();
       Vector<String> MaxValues = new Vector<>();
       Vector<String> DataTypes = new Vector<>();
       Vector<Object> inc       = new Vector<>();
       this.ranges    = new Vector<>();




       

        for(int i = 0 ; i<dimensions ; i++)
                
        {

            String file = "src/main/resources/metadata.csv";
	        BufferedReader reader = null;
			String line ="";
       
            reader = new BufferedReader(new FileReader(file));

			while((line=reader.readLine())!=null)
		    
            {
               
				String[] row = line.split(",");

				
				if((row[0].toString().equals(tablename) && row[1].toString().equals(colNames[i])))
				{

                    MinValues.add(row[5].toString());
					MaxValues.add(row[6].toString());
                    DataTypes.add(row[2].toString());
     			}

                 
           
            }

		}


        for(int i = 0; i<dimensions; i++)
        {
            
        if (DataTypes.get(i).equals("java.lang.Integer"))					
        {
           
        	int increment =   (int) Math.ceil((Integer.parseInt(MaxValues.get(i)) - Integer.parseInt(MinValues.get(i))) / 10.0 );
            inc.add(increment);
							

	    }

						
        else if (DataTypes.get(i).equals("java.lang.Double"))
		{
            double increment =    Math.ceil((Double.parseDouble(MaxValues.get(i)) - Double.parseDouble(MinValues.get(i))) / 10.0 ); ;
						
            inc.add(increment);
		} 

		else if (DataTypes.get(i).equals("java.lang.String"))
		{
            if(MinValues.get(i).charAt(2) == '-')
            {
                int increment = (int) Math.ceil((Integer.parseInt(MaxValues.get(i).substring(0, 2)) - (Integer.parseInt(MinValues.get(i).substring(0, 2)))) / 10.0) ;


                inc.add(increment);

            }

            else
            {

                int increment = (int) Math.ceil(((int)(MaxValues.get(i)).toString().charAt(0) - (int)(MinValues.get(i)).toString().charAt(0)) / 10.0) ;
				
                inc.add(increment);
            }				
	    } 

		else if (DataTypes.get(i).equals("java.util.Date"))
		{
            Date minvalue;
            Date maxvalue;

            DateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");

            DateFormat dateFormat2 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);


            minvalue = dateFormat1.parse(MinValues.get(i));
            maxvalue = dateFormat1.parse(MaxValues.get(i));

            int increment = (int) Math.ceil((maxvalue.getYear() - minvalue.getYear()) / 10.0 );

            inc.add(increment);


		}

   
    
        }


        

        for(int i=0 ;i<dimensions; i++)
        {
            Vector <Range> Range = new Vector<>();

            for(int j = 0; j<10 ; j++)
            {
                if (DataTypes.get(i).equals("java.lang.Integer"))					
                     {
                      int curr;
                      if(j==0)
                      curr = Integer.parseInt(MinValues.get(i));
                      else
                      curr = Integer.parseInt(Range.get(j-1).getub().toString());
                      Range.add(new Range(curr, curr + (Integer)inc.get(i)));
                     }

						
                else if (DataTypes.get(i).equals("java.lang.Double"))
		             {
                        double curr;
                        if(j==0)
                        curr = Double.parseDouble(MinValues.get(i));
                        else
                        curr = Double.parseDouble(Range.get(j-1).getub().toString());
                        Range.add(new Range(curr, curr + Double.parseDouble(inc.get(i).toString())));
		             } 

		        else if (DataTypes.get(i).equals("java.lang.String"))
		             {
                        String curr;
                        String y;
                        


                        if(MinValues.get(i).charAt(2) == '-')
                        {
                        	if(j==0)
                            curr = MinValues.get(i);
                            else
                            curr = Integer.parseInt(Range.get(j-1).getub().toString().substring(0, 2))+1 + "-0000";
                        	
                        	if (Integer.parseInt(curr.toString().substring(0, 2)) > Integer.parseInt(MaxValues.get(i).substring(0,2)))
                        		break;
                        		
                        	
                            int x = Integer.parseInt(curr.substring(0, 2));

                            x+=(Integer)inc.get(i);
                            
                            if (x>(Integer.parseInt(MaxValues.get(i).substring(0,2))))
                            {
                            	x=Integer.parseInt(MaxValues.get(i).substring(0,2));
                            	y = x+"-9999";
                            	Range.add(new Range(curr, y));
                                break;
                            	
                            	
                            }
                            y = x+"-9999";
                            
                            

                        }
                        
                        else
                        {
                        	if(j==0)
                            curr = MinValues.get(i);
                            else
                            curr = Range.get(j-1).getub().toString();
                        	
                            int x = curr.charAt(0);

                            x+= (int)inc.get(i);

                            String n = Character.toString((char)x);

                            y = n + curr.substring(1);
                                 
                            
                        } 
                        
                        Range.add(new Range(curr, y));

                        
	                 } 

		        else if (DataTypes.get(i).equals("java.util.Date"))
		             {
                        DateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");
                        DateFormat dateFormat2 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
                        
                        Date curr;
                        if(j==0)
                        curr = dateFormat1.parse(MinValues.get(i)); 
                        else                        
                        curr = dateFormat2.parse(Range.get(j-1).getub().toString());


                        Object nw = curr.clone();

                        Date n = dateFormat2.parse(nw.toString());

                        n.setYear(curr.getYear()+ Integer.parseInt(inc.get(i).toString()));
                        n.setDate(31);
                        n.setMonth(11);

                        Range.add(new Range(curr, n));

                        

		             } 

            }

            ranges.add(Range);


        }


        
        
        Table t = deserializeTable(tablename);

        for(int i = 0; i<t.getPages().size(); i++)
        {
            Page p = deserializePage(t.getPages().get(i));

            for (int j=0 ; j<p.getTuples().size(); j++)
            {
            	boolean flag = false;
                Tuple tup = p.getTuples().get(j);

                String ind = "";

                Object clustkey = tup.getData().get(0);

                for (int k=0; k<colNames.length; k++)
                {
                    for(int l=0; l<tup.getColNames().size(); l++)
                    {
                    if (colNames[k].equals(tup.getColNames().get(l)))
                    {
                        Object tp = tup.getData().get(l);

                      if (tp.getClass().getName().equals("java.lang.Integer"))					
                     
                      {
                          int tu = (int)tp;

                          Vector <Range> v = ranges.get(k);

                          for(int m =0;m< v.size() ; m++)
                          {
                              if(tu >= ((int)v.get(m).getlb()) && tu < ((int)v.get(m).getub()))
                              {
                                  ind += m;
                                  break;

                              }

                              if(m==(v.size()-1))
                                {
                                	System.out.println("searching for: "+tu+"//////"+"rangeVector: "+v);
                                	ind+=-1;
                                }
                              
                          }
                      
                     
                      }

						
                
                      else if (tp.getClass().getName().equals("java.lang.Double"))
		             
                      {
                        double tu = (Double)tp;

                        Vector <Range> v = ranges.get(k);

                        for(int m =0;m< v.size() ; m++)
                        {
                            if(tu >= ((double)v.get(m).getlb()) && tu < ((double)v.get(m).getub()))
                            {
                                ind += m;
                                break;

                            }

                            if(m==(v.size()-1))
                                {
                                	System.out.println("searching for: "+tu+"//////"+"rangeVector: "+v);
                                	ind+=-1;
                                }
                            
                        }

		             
                      } 

		        
                      else if (tp.getClass().getName().equals("java.lang.String"))
		              {	
                        String tu = tp.toString();
                        
                        

                        Vector <Range> v = ranges.get(k);

                        for(int m =0;m< v.size() ; m++)
                        {
                        	flag = true;
                            if(tu.charAt(2) == '-')
                            {
                                if((Integer.parseInt(tu.substring(0,2)) >= (Integer.parseInt(v.get(m).getlb().toString().substring(0, 2))) && ((Integer.parseInt(tu.substring(0,2)) <= (Integer.parseInt(v.get(m).getub().toString().substring(0, 2)))))))
                            
                                {
                               
                                	
                                    ind += m;
                                    
                                    break;
                                }
                                if(m==(v.size()-1))
                                {
                                	System.out.println("searching for: "+tu+"//////"+"rangeVector: "+v);
                                	ind+=-1;
                                }


                            }

                            else
                            {

                            if((tu.compareTo(v.get(m).getlb().toString())>=0) && (tu.compareTo(v.get(m).getub().toString()) < 0))
                            { 
                            	
                                ind += m;
                               
                                break;

                            }
                            if(m==(v.size()-1))
                            {
                            	System.out.println("searching for: "+tu+"//////"+"rangeVector: "+v);
                            	ind+=-1;
                            }
                            }
                            
                        }

	                  } 

		        
                      else if (tp.getClass().getName().equals("java.util.Date"))
		             
                      {
                        String tu = tp.toString();

                        Vector <Range> v = ranges.get(k);

                        for(int m =0;m< v.size() ; m++)
                        {
                        	flag = true;
                            DateFormat dateFormat3 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);


							Date entryDate = dateFormat3.parse(tu);

                            Date minvalue = dateFormat3.parse(v.get(m).getlb().toString());
							Date maxvalue = dateFormat3.parse(v.get(m).getub().toString());

                            if((entryDate.compareTo(minvalue)>=0) && (entryDate.compareTo(maxvalue) < 0))
                            {
                                ind += m;
                                break;

                            }


                            if(m==(v.size()-1))
                            {
                            	System.out.println("searching for: "+entryDate+"//////"+"rangeVector: "+v);
                            	ind+=-1;
                            }
                        }


		              } 


                    }


                    }
                
                }

                

            
                   

                  Bucket b = Grid.get(Integer.parseInt(ind));
                
                  b.getRefs().add(new Reference(p.getPageName(), p.getTuples().indexOf(tup), clustkey));
               

               


            }



        }
         
        }
    
    
        public Vector<String> getColNames() {
		return colNames;
	}


	


	public Vector<Bucket> getGrid() {
		return Grid;
	}


	

	public Vector<Vector> getRanges() {
		return ranges;
	}


	


	public void addToGrid(Tuple t, Page p) throws ParseException {
		
		 String ind = "";

         Object clustkey = t.getData().get(0);
         
         

         for (int k=0; k<colNames.size(); k++)
         {
             for(int l=0; l<t.getColNames().size(); l++)
             {
             if (colNames.get(k).equals(t.getColNames().get(l)))
             {
                 Object tp = t.getData().get(l);

               if (tp.getClass().getName().equals("java.lang.Integer"))					
              
               {
                   int tu = (int)tp;

                   Vector <Range> v = ranges.get(k);

                   for(int m =0;m< v.size() ; m++)
                   {
                       if(tu >= ((int)v.get(m).getlb()) && tu < ((int)v.get(m).getub()))
                       {
                           ind += m;
                           break;

                       }
                       
                   }
               
              
               }

					
         
               else if (tp.getClass().getName().equals("java.lang.Double"))
	             
               {
                 double tu = (Double)tp;

                 Vector <Range> v = ranges.get(k);

                 for(int m =0;m< v.size() ; m++)
                 {
                     if(tu >= ((double)v.get(m).getlb()) && tu < ((double)v.get(m).getub()))
                     {
                         ind += m;
                         break;

                     }
                     
                 }

	             
               } 

	        
               else if (tp.getClass().getName().equals("java.lang.String"))
	              {	
                 String tu = tp.toString();
                 
                 

                 Vector <Range> v = ranges.get(k);

                 for(int m =0;m< v.size() ; m++)
                 {
                 	
                     if(tu.charAt(2) == '-')
                     {
                         if((Integer.parseInt(tu.substring(0,2)) >= (Integer.parseInt(v.get(m).getlb().toString().substring(0, 2))) && ((Integer.parseInt(tu.substring(0,2)) <= (Integer.parseInt(v.get(m).getub().toString().substring(0, 2)))))))
                     
                         {
                        
                         	
                             ind += m;
                             
                             break;
                         }
                        


                     }

                     else
                     {

                     if((tu.compareTo(v.get(m).getlb().toString())>=0) && (tu.compareTo(v.get(m).getub().toString()) < 0))
                     { 
                     	
                         ind += m;
                        
                         break;

                     }
                     if(m==(v.size()-1))
                     {
                     	System.out.println("searching for: "+tu+"//////"+"rangeVector: "+v);
                     	ind+=-1;
                     }
                     }
                     
                 }

               } 

	        
               else if (tp.getClass().getName().equals("java.util.Date"))
	             
               {
                 String tu = tp.toString();

                 Vector <Range> v = ranges.get(k);

                 for(int m =0;m< v.size() ; m++)
                 {
                 	
                     DateFormat dateFormat3 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);


						Date entryDate = dateFormat3.parse(tu);

                     Date minvalue = dateFormat3.parse(v.get(m).getlb().toString());
						Date maxvalue = dateFormat3.parse(v.get(m).getub().toString());

                     if((entryDate.compareTo(minvalue)>=0) && (entryDate.compareTo(maxvalue) < 0))
                     {
                         ind += m;
                         break;

                     }
                    
                 }


	              } 


             }


             }
         
         }

         

     
           Bucket b = Grid.get(Integer.parseInt(ind));
         
           b.getRefs().add(new Reference(p.getPageName(), p.getTuples().indexOf(t), clustkey));
        

        


     }
		
		
     public void deleteFromGrid(Tuple t) throws ParseException {
		
        String ind = "";

        Object clustkey = t.getData().get(0);
        
        

        for (int k=0; k<colNames.size(); k++)
        {
            for(int l=0; l<t.getColNames().size(); l++)
            {
            if (colNames.get(k).equals(t.getColNames().get(l)))
            {
                Object tp = t.getData().get(l);

              if (tp.getClass().getName().equals("java.lang.Integer"))					
             
              {
                  int tu = (int)tp;

                  Vector <Range> v = ranges.get(k);

                  for(int m =0;m< v.size() ; m++)
                  {
                      if(tu >= ((int)v.get(m).getlb()) && tu < ((int)v.get(m).getub()))
                      {
                          ind += m;
                          break;

                      }
                      
                  }
              
             
              }

                   
        
              else if (tp.getClass().getName().equals("java.lang.Double"))
                
              {
                double tu = (Double)tp;

                Vector <Range> v = ranges.get(k);

                for(int m =0;m< v.size() ; m++)
                {
                    if(tu >= ((double)v.get(m).getlb()) && tu < ((double)v.get(m).getub()))
                    {
                        ind += m;
                        break;

                    }
                    
                }

                
              } 

           
              else if (tp.getClass().getName().equals("java.lang.String"))
                 {	
                String tu = tp.toString();
                
                

                Vector <Range> v = ranges.get(k);

                for(int m =0;m< v.size() ; m++)
                {
                    
                    if(tu.charAt(2) == '-')
                    {
                        if((Integer.parseInt(tu.substring(0,2)) >= (Integer.parseInt(v.get(m).getlb().toString().substring(0, 2))) && ((Integer.parseInt(tu.substring(0,2)) <= (Integer.parseInt(v.get(m).getub().toString().substring(0, 2)))))))
                    
                        {
                       
                            
                            ind += m;
                            
                            break;
                        }
                       


                    }

                    else
                    {

                    if((tu.compareTo(v.get(m).getlb().toString())>=0) && (tu.compareTo(v.get(m).getub().toString()) < 0))
                    { 
                        
                        ind += m;
                       
                        break;

                    }
                    if(m==(v.size()-1))
                    {
                        System.out.println("searching for: "+tu+"//////"+"rangeVector: "+v);
                        ind+=-1;
                    }
                    }
                    
                }

              } 

           
              else if (tp.getClass().getName().equals("java.util.Date"))
                
              {
                String tu = tp.toString();

                Vector <Range> v = ranges.get(k);

                for(int m =0;m< v.size() ; m++)
                {
                    
                    DateFormat dateFormat3 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);


                       Date entryDate = dateFormat3.parse(tu);

                    Date minvalue = dateFormat3.parse(v.get(m).getlb().toString());
                       Date maxvalue = dateFormat3.parse(v.get(m).getub().toString());

                    if((entryDate.compareTo(minvalue)>=0) && (entryDate.compareTo(maxvalue) < 0))
                    {
                        ind += m;
                        break;

                    }
                   
                }


                 } 


            }


            }
        
        }

        

    
          Bucket b = Grid.get(Integer.parseInt(ind));

          for(int i =0; i<b.getRefs().size(); i++)
          {
            if(b.getRefs().get(i).getClustKey().toString().equals(clustkey.toString()))
            b.getRefs().remove(i);
          }

       


    }
	
	
	
		public static void serializeTable(Table t)
	{
	
	 try {
         FileOutputStream fileOut =
         new FileOutputStream("src/main/resources/data/"+t.getTablename()+".class");
         ObjectOutputStream out = new ObjectOutputStream(fileOut);
         out.writeObject(t);
         out.close();
         fileOut.close();
      } catch (IOException e) {
         e.printStackTrace();
      }
	}

	public static Table deserializeTable(String tablename)
	{
		Table t=null;
		try {

			FileInputStream file;
		
			file = new FileInputStream("src/main/resources/data/"+tablename+".class");
		
            ObjectInputStream in = new ObjectInputStream(file);
              
            // Method for deserialization of object
            t = (Table)in.readObject();
              
            in.close();
            file.close();

		    } 
		
			catch (IOException | ClassNotFoundException e ) {
			
			e.printStackTrace();
		}

		return t;


	}
	
	public static void serializePage(Page p){
		try {
			FileOutputStream fileOut =
			new FileOutputStream("src/main/resources/data/"+p.getPageName()+".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(p);
			out.close();
			fileOut.close();
		 } catch (IOException e) {
			e.printStackTrace();
		 }


	}

	public static Page deserializePage(String PageName)
	{
		Page p=null;
		try {

			FileInputStream file;
		
			file = new FileInputStream("src/main/resources/data/"+PageName+".class");
		
            ObjectInputStream in = new ObjectInputStream(file);
              
            // Method for deserialization of object
            p = (Page)in.readObject();
              
            in.close();
            file.close();

		    } 
		
			catch (IOException | ClassNotFoundException  e) {
			
			e.printStackTrace();
		}

		return p;


	}





    

						
        












    


    public static void main(String[] args) throws DBAppException {

        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "students";
        String[] index = {"id"};
        dbApp.createIndex(table, index);

        
        
    }




   
































}
