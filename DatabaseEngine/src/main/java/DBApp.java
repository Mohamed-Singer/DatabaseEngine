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
import java.util.Vector;
import java.util.Map.Entry;


public class DBApp implements DBAppInterface{
	private static String filepath = "src/main/resources/metadata.csv";
	String[] myIntArray = { "java.lang.Integer", "java.lang.String", 
			"java.lang.Double", "java.util.Date"};
	
	public static void appendtoCSV(Vector<Vector<Object>> arraylist) throws IOException{
        FileWriter pw = new FileWriter(filepath,true);
       
        
            for(int i=0;i<arraylist.size();i++)
            {
            	String s="";
            	for(int j=0; j<arraylist.get(i).size();j++)
            	{
            		if(j==(arraylist.get(i).size()-1))
            			s += arraylist.get(i).get(j);
            		else{
            		    s += arraylist.get(i).get(j);
						s+=',';
					}
                 	
            	}
            	pw.append(s+"\n");
            }
            
            
        
            pw.flush();
            pw.close();
    }
	
	
	
	public static void appendStringtoCSV(String o) throws IOException{
        FileWriter pw = new FileWriter(filepath,true);
       
        
            
            	pw.append(o+"\n");
            	
            
            
            
        
            pw.flush();
            pw.close();
    }

	@Override
	public void init() {
		
		try {
			appendStringtoCSV("Table Name,Column Name,Column Type,ClusteringKey,Indexed,min,max");
		} catch (IOException e) {
		
			e.printStackTrace();
		}

		new File("src/main/resources/data").mkdirs();

		


	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
		
		  Iterator<Entry<String, String>> iteratortype = colNameType.entrySet().iterator();

		  Class clustType = null;
		  
		  while(iteratortype.hasNext())
		  {
			  Entry<String, String> entry = iteratortype.next();
			  String x = entry.getValue();
			  boolean f = false;
			  for(int i =0 ; i<myIntArray.length;i++)
			  {
				  if((x.equals(myIntArray[i])))
				  {
					  f=true;
					  
				  }
				 
			  }
			  if(f==false)
			  {
				  throw new DBAppException();
				  
			  } 
			  
		  }
		  
		  Vector<Vector<Object>> arraylist = new Vector<Vector<Object>>();
		  Iterator<Entry<String, String>> iteratortype1 = colNameType.entrySet().iterator();
		  Iterator<Entry<String, String>> datamin = colNameMin.entrySet().iterator();
		  Iterator<Entry<String, String>> datamax = colNameMax.entrySet().iterator();
		  int i =0;
		  int key=0;
		  int key1=0;
		  boolean foundKey= false;
		  while(iteratortype1.hasNext() && datamin.hasNext() && datamax.hasNext())
		  {
			  Entry<String, String> entry1 = iteratortype1.next();
			  Entry<String, String> entry2 = datamin.next();
			  Entry<String, String> entry3 = datamax.next();
			  Vector<Object> row = new Vector<Object>();
			  row.add(i,entry3.getValue());
			  row.add(i,entry2.getValue());
			  row.add(i,"False");
			  if(clusteringKey.equals(entry1.getKey()))
			  {
				  clustType = entry1.getValue().getClass();
				  row.add(i,"True");
				  foundKey=true;
				  key1 = key;
			  }
			  else
				  row.add(i,"False");
			  row.add(i, entry1.getValue());
			  row.add(i, entry1.getKey());
			  row.add(i, tableName);
			  
			  arraylist.add(row);	
			  key++;
		  
		  }
		  Vector<Object> keyarr = new Vector<Object>();
		  keyarr = arraylist.get(key1);
		  arraylist.remove(key1);
		  arraylist.add(0, keyarr);
		 
		  
		  
		  if (!(foundKey)) throw new DBAppException();
		  else foundKey = false;
		  
		  
		 
		  try {
			appendtoCSV(arraylist);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  
		  Table t = new Table(tableName,clusteringKey , clustType);
		  serializeTable(t);
		  
		  
		  
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
	
	




	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		
		
	try

	{
		GridIndex f = new GridIndex(tableName,columnNames);
		Table t = deserializeTable(tableName);
		t.getGridIndices().add(f);
		serializeTable(t);
	} 
	catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

	
	

	}

	public static int binarySearchPages(Table t, Object key){  

		try{

		String DataType = key.getClass().getName();
		
		int first = 1;

		int last = t.getMaxIndices().size();

		Vector<String> v = t.getMaxIndices();


		if (DataType.equals("java.lang.Integer"))
		{

			int k = (Integer) key ;

			int mid = (first + last)/2; 
				
				while( first <= last )
				{ 
					

					if(t.getMaxIndices().size()==1)
					 return 0;


				   if (mid == 1)
				   {
					
					if((k < Integer.parseInt(v.get(mid-1))))
					   return 0;
					else if (k > Integer.parseInt(v.get(mid-1)))
					   return 1;

				

		
				   }
				 

 
				   else if (k > Integer.parseInt(v.get(mid-1)))
				   {  
					 first = mid + 1;     
				   }
				   else if ( k < Integer.parseInt(v.get(mid-1)) &&  (mid != 1) &&  (k > Integer.parseInt(v.get(mid-2))) )
				   {  
					 return mid - 1; 
				   }
				   
				   else
				   {  
					  last = mid - 1;  
				   } 

				   mid = (first + last)/2;  
				}  
				if ( first > last )
				{  
				   return (t.getMaxIndices().size()-1);
				}
				
				return -1;
				
			  							
		}

		else if (DataType.equals("java.lang.Double"))
		{
			
			
			double k = (double) key ;

			int mid = (first + last)/2; 
				
				while( first <= last )
				{ 
					

					if(t.getMaxIndices().size()==1)
					 return 0;


				   if (mid == 1)
				   {
					
					if((k < Double.parseDouble(v.get(mid-1))))
					   return 0;
					else if (k > Double.parseDouble(v.get(mid-1)))
					   return 1;

				

		
				   }
				 

 
				   else if (k > Double.parseDouble(v.get(mid-1)))
				   {  
					 first = mid + 1;     
				   }
				   else if ( k < Double.parseDouble(v.get(mid-1)) &&  (mid != 1) &&  (k > Double.parseDouble(v.get(mid-2))) )
				   {  
					 return mid - 1; 
				   }
				   
				   else
				   {  
					  last = mid - 1;  
				   } 

				   mid = (first + last)/2;  
				}  
				if ( first > last )
				{  
				   return (t.getMaxIndices().size()-1);
				}
				
				return -1;
				
			  									
	    } 

		else if (DataType.equals("java.lang.String"))
		{

	
			
			String k = (String) key ;

			int mid = (first + last)/2; 
				
				while( first <= last )
				{ 
					
					if(t.getMaxIndices().size()==1)
					 return 0;


				   if (mid == 1)
				   {
					
					if((k.compareTo(v.get(mid-1))) < 0 )
					   return 0;
					else if ((k.compareTo(v.get(mid-1))) > 0)
					   return 1;

				

		
				   }
				 

 
				   else if (k.compareTo(v.get(mid-1)) > 0)
				   {  
					 first = mid + 1;     
				   }
				   else if ( (k.compareTo(v.get(mid-1)) < 0) &&  (mid != 1) &&  ( k.compareTo(v.get(mid-2)) > 0 ) )
				   {  
					 return mid - 1; 
				   }
				   
				   else
				   {  
					  last = mid - 1;  
				   } 

				   mid = (first + last)/2;  
				}  
				if ( first > last )
				{  
				   return (t.getMaxIndices().size()-1);
				}
				
				return -1;
				
							
		} 

		else if (DataType.equals("java.util.Date"))
		{

			
			Date k = (Date)key ;

			int mid = (first + last)/2; 

			   

				DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

				    Date d1 = dateFormat.parse(v.get(mid-1));
					Date d2 = null;

					

					if(mid != 1){
					 d2 = dateFormat.parse(v.get(mid-2));
					}


				
				while( first <= last )
				{ 
					
					if(t.getMaxIndices().size()==1)
					 return 0;


				   if (mid == 1)
				   {

					
					if((k.compareTo(d1)) < 0 )
					   return 0;
					else if ((k.compareTo(d1) > 0))
					   return 1;

				

		
				   }
				 

 
				   else if (k.compareTo(d1) > 0)
				   {
					     

					
					first = mid + 1;     
				   }
				   else if ( (k.compareTo(d1) < 0) &&  (mid != 1) &&  ( k.compareTo(d2) > 0 ) )
				   {  
					 return mid - 1; 
				   }
				   
				   else
				   {  
					  last = mid - 1;  
				   } 

				   mid = (first + last)/2;  
				}  
				if ( first > last )
				{  
				   return (t.getMaxIndices().size()-1);
				}
				
				return -1;
				

								
		} 
	}
	catch(ParseException e){
		e.printStackTrace();
	}

		return -1;
		
		
			
		
				

	  }

	  public static int binarySearchTuples(Page p, Object key) throws ParseException
	  {  

		String DataType = key.getClass().getName();

		int first = 1;

		int last = p.getTuples().size();

		

		if (DataType.equals("java.lang.Integer"))
		{
			
			
			Vector<Integer> v = new Vector<>();

			for(int i=0;i<p.getTuples().size();i++)
			{
				v.add((Integer)p.getTuples().get(i).getData().get(0));

			}
			
			int k = (Integer) key ;

			int mid = (first + last)/2; 
				
				while( first <= last )
				{ 
				   if (mid == 1)
				   {
					if(v.size()==1)
					{
						if((k > (v.get(mid-1))))
						   return mid;
						else
						   return 0;
					}
					if ((k < (v.get(mid-1))))
					 return 0;
					 else if(k > (v.get(mid-1)) && k < (v.get(mid)))
					 return 1;
					 
					 else if(k > (v.get(mid-1)) && k > (v.get(mid)))
					 return 2;
				   }

 
				   else if (k > v.get(mid-1) )
				   {  
					 first = mid + 1;     
				   }
				   else if ( k < v.get(mid-1) && k > (v.get(mid-2)) && mid != 1)
				   {  
					 return mid-1; 
				   }
				   
				   else
				   {  
					  last = mid - 1;  
				   }  
				   
				   mid = (first + last)/2;  
				}  
				if ( first > last )
				{  
				   return (p.getTuples().size());
				}  
			  							
		}

		else if (DataType.equals("java.lang.Double"))
		{
			

			Vector<Double> v = new Vector<>();

			for(int i=0;i<p.getTuples().size();i++)
			{
				v.add(Double.parseDouble(p.getTuples().get(i).getData().get(0).toString()));

			}
			
			double k = (double) key ;

			int mid = (first + last)/2; 
				
				while( first <= last )
				{ 
					 
					if (mid == 1)
					{
						if(v.size()==1)
					{
						if((k > (v.get(mid-1))))
						   return mid;
						else
						   return 0;
					}

					 if ((k < (v.get(mid-1))))
					return 0;

					else if((k > (v.get(mid-1))) && (k < (v.get(mid))))
					 return 1;
					
					 else if (k > (v.get(mid)))
					 return 2;
					} 

 
				   else if (k > v.get(mid-1) )
				   {  
					 first = mid + 1;     
				   }
				   else if ( k < v.get(mid-1) && k > (v.get(mid-2)) && mid != 1)
				   {  
					 return mid-1; 
				   }
				   
				   else
				   {  
					  last = mid - 1;  
				   }  
				   mid = (first + last)/2;  
				}  
				if ( first > last )
				{  
					return (p.getTuples().size());
				}  
			
											
	    } 

		else if (DataType.equals("java.lang.String"))
		{

			Vector<String> v = new Vector<>();

			for(int i=0;i<p.getTuples().size();i++)
			{
				v.add((p.getTuples().get(i).getData().get(0).toString()));

			}
			
			String k = (String) key ;

			int mid = (first + last)/2; 
				
				while( first <= last )
				{ 
				   if (mid == 1)
				   {
					if(v.size()==1)
					{
						if((k.compareTo(v.get(mid-1)) > 0))
						   return mid;
						else
						   return 0;
					}

					 if ((k.compareTo(v.get(mid-1)) < 0))
					 return 0;
					 else if(k.compareTo(v.get(mid-1)) > 0 && k.compareTo(v.get(mid)) < 0)
					 return 1;
					 else if(k.compareTo(v.get(mid-1)) > 0 && k.compareTo(v.get(mid)) > 0)
					 return 2;
				   }

 
				   else if (k.compareTo(v.get(mid-1)) > 0)
				   {  
					 first = mid + 1;     
				   }
				   else if ( k.compareTo(v.get(mid-1)) < 0 &&( k.compareTo(v.get(mid-2)) > 0 ) && mid != 1)
				   {  
					 return mid-1; 
				   }
				   
				   else
				   {  
					  last = mid - 1;  
				   }  
				   mid = (first + last)/2;  
				}  
				if ( first > last )
				{  
				   return (p.getTuples().size());
				}  

							
		} 

		else if (DataType.equals("java.util.Date"))
		{

			Vector<Date> v = new Vector<>();

			for(int i=0;i<p.getTuples().size();i++)
			{
				

				Date d = (Date) p.getTuples().get(i).getData().get(0);

				v.add(d);



			}
			
			Date k = (Date)key ;

			int mid = (first + last)/2; 
				
				while( first <= last )
				{ 
					
					if (mid == 1)
					{
						if(v.size()==1)
					{
						if((k.compareTo(v.get(mid-1)) > 0))
						   return mid;
						else
						   return 0;
					}
					 if(k.compareTo(v.get(mid-1)) > 0 && k.compareTo(v.get(mid)) < 0)
					 return 1;
					 else if ((k.compareTo(v.get(mid-1)) < 0))
					 return 0;
					 else if(k.compareTo(v.get(mid-1)) > 0 && k.compareTo(v.get(mid)) > 0)
					 return 2;


					 


					}

 
				   else if (k.compareTo(v.get(mid-1)) > 0)
				   {  
					 first = mid + 1;     
				   }
				   else if ( k.compareTo(v.get(mid-1)) < 0 &&( k.compareTo(v.get(mid-2)) > 0 ) && mid != 1)
				   {  
					 return mid-1; 
				   }
				   
				   else
				   {  
					  last = mid - 1;  
				   }  
				   mid = (first + last)/2;  
				}  
				if ( first > last )
				{  
					return (p.getTuples().size());
				}  

								
		} 

		return -1;
		
		
			
		
	  }
	

	  
	 
	
	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		
		
		
		Tuple tuple = new Tuple();
		Vector<Object> temptuple = new Vector<Object>();
		Vector<String> temptuplenames = new Vector<>();
       
		
            String file = "src/main/resources/metadata.csv";
	        BufferedReader reader = null;
			String line ="";
			String clustkey = "";
		    Object clustvalue = null;
			Vector<String> fieldnames = new Vector<>();
			Vector<String> types = new Vector<>();
			Vector<String> minValues = new Vector<>();
			Vector<String> maxValues = new Vector<>();
			Vector<Boolean> isKey = new Vector<>();
			
		try{
			reader = new BufferedReader(new FileReader(file));
			while((line=reader.readLine())!=null)
		    {
				String[] row = line.split(",");

				
				if(!(row[1].toString().equals("Column Name")||row[2].toString().equals("Column Type")))
				{
					String tablename = row[0];
			        if(tableName.equals(tablename))
				    {
					String fieldname = row[1];
					fieldnames.add(fieldname);
					Vector<String> fields=new Vector<>();
					fields.add(row[1]);
					String type = row[2];
					types.add(type);
					String ckey = row[3];
					isKey.add(Boolean.parseBoolean(ckey));
					String min = row[5];
					minValues.add(min);
					String max = row[6];
					maxValues.add(max);					
					}

     			}

			}
		
			Vector<String> insertedFields=new Vector<>();

			

			for(Entry<String, Object> entry : colNameValue.entrySet())
			{
				insertedFields.add(entry.getKey());
			}

			

			
			


			for(int i=0;i<insertedFields.size();i++)
			{
				if (!(fieldnames.contains(insertedFields.get(i))))
				{
			     throw new DBAppException();
				}
				 
			}








			for(int i=0;i<fieldnames.size();i++)
			{
				Boolean foundField = false;
				Boolean correctDataType = false;
				Boolean withinRange = false;
				Boolean keyy = false;
				Boolean setNull=true;

				
				

				
				Iterator<Entry<String, Object>> iteratortype1 = colNameValue.entrySet().iterator();

                
			
				while(iteratortype1.hasNext())
				{
					foundField=false;
					correctDataType=false;
					withinRange=false;
					keyy=false;

					
	
					Entry<String, Object> entry = iteratortype1.next();
					String DataType=entry.getValue().getClass().getName();
					Object Data = entry.getValue();

					if((entry.getKey()).equals(fieldnames.get(i)))
					{

					  foundField = true;
					  setNull = false;

					  if(isKey.get(i))
					  {
					  clustkey = entry.getKey();
					  clustvalue = entry.getValue();
					  keyy=true;
					  }

					
					  
					
                   
					if(types.get(i).equals(DataType))
					{
					  correctDataType = true;
					  if (DataType.equals("java.lang.Integer") && (Data instanceof Integer))
						{
							int minvalue;
							int maxvalue;

								minvalue=Integer.parseInt(minValues.get(i));
								maxvalue=Integer.parseInt(maxValues.get(i));
					  
								if((((Integer)entry.getValue()<=maxvalue) && ((Integer)entry.getValue()>=minvalue)))
								  withinRange = true;

								else if(foundField) throw new DBAppException();

								
								
								if(!(keyy))
								{ 
								  temptuple.add(entry.getValue());
								
								}

								
						}

						else if (DataType.equals("java.lang.Double") && (Data instanceof Double))
						{
							double minvalue;
							double maxvalue;
								minvalue=Double.parseDouble(minValues.get(i));
								maxvalue=Double.parseDouble(maxValues.get(i));

								if((((double)entry.getValue()<=maxvalue) && ((double)entry.getValue()>=minvalue)))
									 withinRange = true;
									 
							    else if(foundField) throw new DBAppException();
								if(!(keyy))
								{ 
								  temptuple.add(entry.getValue());
								  
								}
								
						} 

						else if (DataType.equals("java.lang.String") && (Data instanceof String))
						{
							
							if((((String)entry.getValue()).compareTo(minValues.get(i))>0) && ((String)entry.getValue()).compareTo(maxValues.get(i))<0)
								  withinRange = true;

							else if(foundField) throw new DBAppException();	
							if(!(keyy))
								{ 
								  temptuple.add(entry.getValue());
								  
								}
						} 

						else if (DataType.equals("java.util.Date") && (Data instanceof Date))
						{
							Date minvalue;
							Date maxvalue;

							DateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");

							DateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd");


							minvalue = dateFormat1.parse(minValues.get(i));
							maxvalue = dateFormat2.parse(maxValues.get(i));

							

							Date entryDate;

							DateFormat dateFormat3 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);


							entryDate = dateFormat3.parse(entry.getValue().toString());


	


							if((entryDate.compareTo(minvalue)>0) && entryDate.compareTo(maxvalue)<0)
								 withinRange = true;
							 else if(foundField) throw new DBAppException();	
							 if(!(keyy))
							 { 
							   temptuple.add(entryDate);
							 }
						} 

						else throw new DBAppException();
						
					
					}

					else if(foundField) throw new DBAppException();
				}

				}

				if (setNull) temptuple.add(null);

				
			}

			if (clustvalue == null) throw new DBAppException();

			tuple.add(clustvalue);

			for(int i=0; i<fieldnames.size();i++)
			tuple.getColNames().add(fieldnames.get(i));

			for(int i=0;i<temptuple.size();i++)
		    tuple.add(temptuple.get(i));
			
			

			
			
			

			Table t=deserializeTable(tableName);

			

			

			if(t.getPages().isEmpty())
			{
			
				Page p = new Page(t.getTablename()+t.getPages().size());
				p.getTuples().add(tuple);
				
				if(!(t.getGridIndices().isEmpty()))
					for(int i = 0 ; i<t.getGridIndices().size(); i++)
						t.getGridIndices().get(i).addToGrid(tuple, p);
				
				
				serializePage(p);
				t.getPages().add(p.getPageName());
				t.getMaxIndices().add(clustvalue.toString());

				


	
			}


			else  // table has pages
			{

				int PageIndex = binarySearchPages(t, clustvalue);

				

				Page p = deserializePage(t.getPages().get(PageIndex));



				if ((p.getTuples().size() < p.getMaxTuples()))  //page not full
				{

					int tupleIndex = binarySearchTuples(p, tuple.getData().get(0));

					

					p.getTuples().add(tupleIndex, tuple);

					if (clustvalue.toString().compareTo(t.getMaxIndices().get(PageIndex))>0)
					  t.getMaxIndices().set(PageIndex, clustvalue.toString());


					if(!(t.getGridIndices().isEmpty()))
						for(int i = 0 ; i<t.getGridIndices().size(); i++)
							t.getGridIndices().get(i).addToGrid(tuple, p);
					
					serializePage(p);


					

					
	
				}


				else if (p.getTuples().size() >= p.getMaxTuples())  // page full
				{
					if(t.getPages().get(t.getPages().size()-1).equals(p.getPageName())) //last page in table
					{
						
						Page nw = new Page(t.getTablename()+t.getPages().size());

						if(clustvalue.toString().compareTo(t.getMaxIndices().get(PageIndex).toString()) > 0) // if  tuple to be entered greather than max index of prev page
						{
							nw.getTuples().add(tuple);
				            t.getPages().add(nw.getPageName());
				            t.getMaxIndices().add(clustvalue.toString());
				            
				            if(!(t.getGridIndices().isEmpty()))
								for(int i = 0 ; i<t.getGridIndices().size(); i++)
									t.getGridIndices().get(i).addToGrid(tuple, nw);
							
							serializePage(p);
							serializePage(nw);

						}

						else  // tuple to be entered is  not greater than max index of prev page
						{
							t.getPages().add(nw.getPageName());

							

							Tuple q = p.getTuples().get((p.getTuples().size()-1));
							nw.getTuples().add(q);
							p.getTuples().remove(p.getTuples().size()-1);

							int tupleIndex = binarySearchTuples(p, tuple.getData().get(0));

							p.getTuples().add(tupleIndex, tuple);

							t.getMaxIndices().set(PageIndex, p.getTuples().get(p.getTuples().size()-1).getData().get(0).toString());

							t.getMaxIndices().add(q.getData().get(0).toString());
							
							if(!(t.getGridIndices().isEmpty()))
								for(int i = 0 ; i<t.getGridIndices().size(); i++)
									t.getGridIndices().get(i).addToGrid(tuple, p);

							serializePage(p);
							serializePage(nw);
						}

					}

					else  // not last page in table
					{
					
					Page nxt = deserializePage(t.getPages().get(PageIndex+1));

					if (nxt.getTuples().size() >= nxt.getMaxTuples()) // next page full overflow
					{
						serializePage(nxt);

						Page pg = new Page(t.getTablename()+t.getPages().size()+"ov");

						t.getPages().add(PageIndex+1, pg.getPageName());


						Tuple q = p.getTuples().get(p.getTuples().size()-1);
						
						pg.getTuples().add(q);
					
						p.getTuples().remove(p.getTuples().size()-1);
					
						int tupleIndex = binarySearchTuples(p, tuple.getData().get(0));
					
						p.getTuples().add(tupleIndex, tuple);

						t.getMaxIndices().set(PageIndex, p.getTuples().get(p.getTuples().size()-1).getData().get(0).toString());

						t.getMaxIndices().add(PageIndex+1, q.getData().get(0).toString());
						
						if(!(t.getGridIndices().isEmpty()))
							for(int i = 0 ; i<t.getGridIndices().size(); i++)
								t.getGridIndices().get(i).addToGrid(tuple, p);
	
						serializePage(p);
						
						serializePage(pg);


					}

					else if(nxt.getTuples().size() < nxt.getMaxTuples()) // next page not full
					{
						Tuple q = p.getTuples().get(p.getTuples().size()-1);
						
						int x = binarySearchTuples(nxt, q.getData().get(0));
						nxt.getTuples().add(x, q);
						p.getTuples().remove(p.getTuples().size()-1);
						
						int tupleIndex = binarySearchTuples(p, tuple.getData().get(0));

						p.getTuples().add(tupleIndex, tuple);

						t.getMaxIndices().set(PageIndex, p.getTuples().get(p.getTuples().size()-1).getData().get(0).toString());
						
						if(!(t.getGridIndices().isEmpty()))
							for(int i = 0 ; i<t.getGridIndices().size(); i++)
								t.getGridIndices().get(i).addToGrid(tuple, p);
							
						serializePage(p);
						serializePage(nxt);

					}

				
				    }


				}


				
			}

			

			serializeTable(t);



			
						
		}
		catch (IOException | ParseException e ){
			e.printStackTrace();
		}



		finally
		{
			try {
				reader.close();
			} catch (IOException e) {
				
				e.printStackTrace();
			}
		}

			
		}

		public static int binarySelect(Page p , Object key) throws DBAppException
		{
			try{
			String DataType = key.getClass().getName();

			int first = 1;

			int last = p.getTuples().size();







			if (DataType.equals("java.lang.Integer"))
		
			{
			
				Vector<String> v = new Vector<>();

				for(int i=0;i<p.getTuples().size();i++)
			
				{
			
					v.add(p.getTuples().get(i).getData().get(0).toString());

				}

			
				for(int i=0;i<p.getTuples().size();i++)
			
				{
			
					v.add((p.getTuples().get(i).getData().get(0).toString()));


			
				}
			
			
				String k = key.toString() ;

			
				int mid = (first + last)/2; 
				
				while( first <= last )
				{ 
 
				   if (k.compareTo(v.get(mid-1)) > 0)
				   {  
					 first = mid + 1;     
				   }
				   else if ( k.compareTo(v.get(mid-1)) == 0)
				   {  
					 return mid-1; 
				   }
				   
				   else
				   {  
					  last = mid - 1;  
				   }  
				   mid = (first + last)/2;  
				}
				  
				throw new DBAppException();
			  							
		
			}

		else if (DataType.equals("java.lang.Double"))
		{
			

			Vector<Double> v = new Vector<>();

			for(int i=0;i<p.getTuples().size();i++)
			{
				v.add(Double.parseDouble(p.getTuples().get(i).getData().get(0).toString()));

			}
			
			double k = (double) key ;

			int mid = (first + last)/2; 
				
				while( first <= last )
				{ 
 
				   if (k > (v.get(mid-1)))
				   {  
					 first = mid + 1;     
				   }
				   else if ( k == (v.get(mid-1)))
				   {  
					 return mid-1; 
				   }
				   
				   else
				   {  
					  last = mid - 1;  
				   }  
				   mid = (first + last)/2;  
				}
				  
				throw new DBAppException();
			
											
	    } 

		else if (DataType.equals("java.lang.String"))
		{

			Vector<String> v = new Vector<>();

			for(int i=0;i<p.getTuples().size();i++)
			{
				v.add((p.getTuples().get(i).getData().get(0).toString()));

			}
			
			String k =  key.toString() ;

			int mid = (first + last)/2; 
				
				while( first <= last )
				{ 
 
				   if (k.compareTo(v.get(mid-1)) > 0)
				   {  
					 first = mid + 1;     
				   }
				   else if ( k.compareTo(v.get(mid-1)) == 0)
				   {  
					 return mid-1; 
				   }
				   
				   else
				   {  
					  last = mid - 1;  
				   }  
				   mid = (first + last)/2;  
				}
				  
				if(first>last) throw new DBAppException();

							
		} 

		else if (DataType.equals("java.util.Date"))
		{
			DateFormat dateFormat1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
			
			Date k = dateFormat1.parse(key.toString());

			Vector<Date> v = new Vector<>();

			for(int i=0;i<p.getTuples().size();i++)
			{
				
				DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

				Date d = dateFormat.parse(p.getTuples().get(i).getData().get(0).toString());

				v.add(d);



			}

			int mid = (first + last)/2; 
				
				while( first <= last )
				{ 
 
				   if (k.compareTo(v.get(mid-1)) > 0)
				   {  
					 first = mid + 1;     
				   }
				   else if ( k.compareTo(v.get(mid-1)) == 0)
				   {  
					 return mid-1; 
				   }
				   
				   else
				   {  
					  last = mid - 1;  
				   }  
				   mid = (first + last)/2;  
				}
				  
				throw new DBAppException();
		}
          

	}

	catch(ParseException e){
		e.printStackTrace();
	}

	return -1;
			
			


		}
	    
		

	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException 
	{
		
		Vector<String> insertedFields=new Vector<>();
		Vector<Object> insertedvalues = new Vector<>();

		columnNameValue.forEach((k,v)->{

			insertedFields.add(k);
			insertedvalues.add(v);  
		});
		
		Table t = deserializeTable(tableName);
		
		boolean flag = false ;
		GridIndex x = null;
		
		for (int i=0; i<t.getGridIndices().size(); i++)
		{
			
			
			 
			
			
			
		}
		
		if(flag) {
			
			
					
					
			
		}
		
		
		
		else
		{
		Tuple tuple = new Tuple();
		Vector<Object> temptuple = new Vector<Object>();
       
		
            String file = "src/main/resources/metadata.csv";
	        BufferedReader reader = null;
			String line ="";
			String clustkey = "";
		    Object clustvalue = null;
			Vector<String> fieldnames = new Vector<>();
			Vector<String> types = new Vector<>();
			Vector<String> minValues = new Vector<>();
			Vector<String> maxValues = new Vector<>();
			Vector<Boolean> isKey = new Vector<>();
			
			
		try{
			reader = new BufferedReader(new FileReader(file));
			while((line=reader.readLine())!=null)
		    {
				String[] row = line.split(",");

				
				if(!(row[1].toString().equals("Column Name")||row[2].toString().equals("Column Type")))
				{
					String tablename = row[0];
			        if(tableName.equals(tablename))
				    {
					String fieldname = row[1];
					fieldnames.add(fieldname);
					Vector<String> fields=new Vector<>();
					fields.add(row[1]);
					String type = row[2];
					types.add(type);
					String ckey = row[3];
					isKey.add(Boolean.parseBoolean(ckey));
					String min = row[5];
					minValues.add(min);
					String max = row[6];
					maxValues.add(max);					
					}

     			}

			}
		
			

			

			for(int i=0;i<insertedFields.size();i++)
			{
				if (!(fieldnames.contains(insertedFields.get(i))))

			      throw new DBAppException();
			
				  
			}
			for(int i=0;i<fieldnames.size();i++)
			{
				Boolean foundField = false;
				Boolean correctDataType = false;
				Boolean withinRange = false;
				Boolean keyy = false;
				Boolean setNull=true;

				
				

				
				Iterator<Entry<String, Object>> iteratortype1 = columnNameValue.entrySet().iterator();

                
			
				while(iteratortype1.hasNext())
				{
					foundField=false;
					correctDataType=false;
					withinRange=false;
					keyy=false;

					
	
					Entry<String, Object> entry = iteratortype1.next();
					String DataType=entry.getValue().getClass().getName();

					if((entry.getKey()).equals(fieldnames.get(i)))
					{

					  foundField = true;
					  setNull = false;

					  if(isKey.get(i))
					  {
					  clustkey = entry.getKey();
					  clustvalue = entry.getValue();
					  keyy=true;
					  }

					
					  
					
                   
					if(types.get(i).equals(DataType))
					{
					  correctDataType = true;
					  if (DataType.equals("java.lang.Integer"))
						{
							int minvalue;
							int maxvalue;

								minvalue=Integer.parseInt(minValues.get(i));
								maxvalue=Integer.parseInt(maxValues.get(i));
					  
								if((((Integer)entry.getValue()<=maxvalue) && ((Integer)entry.getValue()>=minvalue)))
								  withinRange = true;

								else if(foundField) throw new DBAppException();

								
								
								if(!(keyy))
								{ 
								  temptuple.add(entry.getValue());
								}

								
						}

						else if (DataType.equals("java.lang.Double"))
						{
							double minvalue;
							double maxvalue;
								minvalue=Double.parseDouble(minValues.get(i));
								maxvalue=Double.parseDouble(maxValues.get(i));

								if((((double)entry.getValue()<=maxvalue) && ((double)entry.getValue()>=minvalue)))
									 withinRange = true;
									 
							    else if(foundField) throw new DBAppException();
								if(!(keyy))
								{ 
								  temptuple.add(entry.getValue());
								}
								
						} 

						else if (DataType.equals("java.lang.String"))
						{
							
							if((((String)entry.getValue()).compareTo(minValues.get(i))>0) && ((String)entry.getValue()).compareTo(maxValues.get(i))<0)
								  withinRange = true;

							else if(foundField) throw new DBAppException();	
							if(!(keyy))
								{ 
								  temptuple.add(entry.getValue());
								}
						} 

						else if (DataType.equals("java.util.Date"))
						{
							Date minvalue;
							Date maxvalue;

							DateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");

							DateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd");


							minvalue = dateFormat1.parse(minValues.get(i));
							maxvalue = dateFormat2.parse(maxValues.get(i));

							

							Date entryDate;

							DateFormat dateFormat3 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);


							entryDate = dateFormat3.parse(entry.getValue().toString());


	


							if((entryDate.compareTo(minvalue)>0) && entryDate.compareTo(maxvalue)<0)
								 withinRange = true;
							 else if(foundField) throw new DBAppException();	
							 if(!(keyy))
							 { 
							   temptuple.add(entryDate);
							 }
						} 
						
					
					}

					else if(foundField) throw new DBAppException();
				}

				}

				if (setNull) temptuple.add(null);

				
			}

			

			for(int i=0;i<temptuple.size();i++)
		    tuple.add(temptuple.get(i));
		
		
			
		
			int PageIndex = binarySearchPages(t, clusteringKeyValue);

			
		
			Page p = deserializePage(t.getPages().get(PageIndex));

			Object tmp = p.getTuples().get(0).getData().get(0);

			if (tmp instanceof Integer)
			clustvalue = Integer.parseInt(clusteringKeyValue);

			else if (tmp instanceof Double)
			clustvalue = Double.parseDouble(clusteringKeyValue);

			else if (tmp instanceof String)
			clustvalue = clusteringKeyValue;

			else if (tmp instanceof Date)
			{

				DateFormat df = new  SimpleDateFormat("yyyy-MM-dd", Locale.US);


				clustvalue = df.parse(clusteringKeyValue);
			
			}
		
			int UpdateIndex = binarySelect(p, clustvalue);

			Tuple tup = p.getTuples().get(UpdateIndex);

			for (int i = 0 ; i<t.getGridIndices().size();i++)
			{
				t.getGridIndices().get(i).deleteFromGrid(tup);
			}

			Tuple nw = new Tuple();

			for(int i=0; i<tup.getData().size();i++)
			{
				if (tuple.getData().get(i) == null)
				nw.add(tup.getData().get(i));

				else nw.add(tuple.getData().get(i));

			}

			


			p.getTuples().set(UpdateIndex, nw);

			for (int i = 0 ; i<t.getGridIndices().size();i++)
			{
				t.getGridIndices().get(i).addToGrid(nw,p);
			}

			serializePage(p);
			serializeTable(t);
	    
	
	    }
		catch (IOException | ParseException e) 
		{
			e.printStackTrace();
		}
		}


        

		
	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        
		Tuple tuple = new Tuple();
		Vector<Object> temptuple = new Vector<Object>();
       
		
            String file = "src/main/resources/metadata.csv";
	        BufferedReader reader = null;
			String line ="";
			String clustkey = "";
		    Object clustvalue = null;
			Vector<String> fieldnames = new Vector<>();
			Vector<String> types = new Vector<>();
			Vector<String> minValues = new Vector<>();
			Vector<String> maxValues = new Vector<>();
			Vector<Boolean> isKey = new Vector<>();
			
			
		try{
			reader = new BufferedReader(new FileReader(file));
			while((line=reader.readLine())!=null)
		    {
				String[] row = line.split(",");

				
				if(!(row[1].toString().equals("Column Name")||row[2].toString().equals("Column Type")))
				{
					String tablename = row[0];
			        if(tableName.equals(tablename))
				    {
					String fieldname = row[1];
					fieldnames.add(fieldname);
					Vector<String> fields=new Vector<>();
					fields.add(row[1]);
					String type = row[2];
					types.add(type);
					String ckey = row[3];
					isKey.add(Boolean.parseBoolean(ckey));
					String min = row[5];
					minValues.add(min);
					String max = row[6];
					maxValues.add(max);					
					}

     			}

			}
		
			Vector<String> insertedFields=new Vector<>();
			Vector<Object> insertedvalues = new Vector<>();

			columnNameValue.forEach((k,v)->{

				insertedFields.add(k);
				insertedvalues.add(v);  
			});

			

			for(int i=0;i<insertedFields.size();i++)
			{
				if (!(fieldnames.contains(insertedFields.get(i))))

			      throw new DBAppException();
			
				  
			}
			for(int i=0;i<fieldnames.size();i++)
			{
				Boolean foundField = false;
				Boolean correctDataType = false;
				Boolean withinRange = false;
				Boolean keyy = false;
				Boolean setNull=true;

				
				

				
				Iterator<Entry<String, Object>> iteratortype1 = columnNameValue.entrySet().iterator();

                
			
				while(iteratortype1.hasNext())
				{
					foundField=false;
					correctDataType=false;
					withinRange=false;
					keyy=false;

					
	
					Entry<String, Object> entry = iteratortype1.next();
					String DataType=entry.getValue().getClass().getName();

					if((entry.getKey()).equals(fieldnames.get(i)))
					{

					  foundField = true;
					  setNull = false;

					  if(isKey.get(i))
					  {
					  clustkey = entry.getKey();
					  clustvalue = entry.getValue();
					  keyy=true;
					  }

					
					  
					
                   
					if(types.get(i).equals(DataType))
					{
					  correctDataType = true;
					  if (DataType.equals("java.lang.Integer"))
						{
							int minvalue;
							int maxvalue;

								minvalue=Integer.parseInt(minValues.get(i));
								maxvalue=Integer.parseInt(maxValues.get(i));
					  
								if((((Integer)entry.getValue()<=maxvalue) && ((Integer)entry.getValue()>=minvalue)))
								  withinRange = true;

								else if(foundField) throw new DBAppException();

								
								
								if(!(keyy))
								{ 
								  temptuple.add(entry.getValue());
								}

								
						}

						else if (DataType.equals("java.lang.Double"))
						{
							double minvalue;
							double maxvalue;
								minvalue=Double.parseDouble(minValues.get(i));
								maxvalue=Double.parseDouble(maxValues.get(i));

								if((((double)entry.getValue()<=maxvalue) && ((double)entry.getValue()>=minvalue)))
									 withinRange = true;
									 
							    else if(foundField) throw new DBAppException();
								if(!(keyy))
								{ 
								  temptuple.add(entry.getValue());
								}
								
						} 

						else if (DataType.equals("java.lang.String"))
						{
							
							if((((String)entry.getValue()).compareTo(minValues.get(i))>0) && ((String)entry.getValue()).compareTo(maxValues.get(i))<0)
								  withinRange = true;

							else if(foundField) throw new DBAppException();	
							if(!(keyy))
								{ 
								  temptuple.add(entry.getValue());
								}
						} 

						else if (DataType.equals("java.util.Date"))
						{
							Date minvalue;
							Date maxvalue;

							DateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");

							DateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd");


							minvalue = dateFormat1.parse(minValues.get(i));
							maxvalue = dateFormat2.parse(maxValues.get(i));

							

							Date entryDate;

							DateFormat dateFormat3 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);


							entryDate = dateFormat3.parse(entry.getValue().toString());


	


							if((entryDate.compareTo(minvalue)>0) && entryDate.compareTo(maxvalue)<0)
								 withinRange = true;
							 else if(foundField) throw new DBAppException();	
							 if(!(keyy))
							 { 
							   temptuple.add(entryDate);
							 }
						} 
						
					
					}

					else if(foundField) throw new DBAppException();
				}

				}

				if (setNull) temptuple.add(null);
			}
		}
				catch (IOException | ParseException e) 
				{
					e.printStackTrace();
				}

				tuple.add(clustvalue);
			
				for(int i=0;i<temptuple.size();i++)
				tuple.add(temptuple.get(i));
		

				Table t = deserializeTable(tableName);

				for(int i =0; i<t.getGridIndices().size(); i++)
				{
					try {
						t.getGridIndices().get(i).deleteFromGrid(tuple);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

		

				if(clustvalue != null)
				{
					int PageIndex = binarySearchPages(t, clustvalue);
					Page p = deserializePage(t.getPages().get(PageIndex));
					int TupleIndex = binarySelect(p, clustvalue);
					Tuple x = p.getTuples().get(TupleIndex);
					Boolean delete = true;
					for(int i =0; i<tuple.getData().size();i++)
					{
						if( (tuple.getData().get(i) != null) && (tuple.getData().get(i) != x.getData().get(i)))
						  delete = false;


					}
					if(delete) 
					{
						p.getTuples().remove(TupleIndex);
						if(p.getTuples().size()==0)
						t.getPages().remove(PageIndex);
						else
						serializePage(p);
						serializeTable(t);
					}

				}
				else
				{
					Boolean delete = true; 
					for(int i = 0 ; i<t.getPages().size();i++)
					{
						Page p = deserializePage(t.getPages().get(i));
						for(int j = 0;j<p.getTuples().size();j++)
						{
							delete = true;
					       for(int z = 0 ; z<p.getTuples().get(j).getData().size();z++)
						   {
							if( (tuple.getData().get(z) != null) && (tuple.getData().get(i) != p.getTuples().get(j).getData().get(z)))
							delete = false;
                           }
					        if(delete) 
					       {
						  p.getTuples().remove(j);
						  if(p.getTuples().size()==0)
						  {
						  t.getPages().remove(i);
						  i--;
						  }
						  else
						  serializePage(p);
						  
					       }


						   }
						}
					}
					serializeTable(t);

				}

		

			
	



	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		
		Vector <Vector<Tuple>> results = new Vector<>();

		for(int i=0 ; i<sqlTerms.length ;i++)
		{
			Vector<Tuple> r = new Vector<>();

			Table t = deserializeTable(sqlTerms[i]._strTableName);

			for (int j=0 ; j<t.getPages().size() ; j++)
			{
				Page p = deserializePage(t.getPages().get(j));
				
				for (int k = 0 ; k<p.getTuples().size() ; k++)
				{
					Tuple tup = p.getTuples().get(k);

					for (int l = 0 ; l<tup.getColNames().size(); l++)
					{
						if (tup.getColNames().get(l).equals(sqlTerms[i]._strColumnName))
						{
							Object obj = tup.getData().get(l);

							String op = sqlTerms[i]._strOperator;

							switch(op)
							{
								case "=": if(obj.equals(sqlTerms[i]._objValue)) r.add(tup);break;
								case "!=": if(!(obj.equals(sqlTerms[i]._objValue))) r.add(tup);break;
								case "<" : if(obj.toString().compareTo(sqlTerms[i]._objValue.toString())<0) r.add(tup);break;
								case "<=" : if(obj.toString().compareTo(sqlTerms[i]._objValue.toString())<=0) r.add(tup);break;
								case ">" : if(obj.toString().compareTo(sqlTerms[i]._objValue.toString())>0) r.add(tup);break;
								case ">=" : if(obj.toString().compareTo(sqlTerms[i]._objValue.toString())>=0) r.add(tup);break;
							}

						}
					}


				}



			}

			results.add(r);
			
		}


		String op = arrayOperators[0];

		Vector<Tuple> output = new Vector<>();

		if (op.equals("AND"))
		{
			Vector <Tuple> r = results.get(0);

			for(int i=0; i<r.size(); i++)
			{
				boolean flag = true;
				Tuple cur = r.get(i);

				for(int j = 1; j<results.size();j++)
				{
					if(!(results.get(j).contains(cur)))
					{
						flag = false;
					    break;
					}

				}

				if(flag) output.add(cur);
			}
		}

		else if (op.equals("OR"))
		{
			Vector <Tuple> r = results.get(0);

			for(int i=0; i<r.size(); i++)
			{
			 output.add(r.get(i));
			}

			 for(int j = 1; j<results.size();j++)
			 {
				 Vector<Tuple> ru = results.get(j);

				 for(int k=0 ; k<ru.size(); k++)
				 {
					 if(!(output.contains(ru.get(k))))
					 output.add(ru.get(k));

				 }


			 }
			
		}

		else if (op.equals("XOR"))
		{

			Vector <Tuple> r = results.get(0);

			for(int i=0; i<r.size(); i++)
			{
			 output.add(r.get(i));
			}

			 for(int j = 1; j<results.size();j++)
			 {
				 Vector<Tuple> ru = results.get(j);

				 for(int k=0 ; k<ru.size(); k++)
				 {
					 if(!(output.contains(ru.get(k))))
					 output.add(ru.get(k));

					 else output.remove(ru.get(k));

				 }


			 }


		}

		

		Iterator<Tuple> res = output.iterator();

		return res;

	}
	
		


}
