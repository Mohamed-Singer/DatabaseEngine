public class SQLTerm {

    public String _strTableName;
    public String _strColumnName;
    public String _strOperator;
    public Object _objValue;

    public SQLTerm(){
        _strTableName = null;
        _strColumnName = null;
        _strOperator = null;
        _objValue = null;
    }

    public SQLTerm(String tableName,String columnName,String operator,Object value){

        _strTableName = tableName;
        _strColumnName = columnName;
        _strOperator = operator;
        _objValue = value;

    }

    public String toString(){
        return "Select * From "+_strTableName + " Where " + _strColumnName +" "+ _strOperator +" "+_objValue;
    }
}
