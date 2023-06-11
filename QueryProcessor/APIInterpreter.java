import java.util.ArrayList;
import java.util.List;

public class APIInterpreter {
    private QueryEvaluationEngine queryEvaluationEngine;
    public APIInterpreter(){
        queryEvaluationEngine = new QueryEvaluationEngine();
    }
    public boolean createTable(List<String> input){
        try {
            String tableName = input.get(0);
            List<String> columns = new ArrayList<>();
            columns.add("no int(10) not null auto_increment primary key");
            columns.addAll(input.subList(1,input.size()));
            queryEvaluationEngine.createTable(tableName, columns);
            return true;
        }catch(Exception e){
            return false;
        }
    }
    public boolean saveData(String name, List<String> column, String input){
        try {
            queryEvaluationEngine.saveData(name, column, input);
            return true;
        }catch(Exception e){
            return false;
        }

    }
    public boolean createIndex(String name, String input){
        try{
            queryEvaluationEngine.createIndex(name, input);
            return true;
        }catch(Exception e){
            return false;
        }

    }
    public Result seectLibGenre(String table, String lib, String genre) throws Exception{
        return queryEvaluationEngine.selectLibGenre(table, lib, genre);

    }
    public int countLibGenre(String lib, String genre) throws Exception{
        return queryEvaluationEngine.countLibGenre(lib, genre);

    }
    public Result selectGenreStatusIn(String table, String genre) throws Exception{
        return queryEvaluationEngine.selectGenreStatusIn(table, genre);
    }
    public Result selectLibGenreStatusIn(String table, String lib, String genre) throws Exception{
        return queryEvaluationEngine.selectLibGenreStatusIn(table, lib, genre);
    }
    public int countGenreStatusIn(String genre) throws Exception{
        return queryEvaluationEngine.countGenreStatusIn(genre);
    }
    public int countLibGenreStatusIn(String lib, String genre) throws Exception{
        return queryEvaluationEngine.countLibGenreStatusIn(lib, genre);
    }
    public int countLib(String lib) throws Exception{
        return queryEvaluationEngine.countLib(lib);
    }

}
