import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class QueryEvaluationEngine {
    private BufferManager bufferManager;
    private FileManager fileManager;
    private JDBC jdbc;
    public QueryEvaluationEngine(){
        fileManager = new FileManager();
        jdbc = new JDBC();
        bufferManager = new BufferManager(fileManager, jdbc);
    }
    public void createTable(String name, List columns) throws SQLException{
        jdbc.setConnection();
        jdbc.createTable(name, columns);
        jdbc.createIndex(name, "no");
        jdbc.closeConnection();
    }
    public void saveData(String name, List columns, String dir) throws Exception{
        String resultName = String.join("_",columns);
        Optional<ByteBuffer> buf = Optional.of(bufferManager.readFile(resultName, dir));
        Charset charset = Charset.defaultCharset();
        String tmp="";
        int block = 0;
        jdbc.setConnection();
        while(buf.isPresent()){
            if(buf.isPresent()){
                tmp = charset.decode(buf.get().flip()).toString();
                List<String> tmpData = List.of(tmp.split("\r\n"));
                for(int i=0; i<tmpData.size(); i++) {
                    jdbc.saveData(name, columns, tmpData.get(i));
                }
            }else{
                break;
            }
            block+=1;
            buf = bufferManager.readNextFile(resultName, block);
        }
        jdbc.closeConnection();
        bufferManager.accessBufferFinish(resultName);
    }
    public void createIndex(String name, String column) throws Exception{
        jdbc.setConnection();
        Optional<ByteBuffer> buf = Optional.of(bufferManager.readJDBCSingleColumn(name, column));
        jdbc.closeConnection();
        Charset charset = Charset.defaultCharset();
        List<String> tmp2 = new ArrayList<>();
        HashSet<String> columns = new HashSet<>();
        int block = 0;
        while(buf.isPresent()) {
            if(buf.isPresent()) {
                tmp2= List.of(charset.decode(buf.get().flip()).toString().split(","));
                columns.addAll(tmp2);
            }else{
                break;
            }
            block+=1;
            jdbc.setConnection();
            buf = bufferManager.readNextJDBCSingleColumn(name, column, block);
            jdbc.closeConnection();
        }

        List<ByteBuffer> columnIndices = new ArrayList<>();
        List<String> resultName = List.of(columns.toArray(new String[0]));
        List<String> tmpBytes = new ArrayList<>();
        for(int i=0; i<columns.toArray(new String[0]).length; i++) {
            columnIndices.add(bufferManager.makeIndexInit(resultName.get(i), getFilePath(resultName.get(i)))); //결과 저장용 버퍼페이지 할당
            fileManager.saveFile(getFilePath(resultName.get(i)));
            tmpBytes.add(i, "");
        }
        jdbc.setConnection();
        buf = Optional.of(bufferManager.readJDBCSingleColumn(name, column)); //jdbc 다시 읽어오기
        jdbc.closeConnection();
        block = 0;
        while(buf.isPresent()) {
            if(buf.isPresent()) {
                tmp2 = List.of(charset.decode(buf.get().flip()).toString().split(","));
                for (int i = 0; i < columnIndices.size(); i++) {
                    for(int j=0; j<tmp2.size(); j++) {
                        if (!columnIndices.get(i).hasRemaining()) {
                            fileManager.writeFile(getFilePath(resultName.get(i)), columnIndices.get(i));
                            bufferManager.diskUpdated(resultName.get(i));
                            columnIndices.get(i).clear();
                        }
                        if(tmpBytes.get(i).length()<8) {
                            tmpBytes.set(i,tmpBytes.get(i)+(tmp2.get(j).equals(resultName.get(i)) ? "1" : "0"));
                        }
                        if(tmpBytes.get(i).length()==8) {
                            columnIndices.get(i).put((byte)Integer.parseInt(tmpBytes.get(i),2));
                            tmpBytes.set(i, "");
                        }
                    }
                }
            }else{
                break;
            }
            block+=1;
            jdbc.setConnection();
            buf = bufferManager.readNextJDBCSingleColumn(name, column, block);
            jdbc.closeConnection();
        }
        for(int i=0; i<columnIndices.size(); i++){
            if(tmpBytes.get(i).length()>0){
                if (!columnIndices.get(i).hasRemaining()) {
                    fileManager.writeFile(getFilePath(resultName.get(i)), columnIndices.get(i));
                    bufferManager.diskUpdated(resultName.get(i));
                    columnIndices.get(i).clear();
                }
                if(tmpBytes.get(i).length()<8){
                    while(tmpBytes.get(i).length()<8){
                        tmpBytes.set(i, tmpBytes.get(i)+"0");
                    }
                }
                columnIndices.get(i).put((byte)Integer.parseInt(tmpBytes.get(i),2));
            }
            if(columnIndices.get(i).position()>0) {
                fileManager.writeFile(getFilePath(resultName.get(i)), columnIndices.get(i));
                bufferManager.diskUpdated(resultName.get(i));
            }
        }

        bufferManager.accessBufferFinish(column);
        for(int i=0; i<columns.toArray(new String[0]).length; i++) {
            bufferManager.accessBufferFinish(resultName.get(i));
        }

    }
    public int count(String value) throws Exception{
        String resultName = "tmp_count_result_"+value+"_"+String.valueOf((int)(Math.random()*100000));
        ByteBuffer resultBuf = bufferManager.countInit(resultName, getFilePath(resultName));
        Optional<ByteBuffer> buf = Optional.of(bufferManager.readIndexFile(value, getFilePath(value)));
        int result = 0;
        int block = 0;
        while(buf.isPresent()) {
            if(buf.isPresent()) {
                int size = buf.get().flip().limit();
                for(int j=0; j<size; j++){
                    result+= Integer.toBinaryString(buf.get().get(j)&0xff).chars().filter(c->c=='1').count();
                }
            }else{
                break;
            }
            block+=1;
            buf = bufferManager.readNextIndexFile(value, block);
        }
        resultBuf.put((byte)result);
        bufferManager.accessBufferFinish(resultName); //결과 저장용 버퍼
        bufferManager.accessBufferFinish(value); //파일 읽어오기용 버퍼
        return result;
    }

    public int countAnd(List<String> columns, List<String> files) throws Exception{
        String resultName = "tmp_" + String.join("_",columns) + "_count_result_"+String.valueOf((int)(Math.random()*100000));
        ByteBuffer resultBuf = bufferManager.countInit(resultName, getFilePath(resultName));
        List<Optional<ByteBuffer>> buf = new ArrayList<>();
        for(int i=0; i<columns.size(); i++) {
            buf.add(i,Optional.of(bufferManager.readIndexFile(columns.get(i), files.get(i)))); //index 파일 최초 불러옴
        }
        int result = 0;
        int block = 0;
        while(buf.get(0).isPresent()) {
            if(buf.get(0).isPresent()) {
                for(int i=0; i<columns.size(); i++){
                    buf.get(i).get().flip();
                }
                int size = buf.get(0).get().limit();
                for(int i=0; i<size; i++){
                    int tmp = -1;
                    for(int j=0; j<columns.size(); j++){
                        tmp = tmp & (buf.get(j).get().get(i)&0xff); //1바이트와 buf의 한 바이트 and 연산
                    }
                    result+= Integer.toBinaryString(tmp&0xff).chars().filter(c->c=='1').count();
                }
            }else{
                break;
            }
            block+=1;
            for(int i=0; i<columns.size(); i++) {
                buf.set(i,bufferManager.readNextIndexFile(columns.get(i), block)); //index 파일 최초 불러옴
            }
        }
        resultBuf.put((byte)result);
        bufferManager.accessBufferFinish(resultName); //결과저장용버퍼
        for(int i=0; i<columns.size(); i++) {
            bufferManager.accessBufferFinish(columns.get(i)); //각 컬럼 value별 버퍼
        }
        return result;
    }
    public Result selectAnd(String name, List<String> columns, List<String> files) throws Exception{
        String resultName = "tmp_" + String.join("_",columns) + "_select_result_"+String.valueOf((int)(Math.random()*100000));
        ByteBuffer resultBuf = bufferManager.countInit(resultName, getFilePath(resultName));
        List<Optional<ByteBuffer>> buf = new ArrayList<>();
        for(int i=0; i<columns.size(); i++) {
            buf.add(i,Optional.of(bufferManager.readIndexFile(columns.get(i), files.get(i))));
        }
        int offset = 1;
        int block = 0;
        fileManager.saveFile(getFilePath(resultName));

        while(buf.get(0).isPresent()) {
            if(buf.get(0).isPresent()) {
                jdbc.setConnection();
                for(int i=0; i<columns.size(); i++){
                    buf.get(i).get().flip();
                }
                int size = buf.get(0).get().limit();
                for(int i=0; i<size;i++){
                    int tmpByte=-1;
                    for(int j=0; j<columns.size(); j++){
                        tmpByte = tmpByte & (buf.get(j).get().get(i)&0xff); //비트 연산 -> 1인 값만 남음
                    }
                    String tmp = String.format("%08d",Integer.parseInt(Integer.toBinaryString(tmpByte)));
                    for(int k=0; k<tmp.length(); k++){
                        if(tmp.substring(k,k+1).equals("1")){
                            ResultSet resultSet = jdbc.getSpecificDataByIndexing(name, String.valueOf(offset + (i*8)+k));
                            if (resultSet.next()) {
                                String tmpStr = (String.join(",", IntStream.rangeClosed(2, resultSet.getMetaData().getColumnCount())
                                        .mapToObj(t -> {
                                            try {
                                                return resultSet.getString(t);
                                            } catch (SQLException e) {
                                                e.printStackTrace();
                                            }
                                            return "";
                                        })
                                        .collect(Collectors.toList())) + "\r\n");
                                if (!resultBuf.hasRemaining() || resultBuf.remaining() < tmpStr.getBytes().length) {
                                    //이번 버퍼사이즈 다 활용 -> 파일에 저장하고 버퍼 비우기
                                    fileManager.writeFile(getFilePath(resultName), resultBuf);
                                    bufferManager.diskUpdated(resultName);
                                    resultBuf.clear();
                                }
                                resultBuf.put(tmpStr.getBytes());
                            }
                        }
                    }
                }
                jdbc.closeConnection();
                offset += size*8;
                block+=1;
            }else{
                break;
            }
            for(int i=0; i<columns.size(); i++) {
                buf.set(i,bufferManager.readNextIndexFile(columns.get(i), block));
            }
        }

        if(resultBuf.position()>0) {
            fileManager.writeFile(getFilePath(resultName), resultBuf);
            bufferManager.diskUpdated(resultName);
        }
        Result result = new Result<>(bufferManager, resultName, getFilePath(resultName));
        bufferManager.accessBufferFinish(resultName);//결과저장용버퍼
        for(int i=0; i<columns.size(); i++) {
            bufferManager.accessBufferFinish(columns.get(i)); //각 컬럼 value별 버퍼
        }
        return result;
    }

    public String getFilePath(String name){
        return "./"+name+".txt";
    }
    public Result selectLibGenre(String table, String lib, String genre) throws Exception{
        return selectAnd(table, List.of(lib,genre), List.of(getFilePath(lib), getFilePath(genre)));

    }

    public int countLibGenre(String lib, String genre) throws Exception{
        return countAnd(List.of(lib,genre),List.of(getFilePath(lib), getFilePath(genre)));
    }

    public Result selectGenreStatusIn(String table, String genre) throws Exception{
        return selectAnd(table, List.of(genre,"IN"),List.of(getFilePath(genre),getFilePath("IN")));
    }

    public Result selectLibGenreStatusIn(String table, String lib, String genre) throws Exception{
        return selectAnd(table, List.of(lib,genre,"IN"), List.of(getFilePath(lib),getFilePath(genre),getFilePath("IN")));
    }

    public int countGenreStatusIn(String genre) throws Exception{
        return countAnd(List.of(genre,"IN"),List.of(getFilePath(genre), getFilePath("IN")));

    }

    public int countLibGenreStatusIn(String lib, String genre) throws Exception{
        return countAnd(List.of(lib,genre,"IN"),List.of(getFilePath(lib),getFilePath(genre),getFilePath("IN")));
    }

    public int countLib(String lib) throws Exception{
        return count(lib);
    }

}
