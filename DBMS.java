import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DBMS { //전체 메인 코드
    private APIInterpreter apiInterpreter;
    private ConsoleDisplay consoleDisplay;
    public DBMS(){
        apiInterpreter = new APIInterpreter();
        consoleDisplay = new ConsoleDisplay();
    }
    public void inputErrorPrint(){
        this.consoleDisplay.printString("입력값이 잘못됐습니다. 다시 입력해주세요.");
    }
    public void resultErrorPrint(){
        this.consoleDisplay.printString("작업 과정 중 오류가 발생했습니다. 다시 시도해주세요.");
    }
    public boolean resultPrint(){
        this.consoleDisplay.printString("실행이 완료됐습니다. 메인화면으로 돌아가려면 1, 메뉴 화면으로 돌아가려면 2를 입력해주세요.");
        int input = this.consoleDisplay.inputInt();
        if(input==1){
            return true;
        }else if(input==2){
            return false;
        }else{
            inputErrorPrint();
            return resultPrint();
        }
    }
    public void menuSelect(){
        this.consoleDisplay.printString("1.테이블 생성 \n2.데이터 저장 \n3.인덱스 생성 \n4.쿼리 처리");
        int input = this.consoleDisplay.inputInt();
        if (input == 1) {
            List<String> tableInput = new ArrayList<>();
            this.consoleDisplay.printString("생성할 테이블의 테이블명을 입력해주세요.");
            String table = this.consoleDisplay.inputStr();
            this.consoleDisplay.printString("테이블의 컬럼명, 데이터 형을 컬럼명 자료형(크기) 순으로 입력해주세요. (ex.id varchar(10)) 크기가 존재하지 않는 자료형의 경우 크기는 생략해도 됩니다.\r\n 한줄에는 한 컬럼에 대한 정보만 입력하고, 복수개의 줄로 입력해주면 됩니다. 입력을 끝내려면 -1을 입력해주세요.");
            List<String> columns = this.consoleDisplay.inputStrList();
            tableInput.add(table);
            tableInput.addAll(columns);
            boolean result = apiInterpreter.createTable(tableInput);
            if(result) {
                if (!resultPrint()) {
                    menuSelect();
                }
            }else{
                resultErrorPrint();
            }
        } else if (input == 2) {
            this.consoleDisplay.printString("데이터를 삽입할 테이블의 테이블명을 입력해주세요.");
            String table = this.consoleDisplay.inputStr();
            this.consoleDisplay.printString("삽입할 데이터에 존재하는 컬럼명을 순서대로 한 줄씩 입력해주세요. 입력을 끝내려면 -1을 입력해주세요.");
            List<String> columns = this.consoleDisplay.inputStrList();
            this.consoleDisplay.printString("삽입할 데이터의 파일 위치를 입력해주세요. 데이터 파일의 컬럼 순서는 앞서 컬럼을 입력한 순서와 동일해야합니다.");
            String dir = this.consoleDisplay.inputStr();
            boolean result = apiInterpreter.saveData(table, columns, dir);
            if(result){
                if(!resultPrint()){
                    menuSelect();
                }
            }else{
                resultErrorPrint();
            }
        } else if (input == 3) {
            this.consoleDisplay.printString("인덱스를 생성할 테이블명을 입력해주세요.");
            String table = this.consoleDisplay.inputStr();
            this.consoleDisplay.printString("인덱스를 생성할 컬럼 이름을 입력해주세요");
            String column = this.consoleDisplay.inputStr();
            boolean result = apiInterpreter.createIndex(table, column);
            if(result){
                if(!resultPrint()){
                    menuSelect();
                }
            }else{
                resultErrorPrint();
            }

        } else if (input == 4) {
            this.consoleDisplay.printString("쿼리를 사용할 테이블 명을 입력해주세요.\n");
            String table = this.consoleDisplay.inputStr();
            this.consoleDisplay.printString("사용할 쿼리의 번호를 입력해주세요. 사용할 수 있는 쿼리는 다음과 같습니다.\n");
            this.consoleDisplay.printString("1.특정 도서관의 원하는 장르 도서 목록에 대한 검색\n" +
                    "2.특정 도서관의 원하는 장르의 도서 개수에 대한 검색\n" +
                    "3.원하는 장르의 대출 가능한 도서 목록에 대한 검색\n" +
                    "4.특정 도서관의 원하는 장르의 대출 가능한 도서 목록에 대한 검색\n" +
                    "5.원하는 장르의 대출 가능한 도서 개수에 대한 검색\n" +
                    "6.특정 도서관의 원하는 장르의 대출 가능한 도서 개수에 대한 검색\n" +
                    "7.특정 도서관의 전체 도서 개수에 대한 검색\n");
            int queryInput = this.consoleDisplay.inputInt();
            try {
                if (queryInput == 1) {
                    this.consoleDisplay.printString("검색을 원하는 도서관명을 정확히 입력해주세요.");
                    String lib = this.consoleDisplay.inputStr();
                    this.consoleDisplay.printString("검색을 원하는 장르명을 입력해주세요. 장르는 다음과 같습니다.\n");
                    this.consoleDisplay.printString("100:기술과학\n200:철학\n300:예술\n400:종교\n500:언어\n600:사회과학\n700:문학\n800:자연과학\n900:역사");
                    String genre = this.consoleDisplay.inputStr();
                    Result result = apiInterpreter.seectLibGenre(table, lib, genre);
                    this.consoleDisplay.printResult(result);
                } else if (queryInput == 2) {
                    this.consoleDisplay.printString("검색을 원하는 도서관명을 정확히 입력해주세요.");
                    String lib = this.consoleDisplay.inputStr();
                    this.consoleDisplay.printString("검색을 원하는 장르명을 입력해주세요. 장르는 다음과 같습니다.\n");
                    this.consoleDisplay.printString("100:기술과학\n200:철학\n300:예술\n400:종교\n500:언어\n600:사회과학\n700:문학\n800:자연과학\n900:역사");
                    String genre = this.consoleDisplay.inputStr();
                    int result = apiInterpreter.countLibGenre(lib, genre);
                    this.consoleDisplay.printIntResult(result);
                } else if (queryInput == 3) {
                    this.consoleDisplay.printString("검색을 원하는 장르명을 입력해주세요. 장르는 다음과 같습니다.\n");
                    this.consoleDisplay.printString("100:기술과학\n200:철학\n300:예술\n400:종교\n500:언어\n600:사회과학\n700:문학\n800:자연과학\n900:역사");
                    String genre = this.consoleDisplay.inputStr();
                    Result result = apiInterpreter.selectGenreStatusIn(table, genre);
                    this.consoleDisplay.printResult(result);
                } else if (queryInput == 4) {
                    this.consoleDisplay.printString("검색을 원하는 도서관명을 정확히 입력해주세요.");
                    String lib = this.consoleDisplay.inputStr();
                    this.consoleDisplay.printString("검색을 원하는 장르명을 입력해주세요. 장르는 다음과 같습니다.\n");
                    this.consoleDisplay.printString("100:기술과학\n200:철학\n300:예술\n400:종교\n500:언어\n600:사회과학\n700:문학\n800:자연과학\n900:역사");
                    String genre = this.consoleDisplay.inputStr();
                    Result result = apiInterpreter.selectLibGenreStatusIn(table, lib, genre);
                    this.consoleDisplay.printResult(result);
                } else if (queryInput == 5) {
                    this.consoleDisplay.printString("검색을 원하는 장르명을 입력해주세요. 장르는 다음과 같습니다.\n");
                    this.consoleDisplay.printString("100:기술과학\n200:철학\n300:예술\n400:종교\n500:언어\n600:사회과학\n700:문학\n800:자연과학\n900:역사");
                    String genre = this.consoleDisplay.inputStr();
                    int result = apiInterpreter.countGenreStatusIn(genre);
                    this.consoleDisplay.printIntResult(result);
                } else if (queryInput == 6) {
                    this.consoleDisplay.printString("검색을 원하는 도서관명을 정확히 입력해주세요.");
                    String lib = this.consoleDisplay.inputStr();
                    this.consoleDisplay.printString("검색을 원하는 장르명을 입력해주세요. 장르는 다음과 같습니다.\n");
                    this.consoleDisplay.printString("100:기술과학\n200:철학\n300:예술\n400:종교\n500:언어\n600:사회과학\n700:문학\n800:자연과학\n900:역사");
                    String genre = this.consoleDisplay.inputStr();
                    int result = apiInterpreter.countLibGenreStatusIn(lib, genre);
                    this.consoleDisplay.printIntResult(result);
                } else if (queryInput == 7) {
                    this.consoleDisplay.printString("검색을 원하는 도서관명을 정확히 입력해주세요.");
                    String lib = this.consoleDisplay.inputStr();
                    int result = apiInterpreter.countLib(lib);
                    this.consoleDisplay.printIntResult(result);
                } else {
                    inputErrorPrint();
                }
            }catch(Exception e) {
                inputErrorPrint();
            }
        } else {
            inputErrorPrint();
            menuSelect();
        }
    }

    public static void main(String[] args) throws IOException {
        DBMS dbms = new DBMS();
        while(true) {
            dbms.consoleDisplay.printString("1을 입력하면 메뉴 화면으로 진입, 2를 입력하면 시스템이 종료됩니다.");
            int input = dbms.consoleDisplay.inputInt();
            if(input==1){
                dbms.menuSelect();
            }else if(input==2){
                dbms.consoleDisplay.printString("시스템을 종료합니다.");
                break;
            }else{
                dbms.inputErrorPrint();
            }
        }
    }
}
