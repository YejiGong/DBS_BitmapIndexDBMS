import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ConsoleDisplay {
    public void printString(String string){
        System.out.println(string);
    }
    public void printResult(Result result){
        while(result.iterator().hasNext()){
            System.out.println(result.iterator().next());
        }
    }
    public void printIntResult(int result){
        System.out.println(result);
    }
    public String inputStr(){
        Scanner sc = new Scanner(System.in);
        String input = sc.next();
        return input;

    }
    public int inputInt(){
        Scanner sc = new Scanner(System.in);
        int input = sc.nextInt();
        return input;
    }
    public List<String> inputStrList(){
        Scanner sc = new Scanner(System.in);
        List<String> input = new ArrayList<>();
        while(sc.hasNextLine()) {
            String tmp = sc.nextLine();
            if(tmp.equals("-1")){
                break;
            }else {
                input.add(tmp);
            }
        }
        return input;
    }
}
