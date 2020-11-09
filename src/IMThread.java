import org.jsoup.Jsoup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

public class IMThread implements Runnable{
    private String name;
    private Integer selectedUrlId;
    private ArrayList<String> keyArrayList;

   private static ReentrantLock lock = new ReentrantLock(); //If not static, it gives error ([SQLITE_BUSY] Database is locked)

    //Constructor
    public IMThread(String threadName, Integer urlid, ArrayList<String> keys){
        this.name = threadName;
        this.selectedUrlId = urlid;
        this.keyArrayList = keys;
        /*t = new Thread(this,name);
        t.start();*/
}

    //Run method of the Thread
    public void run() {

        if (Thread.interrupted()) { //Check if thread is interrupted.
            System.out.println(Thread.currentThread().getName() + " is interrupted");
            return;
        }

        String stringHtml;
        DBConnection db4 = DBConnection.getInstance();

        lock.lock();    //Lock area for multithreading.Only 1 thread can enter here each time.Without this lock, [SQLITE_BUSY]-database is locked errors occurs.
        try {

            //Search the URL for the keywords
            stringHtml = Jsoup.connect(db4.selectUrlbyID(selectedUrlId)).timeout(0).get().text();


        int k = 0;
        int count = 0;
        String impressionsStr;  // String with 100 characters search for positive-negative impressions .

        int keywordsInsertId;
        ArrayList<Integer> negativeTermsId= new ArrayList<>();
        ArrayList<Integer> positiveTermsId= new ArrayList<>();

        int negid, posid;


            //Loop through all the keywords given by the user.
            for (String keyword : keyArrayList) {

                if (Thread.interrupted()) { //Check if thread is interrupted.
                    System.out.println(Thread.currentThread().getName() + " is interrupted");
                    return;
                }

                //Regular expressions
                Pattern ptrn = Pattern.compile(keyword, CASE_INSENSITIVE);
                Matcher mtchr = ptrn.matcher(stringHtml);
                keywordsInsertId = db4.insertKeyword(selectedUrlId, keyword, 0); //Inserts a new row in keywords table and returns its id .

                while (mtchr.find(k)) { //Resets this matcher and then attempts to find the next subsequence of the input sequence that matches the pattern, starting at the specified index.
                    count++;
                    k = mtchr.start() + 1;  //Returns the start index of the previous match(start()).

                    //Create the String(impressionStr) that will be used for negative-positive impression checking.
                    if (k > 50 && stringHtml.length() > k + 50) {
                        impressionsStr = stringHtml.substring(k - 50, k + 50);
                    } else if (k < 50 && stringHtml.length() > k + 50) {
                        impressionsStr = stringHtml.substring(0, k + 50);
                    } else if (k > 50 && stringHtml.length() < k + 50) {
                        impressionsStr = stringHtml.substring(k - 50, stringHtml.length());
                    } else {
                        impressionsStr = stringHtml.substring(0, stringHtml.length());
                    }


                    negid = db4.searchNegativeImpressions(selectedUrlId, keywordsInsertId, impressionsStr); //Get the id of the negative term found, or -1 if no term was found.
                    posid = db4.searchPositiveImpressions(selectedUrlId, keywordsInsertId, impressionsStr); //Get the id of the positive term found, or -1 if no term was found.

                    //Add the term id to the negative and positive ArrayLists.
                    if(negid != -1)
                        negativeTermsId.add(negid);
                    if(posid != -1)
                        positiveTermsId.add(posid);
                }

                if(count != 0){
                    db4.updateKeywordsCountById(count,keywordsInsertId);    //Update the keywords database table with the number of times the keyword was found in the URL.

                }

                //Print the results of keyword and positive-negative term search.
                System.out.println("\n==========Keywords Count==========");
                System.out.println("URL: "+ db4.selectUrlbyID(selectedUrlId)+" ,Keyword: "+keyword+" ,Times Shown: "+count);

                System.out.println("==========Negative Terms==========");
                if(negativeTermsId.isEmpty()){
                    System.out.println("No negative terms were found.");
                }
                else{
                    for(int nterm : negativeTermsId){
                        System.out.println("Negative term id:" + nterm);
                        System.out.println("URL: "+db4.selectUrlbyID(selectedUrlId)+" Keyword: "+keyword+" Negative term: "+db4.selectNegativeTermByID(nterm));
                    }
                }

                System.out.println("==========Positive Terms==========");
                if(positiveTermsId.isEmpty()){
                    System.out.println("No positive terms were found.");
                }
                else{
                    for(int pterm : positiveTermsId){
                        System.out.println("Positive term id:" + pterm);
                        System.out.println("URL: "+db4.selectUrlbyID(selectedUrlId)+" Keyword: "+keyword+" Positive term: "+db4.selectPositiveTermByID(pterm));
                    }
                }

                k = 0;
                count = 0;
                negativeTermsId.clear();    //Clear ArrayLists that store negative-positive term ids.
                positiveTermsId.clear();


            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return;
        } catch (IOException e) {
            e.printStackTrace();
            return;
        } catch(Exception e){
            e.printStackTrace();
            return;
        }
        finally{
            lock.unlock();  //Unlock area.
        }

    }
}
