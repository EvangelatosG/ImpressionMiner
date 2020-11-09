import java.util.ArrayList;
import java.util.Scanner;

//import java.io.IOException;

public class Main {

    public static void main(String [ ] args)
    {

        try {


            Scanner scanner = new Scanner(System.in);
            DBConnection mainDB = DBConnection.getInstance();

            ArrayList<Thread> threadsList= new ArrayList<Thread>(); //ArrayList which stores all the threads that search keywords in URLs.

            boolean isRunning = true;
            int choice = 0;


            while(isRunning) {



                System.out.print(
                        "\n=========================\n"+
                        "||         Menu        ||\n"+
                        "=========================\n"+
                        "0. Interrupt execution\n"+
                        "1. View saved URLs\n"+
                        "2. Insert URL\n"+
                        "3. Delete URL\n"+
                        "4. Edit URL\n"+
                        "5. Enter Keywords to search in URLs. Search Results will be saved in Database\n"+
                        "6. Exit Application\n"+
                        "7. View Keyword Searches in URLs and Negative-Positive impressions\n"+
                        "8. View Keyword Searches in URLs and Negative-Positive impressions by Date\n"+
                        "9. Export Results to csv Files\n"+
                        "10. Insert Negative Term\n"+
                        "11.Delete Negative Term\n"+
                        "12.Insert Positive Term\n"+
                        "13.Delete Positive Term\n"+
                        "14.View Negative-Positive terms\n"+
                        "=========================\n"+
                        "Please choose an option :\n"
                );

                if (scanner.hasNextInt()) {
                    choice = scanner.nextInt();
                }

                else{
                    System.out.println("Please give an integer!! ");
                    scanner.next();

                    continue;
                }

                switch (choice) {
                    case 0:

                        if(!threadsList.isEmpty()) {
                            for (Thread t : threadsList) {
                                t.interrupt();
                            }
                            System.out.println("Threads interrupted !!!");
                        }
                        else
                            System.out.println("No threads were found to interrupt.");

                        break;
                    case 1:

                        mainDB.printAllUrls();
                        break;
                    case 2:

                        System.out.println("Enter URL:");
                        String url;
                        if(scanner.hasNext()){
                            url = scanner.next();
                        }
                        else{
                            System.out.println("Not a valid URL!!! Return to main menu.");
                            scanner.nextLine();
                            break;
                        }
                        System.out.println("Rows inserted : " + mainDB.insertUrl(url));

                        break;
                    case 3:

                        int urlId;
                        System.out.println("Available URLs:");
                        mainDB.printAllUrls();
                        System.out.println("Please select the ID of the URL you wish to delete:");
                        if (scanner.hasNextInt()){
                            urlId = scanner.nextInt();
                        }
                        else{
                            System.out.println("Not a valid number!!! Return to main menu.");
                            scanner.next();
                            break;
                        }

                        System.out.println("Rows deleted : " + mainDB.deleteUrlbyID(urlId));

                        break;
                    case 4:

                        int urlId4; String newURL;
                        System.out.println("Available URLs:");
                        mainDB.printAllUrls();
                        System.out.println("Please select the ID of the URL you wish to edit:");
                        if (scanner.hasNextInt()){
                            urlId4 = scanner.nextInt();
                        }
                        else{
                            System.out.println("Not a valid number!!! Return to main menu.");
                            scanner.next();
                            break;
                        }
                        System.out.println("Please give the new URL:");

                        if (scanner.hasNext()){
                            newURL = scanner.next();
                        }
                        else{
                            System.out.println("Not a valid URL!!! Return to main menu.");
                            scanner.next();
                            break;
                        }
                        System.out.println("Rows edited : " + mainDB.editUrlbyID(urlId4,newURL));

                        break;
                    case 5:
                        threadsList.clear();

                        //Read the number of keywords
                        System.out.println("Enter the Number of keywords for search");
                            //Not use a new scanner. //Scanner keyScan = new Scanner(System.in);
                        Integer numKey;
                        if (scanner.hasNextInt()){
                            numKey = scanner.nextInt();
                        }
                        else{
                            System.out.println("Not a valid number!!! Return to main menu.");
                            scanner.nextLine();
                            break;
                        }
                        //Read the keywords
                        ArrayList<String> keyArrayList = new ArrayList<String>();   //Stores all the keywords to be searched in all URLs.
                        System.out.println("Enter the keywords that you will search");
                        //Read keywords from the User
                        for(int i=1;i<=numKey;i++){
                            System.out.println("Enter keyword "+ i+" : ");
                            keyArrayList.add(scanner.next());

                        }
                        //Check Array List content
                        for (String arr:keyArrayList) {
                            System.out.println("keyword \""+arr+"\" was entered");
                        }

                        //Store all URL IDs  to urlIdsArrayList  ArrayList .
                        ArrayList<Integer> urlIdsArrayList = mainDB.selectAllUrlsId();
                        String stringHtml;

                        //Check URLs ArrayList. Print all URLs to be searched.
                        System.out.println("============All URLs to be searched=============");
                        for (Integer testUrl:urlIdsArrayList) {
                            System.out.println(mainDB.selectUrlbyID(testUrl));
                        }
                        System.out.println("================================================");


                        int i = 0; //i = 1;

                        for (Integer selectedUrlId : urlIdsArrayList) {
                            threadsList.add(new Thread(new IMThread(String.valueOf(i),selectedUrlId,keyArrayList)));  //Create new thread that will search all the keywords in a URL.
                            threadsList.get(i).start(); //Start the execution of the new thread
                            i++;
                            }

                        /*Join all searching threads, with the main thread.*/
                        for (Thread t : threadsList) {
                            try {
                                t.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }

                        threadsList.clear();    //Clear the thread ArrayList
                        break;
                    case 6:

                        System.out.println("---Exiting---");
                        isRunning = false;
                        break;
                    case 7:

                        mainDB.selectAllResults();
                        break;
                    case 8:
                        String date1,date2;
                        DateFormater df = new DateFormater(); //Create new DateFormater object, for checking user's date format.
                        System.out.println("Please give 2 dates between where to search. Date format must be yyyy-mm-dd .For example: 2019-09-05 .");
                        System.out.println("Please give the 1st date:");
                        date1 = scanner.next();
                        if (df.isValidDate(date1)){
                            System.out.println("Please give the 2nd date:");
                            date2 = scanner.next();
                            if (df.isValidDate(date2)){
                                System.out.println();
                                mainDB.selectAllResultsByDate(date1,date2);
                            }
                            else
                                System.out.println("Wrong Date Format.");
                        }
                        else
                            System.out.println("Wrong Date Format.");

                        break;
                    case 9:
                        ExportResults exportResults = new ExportResults();
                        exportResults.exportKeywordsFile(); //Export URL keyword searches , to csv file.
                        exportResults.exportNegativeFile(); //Export negative impressions found in URL keyword searches.
                        exportResults.exportPositiveFile(); //Export positive impressions found in URL keyword searches.

                        break;
                    case 10:
                        System.out.println("Enter Negative term:");

                        String nterm;
                        if(scanner.hasNext()){
                            nterm = scanner.next();
                        }
                        else{
                            System.out.println("Not a valid term!!! Return to main menu.");
                            scanner.nextLine();
                            break;
                        }

                        mainDB.insertNegativeTerm(nterm);
                        //urlScan.close();
                        break;
                    case 11:

                        int nId;
                        System.out.println("Available Negative terms:");
                        mainDB.selectNegativeTerms();
                        System.out.println("Please select the ID of the Negative term you wish to delete:");
                        if (scanner.hasNextInt()){
                            nId = scanner.nextInt();
                        }
                        else{
                            System.out.println("Not a valid Id!!! Return to main menu.");
                            scanner.next();
                            break;
                        }

                        System.out.println("Rows deleted : " + mainDB.deleteNegativebyID(nId));

                        break;
                    case 12:
                        System.out.println("Enter Positive term:");

                        String pterm;
                        if(scanner.hasNext()){
                            pterm = scanner.next();
                        }
                        else{
                            System.out.println("Not a valid term!!! Return to main menu.");
                            scanner.nextLine();
                            break;
                        }


                        mainDB.insertPositiveTerm(pterm);
                        //urlScan.close();
                        break;
                    case 13:

                        int pId;
                        System.out.println("Available Positive terms:");
                        mainDB.selectPositiveTerms();
                        System.out.println("Please select the ID of the Positive term you wish to delete:");
                        if (scanner.hasNextInt()){
                            pId = scanner.nextInt();
                        }
                        else{
                            System.out.println("Not a valid Id!!! Return to main menu.");
                            scanner.next();
                            break;
                        }

                        System.out.println("Rows deleted : " + mainDB.deletePositivebyID(pId));

                        break;
                    case 14:

                        mainDB.selectNegativeTerms();
                        mainDB.selectPositiveTerms();
                        break;
                    default:
                        // The user input an unexpected choice.
                        System.out.println("Please choose an option between 0 - 14. Please try again");

                }

            }
            scanner.close();

        }
        catch(IllegalArgumentException a){
            a.printStackTrace();

        }


    }


}
