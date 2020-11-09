import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateFormater {

    //Checks if the parameter inDate is in the proper format and return true or false.
    public static boolean isValidDate(String inDate) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setLenient(false);
        try {
            dateFormat.parse(inDate.trim());
        } catch (ParseException pe) {
            return false;
        }
        return true;
    }

}
