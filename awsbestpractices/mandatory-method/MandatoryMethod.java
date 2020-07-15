import javax.swing.ImageIcon;
import com.amazonaws.services.simplesystemsmanagement.model.PutParameterRequest;

public class MandatoryMethod {

    public void checkWindowDescription() {
        //Should not flag
        CreateMaintenanceWindowRequest maintainanceWindow = new CreateMaintenanceWindowRequest();
        maintainanceWindow.setDescription("Description");

        //Should not flag
        CreateMaintenanceWindowRequest maintainanceWindow2 = new CreateMaintenanceWindowRequest();
        maintainanceWindow2.withDescription("Description");

        //Should flagS
        CreateMaintenanceWindowRequest maintainanceWindow3 = new CreateMaintenanceWindowRequest();
        System.out.println(maintainanceWindow3);

    }
    public void checkParameterDescription() {

        //Should not flag
        PutParameterRequest putParameterRequest = new PutParameterRequest();
        putParameterRequest.setDescription("Description");

        //Should flag
        PutParameterRequest putParameterRequest2 = new PutParameterRequest();
        System.out.println(putParameterRequest2);

    }
}
