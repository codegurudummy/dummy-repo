package resourceleak;

import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;

import java.util.List;


public class TestMuGraphCompilationWithJarFiles {

    public static void testMuGraphCompilationWithJarFiles(String[] args) throws Exception {

        String photo = "input.jpg";
        String bucket = "bucket";


        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();

        DetectLabelsRequest request = new DetectLabelsRequest()
                .withImage(new Image()
                        .withS3Object(new S3Object()
                                .withName(photo).withBucket(bucket)))
                .withMaxLabels(10)
                .withMinConfidence(75F);

        try {
            DetectLabelsResult result = rekognitionClient.detectLabels(request);
            List <Label> labels = result.getLabels();

            System.out.println("Detected labels for " + photo);
            for (Label label: labels) {
                System.out.println(label.getName() + ": " + label.getConfidence().toString());
            }
        } catch(AmazonRekognitionException e) {
            e.printStackTrace();
        }
    }
}
