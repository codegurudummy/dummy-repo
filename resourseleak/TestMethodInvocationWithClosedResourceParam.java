package resourceleak;

import android.util.Log;
import org.brickred.socialauth.util.Response;

import java.io.InputStream;


public class TestMethodInvocationWithClosedResourceParam {

    SocialAuthListener<Integer> listener;

    protected Integer testMethodInvocationWithClosedResourceParam(Object... params) {
        Response res = null;
        try {
            res = getCurrentProvider().uploadImage((String) params[0], (String) params[1], (InputStream) params[2]);
            Log.d("SocialAuthAdapter", "Image Uploaded");
            return Integer.valueOf(res.getStatus());
        } catch (Exception e) {
            listener.onError(new SocialAuthError("Image Upload Error", e));
            return null;
        }
    }
}
