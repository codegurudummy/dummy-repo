package concurrency;

import com.amazon.kcp.application.AndroidApplicationController;
import com.amazon.kcp.application.IKindleObjectFactory;
import com.amazon.kindle.krx.KindleReaderSDK;
import com.amazon.kindle.krx.application.IApplicationManager;
import com.amazon.kindle.krx.content.IBook;
import com.amazon.kindle.krx.download.IKRXResponseHandler.DownloadStatus;
import com.amazon.kindle.krx.events.Subscriber;
import com.amazon.kindle.log.Log;
import com.amazon.kindle.model.content.IBookID;
import com.amazon.kindle.services.download.IDownloadService;
import com.amazon.kindle.services.download.IDownloadService.DownloadProgressUpdateEvent;
import com.amazon.kindle.util.BookIdUtils;
import com.amazon.kindle.webservices.IWebRequest;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * implementation of {@link IKRXDownloadManager} interface: this proxies the
 * requests through readers {@link IAssetStateManager} implementation.
 *
 * @author donghao
 *
 */
public class KRXDownloadManager implements IKRXDownloadManager {
    private static final String TAG = Log.getTag(KRXDownloadManager.class);

    private IKindleObjectFactory factory;

    private IApplicationManager applicationManager;

    private IDownloadService downloadService;

    private Map<String, IDownloadStatusListener> progressListenerMap = new ConcurrentHashMap();

    public KRXDownloadManager(IKindleObjectFactory factory,
                              IApplicationManager applicationManager,
                              IDownloadService downloadService) {
        this.factory = factory;
        this.applicationManager = applicationManager;
        this.downloadService = downloadService;
        PubSubMessageService.getInstance().subscribe(this);
    }

    @Override
    @Deprecated
    public void signRequest(IKRXDownloadRequest request) {
        Log.warn(TAG,
                "This is no longer necessary, request will be signed automatically");
    }

    @Override
    public String enqueueDownloadRequest(IKRXDownloadRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("null request");
        }
        if (request.getBookId() != null) {
            validateBook(request.getBookId());
        }
        KrxDownloadRequest downloadRequest;
        //until a few plugins that extend directly IKRXDownloadRequest
        //migrate to a base implementation, we need to use a specific ctor
        //in order to support additional capabilities, once they remove it
        //we should be able to remove this as a whole and move the implementation directly down to IKRXDownloadRequest.
        if (request instanceof IExtendedKRXDownloadRequest) {
            downloadRequest = new KrxDownloadRequest(this.factory.getFileSystem(), (IExtendedKRXDownloadRequest) request);
        } else {
            downloadRequest = new KrxDownloadRequest(this.factory.getFileSystem(), request);
        }
        addDownloadRequest(downloadRequest);
        return downloadRequest.getId();
    }

    @Override
    public DownloadStatus getStatus(String bookId, String requestId) {
        return DownloadStatus.UNKNOWN;
    }

    private boolean validateBook(String bookId) {
        IBookID id = BookIdUtils.parse(bookId);
        if (id == null) {
            throw new IllegalArgumentException("invalid book id");
        }
        IBook book = KindleReaderSDK.getInstance().getLibraryManager()
                .getContent(bookId);
        // if book does not exist, return false.
        if (book == null) {
            Log.warn(TAG, "The book is not recognized as a local book");
            return false;
        }
        return true;
    }

    @Override
    public String downloadSidecar(String bookId, String url,
                                  String sidecarName, boolean isShared, IKRXResponseHandler handler) {
        return downloadSidecar(bookId, url, sidecarName, isShared, handler, null);
    }

    @Override
    public String downloadSidecar(String bookId, String url,
                                  String sidecarName, boolean isShared, IKRXResponseHandler handler,
                                  Map<String, String> headers) {
        // we won't download any sidecar if there is no book for the book id.
        if (!validateBook(bookId)) {
            return "";
        }
        final IBookID bookIdObj = BookIdUtils.parse(bookId);
        if (bookIdObj == null) {
            return "";
        }

        String downloadPath = this.applicationManager
                .getContentSidecarDirectory(bookId, isShared)
                + "/"
                + sidecarName;
        final KrxDownloadRequest downloadRequest = new KrxDownloadRequest(
                this.factory.getFileSystem(), url, downloadPath, IKRXDownloadRequest.HTTP_GET,
                null, headers, handler, IKRXDownloadRequest.DEFAULT_TIMEOUT, bookId);
        downloadRequest.setPriority(IWebRequest.RequestPriority.HIGH);

        // TODO: get the GUID from plugin teams.  Tracking in KFA-6952
        if (!this.factory.getSidecarDownloadService().download(bookIdObj, downloadRequest, null)) {
            return "";
        }

        return downloadRequest.getId();
    }

    private void addDownloadRequest(final KrxDownloadRequest request) {
        request.setPriority(IWebRequest.RequestPriority.HIGH);
        this.factory.getWebRequestManager().addWebRequest(request);
    }

    private static URI getUri(String url) {
        try {
            return new URI(url);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Invalid url:", ex);
        }
    }

    public static String getHttpRequestPathAndQuery(String url) {
        String queryPath = null;
        URI uri = getUri(url);
        queryPath = uri.getRawPath();
        String rawQuery = uri.getRawQuery();
        if (rawQuery != null) {
            if (queryPath != null) {
                queryPath += "?" + rawQuery;
            } else {
                queryPath = "?" + rawQuery;
            }
        }
        return queryPath;
    }

    @Override
    public boolean downloadBook(String bookId, IDownloadStatusListener listener) {
        if (listener != null) {
            registerDownloadProgressListener(bookId, listener);
        }
        factory.getLibraryController().downloadBook(bookId);
        return true;
    }

    @Override
    // Note that this method is around for legacy reasons.  Standalone which was on
    // KRX 1.1 recently migrated to KRX 2.0 (as of April '14 - SA 4.5 release) and hence
    // the dependency on this method.  Once the partner team (Audible is ready to use
    // the above API), this method would be deprecated.
    public boolean downloadBook(IBook book) {
        return AndroidApplicationController.getInstance().library().downloadBook(book.getBookId());
    }

    @Override
    public void registerDownloadProgressListener(final String bookId,
                                                 final IDownloadStatusListener listener) {
        if (bookId != null && listener != null) {
            this.progressListenerMap.put(bookId, listener);
        }
    }

    @Subscriber
    public void onDownloadProgressEvent(DownloadProgressUpdateEvent event) {
        String bookId = event.getDownload().getBookId();
        if (bookId != null) {
            IDownloadStatusListener listener = this.progressListenerMap.get(bookId);
            if (listener != null) {
                int progressPercent = event.getDownload().getPercentage();
                listener.onProgressChange(progressPercent);
                if (progressPercent >= 99) {
                    progressListenerMap.remove(bookId);
                }
            }
        }

    }

    @Subscriber
    public void onDownloadStateUpdateEvent(@Nonnull IDownloadService.DownloadStateUpdateEvent event) {
        String bookId = event.getDownload().getBookId();
        if (bookId != null) {
            IDownloadStatusListener listener = this.progressListenerMap.get(bookId);
            if (listener != null) {
                switch (event.getDownloadState()) {
                    case LOCAL:
                        listener.onSuccess();
                        progressListenerMap.remove(bookId);
                        break;
                    //TODO: Do we want to handle FAILED_RETRYABLE and FAILED_OPENABLE?
                    case FAILED:
                        listener.onFailure();
                        progressListenerMap.remove(bookId);
                        break;
                    default:
                }
            }
        }
    }

    @Override
    public void cancelDownload(String bookId) {
        factory.getLibraryController().cancelDownload(bookId);
    }
}