package concurrency;

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.os.Build;
import android.system.Os;
import com.amazon.foundation.internal.ThreadPoolManager;
import com.amazon.kcp.application.AndroidApplicationController;
import com.amazon.kcp.application.ErrorState;
import com.amazon.kcp.application.ReddingApplication;
import com.amazon.kcp.application.metrics.internal.MetricsManager;
import com.amazon.kcp.debug.DownloadOnDemandDebugUtils;
import com.amazon.kcp.readingstreams.ReadingStreamUtil;
import com.amazon.kcp.util.IOUtils;
import com.amazon.kcp.util.Utils;
import com.amazon.kindle.build.BuildInfo;
import com.amazon.kindle.krl.R;
import com.amazon.kindle.krx.appexpan.*;
import com.amazon.kindle.krx.events.IMessageQueue;
import com.amazon.kindle.krx.events.Subscriber;
import com.amazon.kindle.log.Log;
import com.mobipocket.android.drawing.FontFamily;
import com.mobipocket.android.drawing.LanguageSet;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is responsible for downloading fonts dynamically at runtime using appExpan service.
 * We will get callback on every resourceSet download and it will move the files to the internal
 * font directory if they are font resourceSet. We will also update the KRF font cache and internal
 * font menu items.
 * Created by santhanm on 10/26/16.
 */

public class DynamicFontDownloadHelper {
    private static final String TAG = Utils.getTag(DynamicFontDownloadHelper.class);

    private static final String METRIC_SUCCESS = "Success";
    private static final String METRIC_FAILURE = "Failure";
    private static final String ATTRIBUTE_DYNAMIC_FONT_DOWNLOAD_RESOURCE_NAME = "DynamicFontDownloadResourceName";
    private static final String ATTRIBUTE_DYNAMIC_FONT_DOWNLOAD_RESOURCE_STATE = "DynamicFontDownloadResourceState";
    private static final String EVENT_DYNAMIC_FONT_DOWNLOAD = "DynamicFontDownload";
    private static final String DOWNLOAD_TIMER_SUFFIX = "FontDownloadTimer";

    private IKRXAppExpanClient appExpanClient;

    private static final DynamicFontDownloadHelper INSTANCE = new DynamicFontDownloadHelper();

    //Map that contains whether the font we have is the latest version
    //TRUE - > we have the latest font , no need to refetch
    //FALSE -> we don't have the latest font, fetch it
    private Map<String, Boolean> fontUpdateStatus = new ConcurrentHashMap<>();

    // these fields are for keeping track of download completion state
    private Set<DownloadableFonts> checkedFonts = new HashSet<>();
    private AtomicInteger numDownloadedFonts = new AtomicInteger(0);
    private AtomicInteger numInvalidFonts = new AtomicInteger(0); // number of fonts inapplicable for the platform
    protected final IMessageQueue messageQueue = PubSubMessageService.getInstance().createMessageQueue(DynamicFontDownloadHelper.class);
    // To store the UI status of DOD fonts.
    private Map<FontFamily, OnDemandFontInfo> fontInfoMap = new ConcurrentHashMap<>();

    private DynamicFontDownloadHelper() {
        //initialize all the fonts will not have downloaded status
        for (DownloadableFonts font : DownloadableFonts.values()) {
            fontUpdateStatus.put(font.resourceSetName, false);
        }

        PubSubMessageService.getInstance().subscribe(this);
    }


    public static DynamicFontDownloadHelper getInstance() {
        return INSTANCE;
    }

    /**
     * Retrieve the DynamicFontDownloadHelper's internal target directory for downloads.
     */
    public static File getInternalFontDir() {
        return ReddingApplication.getDefaultApplicationContext().getFilesDir();
    }

    /**
     * Get the legacy location where font used to get downloaded
     * @return
     */
    private static File getLegacyFontDownloadDir(@Nonnull String language) {
        return new File(FontUtils.getFontDir(Utils.getFactory().getFileSystem(), language));
    }

    /**
     * Simple representation of a font that needs to be downloaded through app-expan service
     */
    public enum DownloadableFonts {
        OPEN_DYSLEXIC("OpenDyslexicFontRS", false, LanguageSet.LATIN, false),
        EMBER_BOLD("EmberBoldFontRS", false, LanguageSet.LATIN, false),
        GUJARATI("IndicGUFontRS", true, LanguageSet.GU, true),
        TAMIL("IndicTAFontRS", true, LanguageSet.TA, true),
        MALAYALAM("IndicMLFontRS", true, LanguageSet.ML, true),
        SAKKAL("ArabicSakkalFontRS", !BuildInfo.isFirstPartyBuild(), LanguageSet.ARABIC, true),
        DIWAN("ArabicDiwanFontRS", !BuildInfo.isFirstPartyBuild(), LanguageSet.ARABIC, true),
        DEVANAGARI("IndicDEVANAGARIFontRS", true, LanguageSet.DEVANAGARI, true),
        SECONDARY_JAPANESE("SecondaryJapaneseFontRS", BuildInfo.isFirstPartyBuild(), LanguageSet.JA, false),
        SECONDARY_LATIN("SecondaryLatinFontRS", BuildInfo.isFirstPartyBuild(), LanguageSet.LATIN, false),
        SECONDARY_CHINESE("SecondaryChineseFontRS", BuildInfo.isFirstPartyBuild(), LanguageSet.CN, false),
        STSONGTC("STSongTCRS", true, LanguageSet.TCN, false, true, FontFamily.STSONGTC, R.drawable.ic_font_stsongtc, "23.5MB", true),
        STHEITITC("STHeitiTCRS", true, LanguageSet.TCN, true, BuildInfo.isFirstPartyBuild(), FontFamily.STHEITITC, R.drawable.ic_font_stheititc, "17.2MB", false),
        STKAITITC("STKaitiTCRS", true, LanguageSet.TCN, false, true, FontFamily.STKAITITC, R.drawable.ic_font_stkaititc, "31MB", true),
        STYUANTC("STYuanTCRS", true, LanguageSet.TCN, false, true, FontFamily.STYUANTC, R.drawable.ic_font_styuantc, "21.8MB", true);

        //Name of the font resourceSet
        private String resourceSetName;

        //Whether the font will be downloaded only on demand
        private boolean onDemandDownload;

        //The languages thats been supported by this font
        private LanguageSet languageSet;

        //whether this font should be treated as the default font for the languageSet;
        private boolean defaultFont;

        //Whether this font will be downloaded by manually trigger
        private boolean supportDownloadManually;

        private FontFamily fontFamily;

        // The resource id of the font icon in Aa menu.
        private int previewDrawbale;

        private String resourceSize;

        private boolean supportDeletion;

        DownloadableFonts(String resourceSetName, boolean onDemandDownload, LanguageSet languageSet,
                          boolean defaultFont) {
            this(resourceSetName, onDemandDownload, languageSet, defaultFont, false, null, 0, "", false);
        }

        DownloadableFonts(String resourceSetName, boolean onDemandDownload, LanguageSet languageSet,
                          boolean defaultFont, boolean shouldDownloadManually, FontFamily fontFamily, int previewDrawbale, String resourceSize, boolean supportDeletion) {
            this.resourceSetName = resourceSetName;
            this.onDemandDownload = onDemandDownload;
            this.languageSet = languageSet;
            this.defaultFont = defaultFont;
            this.supportDownloadManually = shouldDownloadManually;
            this.fontFamily = fontFamily;
            this.previewDrawbale = previewDrawbale;
            this.resourceSize = resourceSize;
            this.supportDeletion = supportDeletion;
        }

        //Helper method which tells whether a given resourceSetName belongs to Fonts
        @CheckForNull
        static DownloadableFonts getFontResourceSet(@Nonnull String resourceSetName) {
            for (DownloadableFonts f : DownloadableFonts.values()) {
                if (f.resourceSetName.equals(resourceSetName)) {
                    return f;
                }
            }
            return null;
        }

        //Helper method which gives all OnDemand fonts for a given language
        @Nonnull
        static List<DownloadableFonts> getOnDemandFonts(@Nonnull String language) {
            List<DownloadableFonts> onDemandFonts = new ArrayList<>();
            for (DownloadableFonts f : DownloadableFonts.values()) {
                if (f.languageSet.contains(language) && f.onDemandDownload && !f.supportDownloadManually) {
                    if (DynamicFontDownloadHelper.getInstance().supportsFontsDownloadManually(f.languageSet) && !DownloadOnDemandDebugUtils.isDownloadOnDemandEnabled()) {
                        continue;
                    }
                    onDemandFonts.add(f);
                }
            }
            return onDemandFonts;
        }

        //Helper method which tells whether there exist any onDemand font for a given language
        public static boolean doesOnDemandFontExist(@Nonnull String language) {
            return !getOnDemandFonts(language).isEmpty();
        }

        public int getPreviewDrawbale() {
            return previewDrawbale;
        }

        public LanguageSet getLanguageSet() {
            return languageSet;
        }

        public String getResourceSize() {
            return resourceSize;
        }

        public FontFamily getFontFamily(){
            return fontFamily;
        }

        public boolean isSupportDeletion() {
            return supportDeletion;
        }
    }

    /**
     * This method will get all default fonts if already downloaded and move it to font location. It makes call to
     * AppExpanClient for font resources and copy the downloaded fonts to the internal fonts directory
     * Since this involves lots of file operation, its caller responsible to call it in BG thread
     * Note: This wont download the font, just move it
     */
    public void getFontsAndMoveIfNeeded() {
        boolean fontDownloadedAndMoved = false;

        for (DownloadableFonts font : DownloadableFonts.values()) {
            if (!font.onDemandDownload && getFonts(font, null)) {
                fontDownloadedAndMoved = true;
            }
        }

        if (fontDownloadedAndMoved) {
            // Notify the font config initializer of the new fonts downloaded so that
            // the cache can be updated appropriately.
            Utils.getFactory().getFontConfigInitializer().onFontDownload();
            //Need to update the view options list as well
            Utils.getFactory().getFontFactory().populateSupportedFonts();
        }
    }

    /**Note: made it private as we dont need it now. make it public when you need it
     *
     * Check for the given language whether onDemand font has already been downloaded. This will only
     * check the current cache for the state. If you want to check with the DB then do forceCheck as true
     * Note: forceCheck involve DB query and also start the font download if its not already started
     * @param language, the language for which we need to check the fonts
     * @return true if the font belong to the language need to be downloaded, false if they are already downloaded
     */
    private boolean doesOnDemandFontDownloadNeeded(@Nonnull String language, boolean forceCheck) {
        final List<DownloadableFonts> onDemandFonts = DownloadableFonts.getOnDemandFonts(language);

        for (DownloadableFonts font : onDemandFonts) {
            if (forceCheck) {
                getFonts(font, language);
            }

            if (!fontUpdateStatus.get(font.resourceSetName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check whether exists any onDemand font for the given language and try to download the font and
     * move it to internal app location if not already done. The caller can call this method on any
     * thread
     * @param language
     */
    public void downloadOnDemandFontsIfNeeded(@Nonnull final String language) {
        final List<DownloadableFonts> onDemandFonts = DownloadableFonts.getOnDemandFonts(language);

        if (onDemandFonts.isEmpty()) {
            return;
        }

        ThreadPoolManager.getInstance().submit(new Runnable() {
            @Override
            public void run() {
                for (DownloadableFonts font : onDemandFonts) {
                    getFonts(font, language);
                }
            }
        });

        // When this book supports DOD feature but the font need to download when open book, we will
        // update the UI status in advance.
        if (supportsFontsDownloadManually(LanguageSet.getSet(language))) {
            for (DownloadableFonts font : onDemandFonts) {
                OnDemandFontInfo fontInfo = fontInfoMap.get(font.fontFamily);
                if (fontInfo == null) {
                    fontInfo = new OnDemandFontInfo(font);
                }

                if (!fontInfo.isDownloaded()) {
                    if (!fontInfo.isDownloading()) {
                        MetricsManager.getInstance().startMetricTimer(getFontDownloadTimer(fontInfo));
                        fontInfo.setDownloading();
                    }
                }
                fontInfoMap.put(font.fontFamily, fontInfo);
            }
        }
    }

    private IKRXAppExpanClient getAppExpanClient() {
        if (appExpanClient == null) {
            appExpanClient = Utils.getFactory().getKindleReaderSDK().getReaderManager().getAppExpanClient();
        }
        return appExpanClient;
    }

    /**
     * Given a resourceSetName, get the resources and copy it to the internal font location
     * @param font, the font that needs to be downloaded and moved
     * @param language, the language comes as part of download request
     */
    private boolean getFonts(@Nonnull DownloadableFonts font, @CheckForNull String language) {
        boolean success = false;

        //If language is null then lets take the first language from languageSet
        if (language == null) {
            Iterator<String> it = font.languageSet.getLanguages().iterator();
            language = it.next();
        }

        //Its possible that getFonts might be called from 2 different threads, lets lock it before
        //we access
        synchronized (font) {
            String resourceSetName = font.resourceSetName;
            Log.info(TAG, "Acquired lock for " + resourceSetName);

            //Check whether we have any font update. Some fonts like INDIC will ask for fonts on every
            //book open, we dont need to do resourceSet call each time
            if (!fontUpdateStatus.get(resourceSetName)) {
                boolean moveFailed = false;

                //Get the resourceSet information
                IAppExpanResourceSetResponse resourceSetResponse = getAppExpanClient().getResourceSet(resourceSetName);
                IAppExpanResourceSet resourceSet = resourceSetResponse.getResourceSet();

                if (resourceSet != null) {
                    int currentVersion = getAppExpanClient().retrieveCurrentUsedVersion(this.getClass(), resourceSet.getName());
                    if (resourceSet.getVersion() > currentVersion) {
                        Log.info(TAG, "We got a new resourceSet " + resourceSetName + " version " + resourceSet.getVersion());
                        Set<String> resources = resourceSet.getResourceNames();

                        for (String s : resources) {
                            IAppExpanResource resource = resourceSet.getResourceByName(s);
                            if (resource != null) {
                                if (supportsFontsDownloadManually(font.languageSet)) {
                                    if (!createSymlinkAtInternalFileSystem(resource.getFile())) {
                                        moveFailed = true;
                                    }
                                } else if(!moveFontsToInternalFileSystem(resource.getFile(), resource.getName(), language)) {
                                    moveFailed = true;
                                }
                            } else {
                                moveFailed = true;
                            }
                        }

                        //Update the font status only if the font copy succeeded.
                        if (!moveFailed) {
                            getAppExpanClient().saveCurrentUsedVersion(this.getClass(), resourceSet.getName(), resourceSet.getVersion());
                            fontUpdateStatus.put(resourceSetName, true);
                            Log.info(TAG, "Move succeeded for " + resourceSetName + "version " + resourceSet.getVersion());
                            success = true;
                        } else {
                            Log.error(TAG, "Move failed for " + resourceSetName + "version " + resourceSet.getVersion());
                            // If we failed in copy the dod font to internal, we need to send fail event.
                            if (supportsFontsDownloadManually(font.languageSet)) {
                                handleDownloadFailure(font, DownloadOnDemandFontEvent.EventType.FONT_DOWNLOAD_FAIL,
                                        DownloadOnDemandFontEvent.ErrorType.FAIL_GENERAL_ERROR, true);
                            }
                        }
                    } else {
                        //No new version lets change the status as well
                        fontUpdateStatus.put(resourceSetName, true);
                    }
                    numDownloadedFonts.incrementAndGet();
                } else if (resourceSetResponse != null
                        && IAppExpanResourceSetResponse.FailureReason.RESOURCE_SET_INVALID.equals(resourceSetResponse.getFailureReason())) {
                    // RESOURCE_SET_INVALID indicates that the downloaded font is not valid for the current platform (for instance, we do not
                    // get OPEN_DYSLEXIC on FOS through this class).
                    numInvalidFonts.incrementAndGet();
                    if (supportsFontsDownloadManually(font.languageSet)) {
                        handleDownloadFailure(font, DownloadOnDemandFontEvent.EventType.FONT_DOWNLOAD_FAIL,
                                DownloadOnDemandFontEvent.ErrorType.FAIL_GENERAL_ERROR, true);
                    }
                }

                synchronized (checkedFonts) {
                    checkedFonts.add(font);
                }
            }
            Log.info(TAG, "Releasing lock for " + resourceSetName);
        }
        return success;
    }

    public boolean areDownloadsCompleted() {
        synchronized (checkedFonts) {
            // downloads are complete if we've checked the status of all downloadable fonts and all valid fonts are downloaded.
            return checkedFonts.size() == DownloadableFonts.values().length
                    && numDownloadedFonts.get() == checkedFonts.size() - numInvalidFonts.get();
        }
    }

    private void reportAnalytics(@Nonnull String resourceName, @Nonnull String state) {
        Map<String, String> eventAttributes = new HashMap<>();
        eventAttributes.put(ATTRIBUTE_DYNAMIC_FONT_DOWNLOAD_RESOURCE_NAME, resourceName);
        eventAttributes.put(ATTRIBUTE_DYNAMIC_FONT_DOWNLOAD_RESOURCE_STATE, state);
        MetricsManager.getInstance().reportMetric(TAG, EVENT_DYNAMIC_FONT_DOWNLOAD, eventAttributes);
    }

    /**
     * Copy the file to the internal font directory
     * This method will return true if copy to internal file system succeeds
     * @param f, file to copy
     * @return true if file is available in internal directory, false otherwise
     */
    private boolean copyFontToInternalFileSystem(@Nonnull File f) {
        FileInputStream inputStream = null;
        FileOutputStream outputStream = null;
        try {

            inputStream = new FileInputStream(f);
            outputStream = ReddingApplication.getDefaultApplicationContext().openFileOutput(f.getName(), Context.MODE_PRIVATE);

            //Copy the content to the internal directory
            IOUtils.writeInToOut(inputStream, outputStream);

            //TODO: When we started supporting version upgrades for fonts then we need to handle copying the
            //file and deleting the existing font file

            File outputFile = new File(getInternalFontDir(), f.getName());

            if (!outputFile.exists()) {
                Log.error(TAG, "Copied file " + f.getName() + "doesnt exist");
                return false;
            }
        } catch (Exception e) {
            Log.error(TAG, "Exception in copyAndDeleteFontFiles", e);
            return false;
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
                if (outputStream != null) {
                    outputStream.close();
                }
            } catch (Exception e) {
                Log.error(TAG, "Error in closing streams", e);
            }
        }
        return true;
    }

    /**
     * Helper method which will create a symbolic link between font fils in AES to internal font location.
     *
     * @param directory the directory of resource to scan
     * @return true if create success, false otherwise.
     */
    private boolean createSymlinkAtInternalFileSystem(@Nonnull File directory) {
        if (!directory.exists() || !directory.isDirectory()) {
            Log.error(TAG, " The file " + directory.getName() + " is not a directory or doesnt exist");
            return false;
        }

        File[] files = directory.listFiles();

        if (files != null) {
            for (File f : files) {
                if (!createSymlinkForFontFile(f)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Helper method which will copy all the files in the input directory to internal font location
     * @param directory, the directory to scan
     * @param, resourceName, the name of the resource for which we are moving the files
     * @return, true if copy succeeded, false otherwise
     */
    private boolean moveFontsToInternalFileSystem(@Nonnull File directory, @Nonnull String resourceName, @Nonnull String language) {
        boolean hasError = false;
        if (!directory.exists() || !directory.isDirectory()) {
            Log.error(TAG, " The file " + directory.getName() + " is not a directory or doesnt exist");
            return false;
        }

        File[] files = directory.listFiles();

        boolean copyFonts = false;

        if (files != null) {
            for (File f : files) {
                //Currently we can just return if font file already exist.When we get a newer version of fonts
                //then we need to remove this If and support replacing the existing file. However font upgrade is a
                //rare scenario.
                if (doesFontFileExist(f.getName(), language)) {
                    //File already exist lets delete the app expan file
                    f.delete();
                    continue;
                }

                //Even if one file doesnt exist already then we need to copy from downloaded path
                copyFonts = true;

                if (copyFontToInternalFileSystem(f)) {
                    //Lets delete the font file from app expan directory
                    f.delete();
                } else {
                    //Continue with other files even if 1 file is in error. We wont return true if
                    //in error anyway so that next time we will try copying again. But till that
                    //moved font files can be used.
                    hasError = true;
                }
            }
        }

        //If even for 1 file we copied the file from downloaded path then we have to report metric
        if (copyFonts) {
            //If we use downloaded path and even if 1 f file is in error then report FAILURE
            if (hasError) {
                reportAnalytics(resourceName, METRIC_FAILURE);

            } else {
                reportAnalytics(resourceName, METRIC_SUCCESS);
            }
        }
        return !hasError;
    }

    /**
     * Checks whether the font file exist in either the internal app location or in the legacy font download
     * location
     * @param fileName
     * @param language
     * @return
     */
    private boolean doesFontFileExist(@Nonnull String fileName, @Nonnull String language) {
        File existingFile = new File(getInternalFontDir(), fileName);

        //Check internal app location
        if (existingFile.exists()) {
            Log.debug(TAG, "Font file " + fileName + " already exist in internal dir");
            return true;
        }

        existingFile = new File(getLegacyFontDownloadDir(language), fileName);

        //check legacy font download location
        if (existingFile.exists()) {
            Log.debug(TAG, "Font file " + fileName + " already exist in legacy dir");
            return true;
        }
        return false;
    }

    /**
     * We will get callback on every resourceSet download. Filter it based on fonts
     * @param event
     */
    @Subscriber
    public void onResourceSetDownload(ResourceSetAvailableEvent event) {
        DownloadableFonts downloadableFont = DownloadableFonts.getFontResourceSet(event.getResourceSetName());
        if (downloadableFont != null) {
            Log.info(TAG, "Got ResourceSetAvailableEvent callback for " + event.getResourceSetName());
            synchronized (downloadableFont) {
                fontUpdateStatus.put(event.getResourceSetName(), false);
            }

            if (getFonts(downloadableFont, null)) {
                handleDownloadSuccess(downloadableFont);
            }
        }
    }

    @Subscriber
    public void onResourceSetFailInDownload(ResourceSetDownloadFailedEvent event) {
        DownloadableFonts downloadableFont = DownloadableFonts.getFontResourceSet(event.getResourceSetName());
        if (downloadableFont != null) {
            handleDownloadFailure(downloadableFont, DownloadOnDemandFontEvent.EventType.FONT_DOWNLOAD_FAIL,
                    DownloadOnDemandFontEvent.ErrorType.FAIL_GENERAL_ERROR, true);
        }
    }

    //Set the font as default font for all the languages it belongs to
    private void setDefaultFont(@Nonnull DownloadableFonts font) {
        Log.info(TAG, "Setting default font for " + font.name());
        Set<String> languages = font.languageSet.getLanguages();

        for (String language : languages) {
            FontFamily fontFamily = FontFamily.getDefaultFont(language);
            Utils.getFactory().getUserSettingsController().setFontFamily(fontFamily, language);
        }
    }

    /**
     * Determine the input language support Dynamic on Demand or not
     * @param languageSet
     * @return
     */
    public boolean supportsFontsDownloadManually(LanguageSet languageSet) {
        for (DownloadableFonts font : DownloadableFonts.values()) {
            if (font.supportDownloadManually && font.languageSet == languageSet) {
                return true;
            }
        }
        return false;
    }

    /**
     * Dynamic download fonts when triggered by customers.
     * @param fontInfo
     */
    public void downloadFontManuallyIfNeeds(@Nonnull final OnDemandFontInfo fontInfo, final String language) {
        if (fontInfo.isDownloading()) {
            return;
        }

        ConnectivityManager connectivityManager = (ConnectivityManager) Utils.getFactory().getContext().getSystemService(Context.CONNECTIVITY_SERVICE);
        NetworkInfo networkInfo = connectivityManager.getActiveNetworkInfo();
        boolean isConnected = (networkInfo != null && networkInfo.isConnected());

        if (isConnected) {
            ThreadPoolManager.getInstance().submit(new Runnable() {
                @Override
                public void run() {
                    MetricsManager.getInstance().startMetricTimer(getFontDownloadTimer(fontInfo));
                    if (getFonts(fontInfo.downloadableFont, language)) {
                        handleDownloadSuccess(fontInfo.downloadableFont);
                    }
                }
            });
            fontInfo.setDownloading();
            fontInfoMap.put(fontInfo.downloadableFont.fontFamily, fontInfo);
        } else {
            handleDownloadFailure(fontInfo.downloadableFont, DownloadOnDemandFontEvent.EventType.FONT_DOWNLOAD_FAIL,
                    DownloadOnDemandFontEvent.ErrorType.FAIL_NETWORK, false);
        }
    }

    /**
     * Delete fonts which download on demand, also will reset the value in the records.
     *
     * @param fontInfo
     */
    public void deleteFontOnDemand(@Nonnull final OnDemandFontInfo fontInfo) {
        if (fontInfo.isDownloaded()) {
            getAppExpanClient().releaseResourceSet(fontInfo.downloadableFont.resourceSetName);
            fontInfo.downloadStatus = DownloadStatus.NEED_DOWNLOAD;
            Utils.getFactory().getFontConfigInitializer().onFontDelete();
            getAppExpanClient().saveCurrentUsedVersion(this.getClass(), fontInfo.downloadableFont.resourceSetName, -1);

            fontUpdateStatus.put(fontInfo.downloadableFont.resourceSetName, false);
            this.numDownloadedFonts.decrementAndGet();
            synchronized (checkedFonts) {
                checkedFonts.remove(fontInfo.downloadableFont);
            }

            //If this font is be chosen as rendering font, we need to fallback to Default Font.
            for (String lang : fontInfo.downloadableFont.languageSet.getLanguages()) {
                FontFamily fontFamily = Utils.getFactory().getUserSettingsController().getFontFamily(lang);
                if (fontFamily.equals(fontInfo.getFontFamily())) {
                    Utils.getFactory().getUserSettingsController().setFontFamily(FontFamily.getDefaultFont(lang), lang);
                }
            }

            reportFontDownloadOnDemandMetrics(fontInfo, ReadingStreamUtil.ACTION_DOWNLOAD_ON_DEMAND_CLICK_DELETE_BUTTON);
        }
    }

    /**
     * An object to record the info of on demand downloaded fonts, including the downloading status & fonts.
     */
    public class OnDemandFontInfo implements IFontInfo {
        private DownloadStatus downloadStatus;
        private DownloadableFonts downloadableFont;

        public OnDemandFontInfo(DownloadableFonts downloadableFont) {
            this.downloadableFont = downloadableFont;
            if (Utils.getFactory().getFontConfigInitializer().validateFont(this.downloadableFont.fontFamily)) {
                this.downloadStatus = DownloadStatus.DOWNLOADED;
            } else {
                this.downloadStatus = DownloadStatus.NEED_DOWNLOAD;
            }
        }

        @Override
        public boolean isDownloaded() {
            return downloadStatus == DownloadStatus.DOWNLOADED;
        }

        @Override
        public boolean isDownloading() {
            return downloadStatus == DownloadStatus.DOWNLOADING;
        }

        @Override
        public String getResourceSizeString() {
            return downloadableFont.resourceSize;
        }

        @Override
        public int getDownloadStatusStringId() {
            return 0;
        }

        public DownloadableFonts getDownloadableFont() {
            return this.downloadableFont;
        }

        public void setDownloading() {
            downloadStatus = DownloadStatus.DOWNLOADING;
        }

        public void setDownloaded() {
            downloadStatus = DownloadStatus.DOWNLOADED;
        }

        @Override
        public FontFamily getFontFamily() {
            return this.downloadableFont.fontFamily;
        }

        @Override
        public boolean isDefaultFont() {
            return this.downloadableFont.defaultFont;
        }
    }

    /**
     * Get font info with font family.
     * @param font
     * @return
     */
    @CheckForNull
    public OnDemandFontInfo getFontInfo(FontFamily font) {
        if (font == null) {
            return null;
        }

        OnDemandFontInfo fontInfo = fontInfoMap.get(font);
        if (fontInfo == null) {
            DownloadableFonts downloadableFonts = getDownloadableFonts(font);
            if (downloadableFonts != null) {
                fontInfo = new OnDemandFontInfo(downloadableFonts);
                fontInfoMap.put(font, fontInfo);
            }
        }
        return fontInfo;
    }

    @CheckForNull
    public DownloadableFonts getDownloadableFonts(FontFamily fontFamily) {
        for (DownloadableFonts downloadableFonts : DownloadableFonts.values()) {
            if (downloadableFonts.fontFamily == fontFamily) {
               return downloadableFonts;
            }
        }
        return null;
    }

    private void handleDownloadSuccess(DownloadableFonts downloadableFont) {
        // Notify the font config initializer of the new fonts downloaded so that
        // the cache can be updated appropriately.
        Utils.getFactory().getFontConfigInitializer().onFontDownload();
        //Need to update the view options list as well
        Utils.getFactory().getFontFactory().populateSupportedFonts();

        //For DoD font, we need to update the status and notify UI to refresh view.
        OnDemandFontInfo fontInfo = fontInfoMap.get(downloadableFont.fontFamily);
        if (fontInfo != null) {
            reportFontDownloadOnDemandMetrics(fontInfo, ReadingStreamUtil.EVENT_DOWNLOAD_ON_DEMAND_SUCCESS);
            fontInfo.setDownloaded();
            AESFontNotification notification = new AESFontNotification(TAG, Utils.getFactory().getContext(), downloadableFont.fontFamily.getDisplayName());
            notification.popFontNotification(AESFontNotification.SUCCESS);
        }
        if (supportsFontsDownloadManually(downloadableFont.fontFamily.getLanguages())) {
            this.messageQueue.publish(new DownloadOnDemandFontEvent(downloadableFont,
                    DownloadOnDemandFontEvent.EventType.FONT_DOWNLOAD_SUCCESS, DownloadOnDemandFontEvent.ErrorType.NONE));
        }
        if (downloadableFont.defaultFont) {
            setDefaultFont(downloadableFont);
        }
    }

    /**
     * Handling After the download font on demand fails
     * @param downloadableFont
     * @param eventType fail/success
     * @param errorType the error type for event, success' error type is NONE.
     * @param doReportMetric true   we will report this fail event to PMET/ReadingStream
     *                       false  we won't report this event, such as no network when customers click the button.
     */
    private void handleDownloadFailure(DownloadableFonts downloadableFont, DownloadOnDemandFontEvent.EventType eventType,
                                       DownloadOnDemandFontEvent.ErrorType errorType, boolean doReportMetric) {
        OnDemandFontInfo fontInfo = fontInfoMap.get(downloadableFont.fontFamily);
        if (fontInfo != null) {
            if (doReportMetric) {
                reportFontDownloadOnDemandMetrics(fontInfo, ReadingStreamUtil.EVENT_DOWNLOAD_ON_DEMAND_FAIL);
            }

            fontInfo.downloadStatus = DownloadStatus.NEED_DOWNLOAD;
            this.messageQueue.publish(new DownloadOnDemandFontEvent(downloadableFont, eventType, errorType));
            switch (errorType) {
                case FAIL_GENERAL_ERROR:
                    AndroidApplicationController.getInstance().showAlert(ErrorState.FONT_DOWNLOAD_ERROR, null);
                    break;
                case FAIL_NETWORK:
                    AndroidApplicationController.getInstance().showAlert(ErrorState.CONNECTION_ERROR, null);
                    break;
                default:
                    break;
            }
        }
    }

    private void reportFontDownloadOnDemandMetrics(@Nonnull OnDemandFontInfo fontInfo, @Nonnull String event) {
        String fontTypefaceName = fontInfo.downloadableFont.fontFamily.getTypeFaceName();
        String fontTimer = getFontDownloadTimer(fontInfo);
        switch (event) {
            case ReadingStreamUtil.EVENT_DOWNLOAD_ON_DEMAND_SUCCESS:
                MetricsManager.getInstance().stopMetricTimerIfExists(ReadingStreamUtil.CONTEXT_DOWNLOAD_ON_DEMAND,
                        fontTimer, fontTimer);
                break;
            case ReadingStreamUtil.EVENT_DOWNLOAD_ON_DEMAND_FAIL:
                MetricsManager.getInstance().cancelMetricTimer(fontTimer);
                break;
            case ReadingStreamUtil.ACTION_DOWNLOAD_ON_DEMAND_CLICK_CANCEL_BUTTON:
                MetricsManager.getInstance().cancelMetricTimer(fontTimer);
                break;
            case ReadingStreamUtil.ACTION_DOWNLOAD_ON_DEMAND_CLICK_DELETE_BUTTON:
                break;
            default:
                return;
        }

        MetricsManager.getInstance().reportMetric(ReadingStreamUtil.CONTEXT_DOWNLOAD_ON_DEMAND,
                event + fontTypefaceName);
        Utils.getFactory().getKindleReaderSDK().getReadingStreamsEncoder().performAction(
                ReadingStreamUtil.CONTEXT_DOWNLOAD_ON_DEMAND,
                event + fontTypefaceName);
    }

    private String getFontDownloadTimer(@Nonnull OnDemandFontInfo fontInfo) {
        return fontInfo.downloadableFont.fontFamily.getTypeFaceName() + DOWNLOAD_TIMER_SUFFIX;
    }

    public void cancelDownloadFontOnDemand(IFontInfo font) {
        OnDemandFontInfo fontInfo = fontInfoMap.get(font.getFontFamily());
        if (fontInfo != null && fontInfo.isDownloading()) {
            getAppExpanClient().releaseResourceSet(fontInfo.downloadableFont.resourceSetName);
            fontInfo.downloadStatus = DownloadStatus.NEED_DOWNLOAD;
            reportFontDownloadOnDemandMetrics(fontInfo, ReadingStreamUtil.ACTION_DOWNLOAD_ON_DEMAND_CLICK_CANCEL_BUTTON);
        }
    }

    /**
     * Create a symbolic link of the font file to file/, so we don't need to copy files.
     *
     * @param f
     * @return
     */
    private boolean createSymlinkForFontFile(File f) {
        String sourcePath = f.getAbsolutePath();
        String symLinkPath = Utils.getFactory().getContext().getFilesDir().getAbsolutePath() + "/" + f.getName();
        File sourceFile = new File(sourcePath);

        if (!sourceFile.exists()) {
            return false;
        }
        try {
            File symFile = new File(symLinkPath);

            if (symFile.exists()) {
                symFile.delete();
            }

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP) {
                Os.symlink(sourcePath, symLinkPath);
            } else {
                Runtime.getRuntime().exec(String.format("ln -s %s %s", sourcePath, symLinkPath));
            }
            return true;
        } catch (Exception e) {
            Log.debug(TAG, "Fail to create a symbolic link of the font file.");
           return false;
        }
    }
}
