package concurrency;

import com.amazon.android.docviewer.KindleDocViewer;
import com.amazon.android.docviewer.mobi.IPageProvider.PagePosition;
import com.amazon.android.docviewer.mobi.MobiDocViewer;
import com.amazon.android.docviewer.mobi.MobiPageWrapper;
import com.amazon.kcp.reader.utterance.MarkedUtterance;
import com.amazon.kcp.reader.utterance.SentenceBreakIteratorUtteranceGetter;
import com.amazon.kcp.reader.utterance.SpeechBreakersUtteranceGetter;
import com.amazon.kcp.reader.utterance.UtteranceGetter;
import com.amazon.kindle.log.Log;
import com.amazon.kindle.search.IKindleWordTokenIterator;
import com.amazon.kindle.speech.breaker.ISpeechBreakerList;

import java.text.BreakIterator;
import java.util.*;

/**
 * This class encapsulates the buffer that the Tts engine will speak. It keeps
 * track of what is being read, and tosses the sentence that was just uttered.
 */
public class TtsEngineSpeechBuffer {

    private static final String TAG = TtsEngineSpeechBuffer.class.getName();

    public static final String SPEAK_OPEN_TAG = "<speak>";
    public static final String SPEAK_CLOSE_TAG = "</speak>";
    public static final String CDATA_OPEN_TAG = "<![CDATA[";
    public static final String CDATA_CLOSE_TAG = "]]>";
    public static final String SSML_PARAGRAPH_OPEN = "<p>";
    public static final String SSML_PARAGRAPH_CLOSE = "</p>";
    public static final String SSML_BREAK = "<break/>";
    public static final char NON_BREAKING_SPACE = '\u00A0';

    public static final int NEXT_PAGE_INVALID = -1;

    private BreakIterator wordBreakIterator;
    private BreakIterator sentenceBreakIterator;

    private TtsEngineDriver ttsEngineDriver;
    private boolean pageFlipped;
    private int pageStartPosition;
    private int pageEndPosition;
    private Map<Integer, MarkedUtterance> sentences;
    private int progress = -1;

    private String[] abbreviations;

    private IKindleWordTokenIterator wordTokenIterator;
    private ISpeechBreakerList speechBreakers = null;
    private boolean speechBreakersAvailable;

    private UtteranceGetter utteranceGetter;

    public TtsEngineSpeechBuffer(TtsEngineDriver ttsEngineDriver) {
        Log.debug(TAG, "<init>()");
        this.ttsEngineDriver = ttsEngineDriver;
        this.sentences = new TreeMap<Integer, MarkedUtterance>();
    }

    private void refreshSpeechBreakersAvailability() {
        speechBreakersAvailable = (wordTokenIterator != null && speechBreakers != null && speechBreakers.getCount() > 0);
    }

    public synchronized void setWordTokenIterator(IKindleWordTokenIterator wordTokenIterator) {
        if (this.wordTokenIterator != null) {
            this.wordTokenIterator.close();
        }
        this.wordTokenIterator = wordTokenIterator;
        refreshSpeechBreakersAvailability();
    }

    public void setSpeechBreakers(ISpeechBreakerList speechBreakers) {
        this.speechBreakers = speechBreakers;
        refreshSpeechBreakersAvailability();
    }

    public List<MarkedUtterance> readText(int startPosition, int endPosition, int currentUtteranceId) {
        List<MarkedUtterance> utterancesParsed = new LinkedList<MarkedUtterance>();
        if (sentences.size() == 0 || startPosition != pageStartPosition || endPosition != pageEndPosition) {
            sentences.clear();
            this.pageStartPosition = startPosition;
            this.pageEndPosition = endPosition;

            Log.debug(TAG, "start: " + startPosition + ", end: " + endPosition);
            if (startPosition == endPosition)
                return null;

            int nextEndPosition = NEXT_PAGE_INVALID;
            KindleDocViewer kindleDocViewer = ttsEngineDriver.getKindleDocViewer();
            if (kindleDocViewer instanceof MobiDocViewer) {
                MobiDocViewer mobiDocViewer = (MobiDocViewer) kindleDocViewer;
                nextEndPosition = MobiPageWrapper.getLastElementPositionId(mobiDocViewer.getPage(PagePosition.NEXT));
            }

            // use SpeechBreakersUtteranceGetter only if speech breakers are available and they cover the whole page
            synchronized (this) {
                if (speechBreakersAvailable
                        && speechBreakers.getPositionForItemAt(
                        speechBreakers.getCount() - 1) > endPosition) {
                    utteranceGetter = new SpeechBreakersUtteranceGetter(wordTokenIterator,
                            speechBreakers, startPosition, endPosition, nextEndPosition);
                } else {
                    utteranceGetter = new SentenceBreakIteratorUtteranceGetter(
                            wordTokenIterator, sentenceBreakIterator, startPosition, endPosition, abbreviations);
                }
            }

            utteranceGetter.setProgress(progress);
            List<MarkedUtterance> utterances = utteranceGetter.getUtterances();
            progress = utteranceGetter.getProgress();
            if (utterances != null && !utterances.isEmpty()) {
                synchronized (this) {
                    for (MarkedUtterance utterance : utterances) {
                        sentences.put(utterance.getId(), utterance);
                    }
                }
            }
        }
        synchronized (this) {
            // We are on the same page.
            if (sentences.size() > 0) {
                for (MarkedUtterance utterance : sentences.values()) {
                    if (utterance.getId() >= currentUtteranceId) {
                        utterancesParsed.add(utterance);
                    }
                }
            }
            sentences.clear();
        }

        return utterancesParsed;
    }

    public synchronized void clear() {
        sentences.clear();
        progress = -1;
        if (utteranceGetter != null)
            utteranceGetter.clearProgress();
    }

    public void setSentenceIterator(BreakIterator sentenceIterator) {
        this.sentenceBreakIterator = sentenceIterator;
    }

    public synchronized MarkedUtterance removeUtterance(int utteranceId) {
        return sentences.remove(utteranceId);
    }

    public synchronized MarkedUtterance getUtterance(int utteranceId) {
        return sentences.get(utteranceId);
    }


    public synchronized int getPageStartPosition() {
        return pageStartPosition;
    }

    public synchronized int getPageEndPosition() {
        return pageEndPosition;
    }

    public BreakIterator getWordIterator() {
        return wordBreakIterator;
    }

    public void setWordIterator(BreakIterator wordIterator) {
        this.wordBreakIterator = wordIterator;
    }

    public synchronized void addSentence(MarkedUtterance sentence) {
        sentences.put(sentence.getId(), sentence);
    }

    public synchronized List<MarkedUtterance> getUtteranceListAndClear() {

        List<MarkedUtterance> utteranceList = new ArrayList<MarkedUtterance>();
        utteranceList.addAll(sentences.values());
        sentences.clear();
        return utteranceList;
    }

    public boolean isPageFlipped() {
        return pageFlipped;
    }

    public void setPageFlipped(boolean pageFlipped) {
        this.pageFlipped = pageFlipped;
    }

    public void setAbbreviations(String[] abbreviations) {
        this.abbreviations = abbreviations;
    }

    public void setLastSpokenPosition(int position) {
        progress = position;
    }
}