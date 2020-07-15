package concurrency;

import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import com.amazon.gallery.foundation.utils.log.GLogger;

import java.util.ArrayList;
import java.util.List;

/**
 * Expandable list widget
 * @author - giacobbe
 */
public abstract class ExpandableListAdapter extends BaseAdapter {
    private static final String TAG = ExpandableListAdapter.class.getName();

    protected static final int UNINITIALIZED = -1;

    protected List<ExpandableContainer> containers = new ArrayList<>();
    protected List<ContainerItem> activeMarkers = new ArrayList<>();

    protected ExpandableContainer activeContainer = null;
    protected int activeContainerIndex = UNINITIALIZED;

    protected ExpandableContainer highlightedContainer;
    protected ContainerItem highlightedMarker;

    //Must be called on UI thread!
    public void setData(List<? extends ExpandableContainer> containers) {
        this.containers.clear();
        this.containers.addAll(containers);
        activeMarkers.clear();
        activeContainerIndex=UNINITIALIZED;
        notifyDataSetChanged();
    }

    public synchronized ContainerItem getHighlightedMarker() {
        return highlightedMarker;
    }

    public synchronized ExpandableContainer getHighlightedContainer() {
        return highlightedContainer;
    }

    public synchronized void expandContainer(int containerIndex) {
        ExpandableContainer container = containers.get(containerIndex);

        activeContainerIndex = containerIndex;
        //Set the new container as active
        activeContainer = container;

        activeMarkers.clear();
        activeMarkers.addAll(container.getSubItems());
    }

    public synchronized void collapseAll() {
        activeContainer = null;
        activeContainerIndex = UNINITIALIZED;
        activeMarkers.clear();
    }

    public void highlightContainer(int containerIndex) {
        if(highlightedContainer != null) {
            highlightedContainer.setContainerHighlighted(false);
            highlightedContainer = null;
        }
        if(containerIndex != UNINITIALIZED) {
            highlightedContainer = containers.get(containerIndex);
            highlightedContainer.setContainerHighlighted(true);
        }
        if(highlightedContainer != activeContainer && highlightedContainer != null) {
            highlightActiveMarker(UNINITIALIZED);
        }
    }

    public void highlightActiveMarker(int markerIndex) {
        highlightMarker(activeMarkers, markerIndex);
    }

    public void highlightMarker(List<ContainerItem> markers, int markerIndex) {
        if(highlightedMarker != null) {
            highlightedMarker.setLabelHighlighted(false);
        }
        if(markers == null || markerIndex < 0 || markerIndex>=markers.size()) {
            highlightedMarker = null;
            return;
        }
        ContainerItem marker = markers.get(markerIndex);

        marker.setLabelHighlighted(true);
        highlightedMarker = marker;
    }

    public void highlightElement(int containerIndex, int markerIndex) {
        if(containerIndex == UNINITIALIZED) {
            highlightMarker(null,markerIndex);
        } else {
            highlightMarker((List<ContainerItem>) containers.get(containerIndex).getSubItems(),markerIndex);
        }
        highlightContainer(containerIndex);
        notifyDataSetChanged();
    }

    public synchronized void clearList() {
        containers.clear();
        activeMarkers.clear();
        notifyDataSetChanged();
    }

    @Override
    public synchronized int getCount() {
        return containers.size() + activeMarkers.size();
    }

    public synchronized int getCountWithoutPadding() {
        return containers.size() + activeMarkers.size();
    }

    @Override
    public synchronized ExpandableListComponent getItem(int index) {
        GLogger.d(TAG, "index: %d,  ActiveContainerIndex: %d,  containerSize: %d,  markerSize: %d", index, activeContainerIndex, containers.size(), activeMarkers.size());

        if(activeContainerIndex == UNINITIALIZED || index <= activeContainerIndex) {
            return containers.get(index);
        } else if(index > activeContainerIndex + activeMarkers.size()) {
            return containers.get(index - activeMarkers.size());
        } else {
            return activeMarkers.get(index - (activeContainerIndex+1));
        }
    }

    @Override
    public synchronized long getItemId(int index) {
        return getItem(index).hashCode();
    }

    public abstract View getView(int i, View view, ViewGroup viewGroup);

    public int getActiveContainerIndex() {
        return activeContainerIndex;
    }

    public int containerToListIndex(Integer containerIndex) {
        if(activeContainerIndex<containerIndex) {
            return containerIndex+activeMarkers.size();
        }
        return containerIndex;
    }

    public List<ExpandableContainer> getContainers() {
        return containers;
    }
}