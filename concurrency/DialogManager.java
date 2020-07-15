package concurrency;

import android.app.Activity;
import android.app.Dialog;
import android.content.Context;
import android.content.DialogInterface;
import android.os.Bundle;
import android.support.v4.app.*;
import android.support.v7.app.AlertDialog;
import com.amazon.gallery.foundation.utils.DebugAssert;
import com.amazon.gallery.framework.gallery.actions.PersistentDialogFragment;
import com.amazon.gallery.thor.app.ThorGalleryApplication;
import com.amazon.photos.R;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Dialog manager is used to create custom dialogs and tracks at an application level the dialogs displayed to the user.
 * This manager ensures that dialogs are queued up and the user doesn't have dialogs popping up over other dialogs.
 *
 * @author giacobbe
 */
public class DialogManager {
    public final static String DIALOG_FRAGMENT_TAG = "DIALOG_FRAGMENT_TAG";
    protected Dialog activeDialog = null;

    private Queue<ShowDialogTask> pendingDialogTasks = new ConcurrentLinkedQueue<>();
    private ShowDialogTask currentDialogTask = null;

    public AlertDialog createConfirmDialogWithOKButton(Activity context, int confirmMsgId) {
        DebugAssert.assertFalse((activeDialog != null && activeDialog.isShowing()), "You are creating a dialog when one already is displayed");

        AlertDialog.Builder dialogBuilder = new AlertDialog.Builder(context);
        dialogBuilder.setTitle(confirmMsgId);
        dialogBuilder.setPositiveButton(R.string.adrive_gallery_common_dialog_positive, (dialog, which) -> {
            dialog.dismiss();
            activeDialog = null;
        });
        AlertDialog thisDialog = dialogBuilder.create();

        activeDialog = thisDialog;

        return thisDialog;
    }

    /**
     * Create a dialog
     * @return custom dialog
     */
    public AlertDialog createDialog(Activity context, String confirm, String cancel, String message, DialogInterface.OnClickListener onClickListener) {
        DebugAssert.assertFalse((activeDialog != null && activeDialog.isShowing()), "You are creating a dialog when one already is displayed");

        AlertDialog.Builder dialogBuilder = new AlertDialog.Builder(context);
        dialogBuilder.setPositiveButton(confirm, onClickListener);
        dialogBuilder.setNegativeButton(cancel, (dialog, which) -> {
            dialog.dismiss();
            activeDialog = null;
        });

        if (message != null) {
            dialogBuilder.setMessage(message);
        }

        AlertDialog thisDialog = dialogBuilder.create();

        activeDialog = thisDialog;

        return thisDialog;
    }

    public AlertDialog createCustomDialog(Context context, String title, String message,
                                               String positive, String negative,
                                               DialogInterface.OnClickListener clickListener) {
        DebugAssert.assertFalse((activeDialog != null && activeDialog.isShowing()), "You are creating a dialog when one already is displayed");

        AlertDialog.Builder customDialogBuilder = new AlertDialog.Builder(context)
                .setTitle(title)
                .setMessage(message)
                .setPositiveButton(positive, clickListener)
                .setNegativeButton(negative, clickListener)
                .setCancelable(false);

        AlertDialog thisDialog;
        thisDialog = customDialogBuilder.create();

        activeDialog = thisDialog;

        return thisDialog;
    }

    public AlertDialog createCustomDialog(Context context, String title, String message,
                                          String positive,
                                          DialogInterface.OnClickListener clickListener) {
        DebugAssert.assertFalse((activeDialog != null && activeDialog.isShowing()), "You are creating a dialog when one already is displayed");

        AlertDialog.Builder customDialogBuilder = new AlertDialog.Builder(context)
                .setTitle(title)
                .setMessage(message)
                .setPositiveButton(positive, clickListener)
                .setCancelable(false);

        AlertDialog thisDialog;
        thisDialog = customDialogBuilder.create();

        activeDialog = thisDialog;

        return thisDialog;
    }

    public void dismissActiveDialog() {
        clearInvalidDialog();
        if(activeDialog != null) {
            if(activeDialog.isShowing()) {
                activeDialog.dismiss();
            }
            activeDialog = null;
        }
    }

    public void dismissActiveDialogTaskAndPendingDialogsIfPresent() {
        pendingDialogTasks.clear();
        if (currentDialogTask != null) {
            currentDialogTask.dismiss();
            currentDialogTask = null;
        } else {
            dismissActiveDialog();
        }
    }

    public void dismissActiveAndPendingDialogs() {
        pendingDialogTasks.clear();
        dismissActiveDialog();
        if (currentDialogTask != null) {
            currentDialogTask.cancel();
            currentDialogTask = null;
        }
    }

    public boolean isDialogActive() {
        clearInvalidDialog();
        return activeDialog != null && activeDialog.isShowing();
    }

    private void clearInvalidDialog() {
        if (activeDialog != null && activeDialog.getWindow() != null && !activeDialog.getWindow().isActive()) {
            activeDialog = null;
        }
    }

    public void queueDialogTask(final ShowDialogTask showDialogTask) {
        pendingDialogTasks.add(showDialogTask);
        executeNextDialogTask();
    }

    /**
     * Utility method that dismisses all existing dialogs and queues up an instance
     * of {@link ShowDialogAsyncTask} on the DialogManager
     *
     * @param fragmentActivity the context in which the dialog is to be displayed
     * @param dialogFragment the dialog fragment to display
     */
    public void dismissActiveDialogsAndShowDialog(final FragmentActivity fragmentActivity, final PersistentDialogFragment dialogFragment) {
        dismissActiveAndPendingDialogs();

        new ShowDialogFragmentSyncTask(fragmentActivity){
            @Override
            protected PersistentDialogFragment createDialogFragment() {
                return dialogFragment;
            }
        }.queue();
    }

    private void executeNextDialogTask() {
        if(currentDialogTask != null) {
            return;
        }

        if(pendingDialogTasks.isEmpty()) {
            return;
        }

        currentDialogTask = pendingDialogTasks.remove();
        if(currentDialogTask.mActivity.isFinishing()) {
            executeNextDialogTask();
            return;
        }

        currentDialogTask.execute();
    }

    private void completeTask() {
        if(currentDialogTask != null) {
            currentDialogTask.cancel();
            currentDialogTask = null;
        }

        executeNextDialogTask();
    }

    private static void showDialogFragment(final FragmentActivity fragmentActivity, final DialogFragment dialogFragment) {
        FragmentManager fragmentManager = fragmentActivity.getSupportFragmentManager();
        FragmentTransaction fragmentTransaction = fragmentManager.beginTransaction();
        Fragment previousFragment = fragmentManager.findFragmentByTag(DIALOG_FRAGMENT_TAG);
        if (previousFragment != null) {
            fragmentTransaction.remove(previousFragment);
        }
        fragmentTransaction.add(dialogFragment, DIALOG_FRAGMENT_TAG);
        fragmentTransaction.commitAllowingStateLoss();
    }

    private static void removeDialogFragmentIfPresent(final FragmentActivity fragmentActivity) {
        FragmentManager fragmentManager = fragmentActivity.getSupportFragmentManager();
        FragmentTransaction fragmentTransaction = fragmentManager.beginTransaction();
        Fragment previousFragment = fragmentManager.findFragmentByTag(DIALOG_FRAGMENT_TAG);
        if (previousFragment != null) {
            fragmentTransaction.remove(previousFragment);
            fragmentTransaction.commitAllowingStateLoss();
        }
    }

    /**
     * Task that is queued in the DialogManager and used to display a sequence of dialogs one after another.
     * This is a nested class so we have access to private methods for creating dialogs and the only
     * way to create a dialog is by queueing it.
     */
    public abstract static class ShowDialogTask implements DialogInterface.OnDismissListener {

        protected final Activity mActivity;
        protected final DialogManager mDialogManager;

        DialogInterface.OnDismissListener mDismissListener = null;
        DialogInterface.OnCancelListener mCancelListener = null;
        Bundle mBundle = null;
        boolean mCancellable = true;

        ShowDialogTask(Activity activity) {
            mActivity = activity;
            mDialogManager = ThorGalleryApplication.getAppComponent().getDialogManager();
        }

        public ShowDialogTask useDismissListener(DialogInterface.OnDismissListener listener) {
            mDismissListener = listener;
            return this;
        }

        public ShowDialogTask useCancelListener(DialogInterface.OnCancelListener listener) {
            mCancelListener = listener;
            return this;
        }

        public ShowDialogTask useBundle(Bundle bundle) {
            mBundle = bundle;
            return this;
        }

        public ShowDialogTask setCancellable(boolean cancellable) {
            mCancellable = cancellable;
            return this;
        }

        public void queue() {
            mDialogManager.queueDialogTask(this);
        }

        public void dismiss() {
            if(mDialogManager.currentDialogTask != null) {
                if(mActivity instanceof FragmentActivity) {
                    FragmentActivity fragmentActivity = (FragmentActivity) mActivity;
                    if (fragmentActivity.getSupportFragmentManager().isDestroyed()) {
                        mDialogManager.completeTask();
                        return;
                    }
                    DialogManager.removeDialogFragmentIfPresent(fragmentActivity);
                } else {
                    mDialogManager.dismissActiveDialog();
                }

                mDialogManager.currentDialogTask.cancel();
                mDialogManager.currentDialogTask = null;
            }
        }

        protected abstract void execute();

        protected abstract void cancel();


        public void onDismiss(DialogInterface dialog) {
            if(mDismissListener != null) {
                mDismissListener.onDismiss(dialog);
            }

            mDialogManager.completeTask();
        }

        void showDialog() {
            if(mActivity instanceof FragmentActivity) {
                FragmentActivity fragmentActivity = (FragmentActivity)mActivity;
                if(fragmentActivity.getSupportFragmentManager().isDestroyed()) {
                    mDialogManager.completeTask();
                    return;
                }

                DialogFragment dialogFragment = getDialogFragment();
                DialogManager.showDialogFragment(fragmentActivity, dialogFragment);
            } else {
                Dialog dialog = createDialog();
                dialog.show();
            }
        }

        void completeWithoutShowingDialog() {
            mDialogManager.completeTask();
        }

        protected abstract Dialog createDialog();

        protected PersistentDialogFragment getDialogFragment() {
            PersistentDialogFragment dialogFragment = new PersistentDialogFragment();
            // Must use deprecated setDialog here. A better setup for new code is
            // using ShowDialogFragmentSyncTask to bypass this code, see DeleteMediaAction.
            // Using an anonymous subclass of PersistentDialogFragment here causes PHOTOS-10191.
            dialogFragment.setDialog(createDialog());
            dialogFragment.setOnDismissListener(this);
            dialogFragment.setOnCancelListener(mCancelListener);
            dialogFragment.setCancelable(mCancellable);
            if (mBundle != null) {
                dialogFragment.setArguments(mBundle);
            }
            return dialogFragment;
        }
    }
}
