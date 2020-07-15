package resourceleak;

import com.amazon.fws.trigger.types.TriggerInternal;
import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Projections;

public class TestCheckIsOpen {

    public List<Integer> testCheckIsOpen() {
        Session session = null;
        Transaction tx = null;
        List<Integer> list = null;
        try {
            session = HibernateUtil.getInstance().getSessionFactory().openSession();
            tx = session.beginTransaction();
            // (i) From the trigger table, extract all the bucketIds
            // (ii) Eliminate all duplicates and return the result
            Criteria criteria = session.createCriteria(TriggerInternal.class).setProjection(
                    Projections.distinct(Projections.property("bucketId")));
            list = (List<Integer>) criteria.list();
            tx.commit();
        } catch (RuntimeException e) {
            if (tx != null && tx.isActive())
                tx.rollback();
        } finally {
            if (session != null && session.isOpen())
                session.close();
        }
        return (list == null) ? (new ArrayList<Integer>()) : list;
    }
}
