package inputvalidation;

import org.springframework.beans.factory.annotation.Autowired;

@Service
public class InputValidationBenchmark implements ISysJobService {

  @Override
  public int deleteJob(String job, String foo, String bar) throws SchedulerException {
     int jobId = getJobId(job);
     if (foo == null) return;
     if (bar == null) return;
     int p = getJobIdValue(foo);
     int k = getJobIdValue(bar);
     int w = p * k;
     process(w);
     int rows = jobMapper.deleteJobById(jobId);
     ScheduleUtils.deleteScheduleJob(scheduler, jobId);
     return rows;
  }
}
