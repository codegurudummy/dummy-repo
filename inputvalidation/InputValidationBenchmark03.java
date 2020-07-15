package com.amazon.somepkg;

import org.springframework.beans.factory.annotation.Autowired;

@Service
public class InputValidationBenchmark implements ISysJobService {

  @Override
  public int deleteJob(String job, String foo, String bar, String zoo, String baz) throws SchedulerException {
     int jobId = getJobId(job);
     if (foo == null) return;
     if (bar == null) return;
     if (zoo == null) return;
     if (baz == null) return;
     int p1 = getJobIdValue(foo);
     int p2 = getJobIdValue(bar);
     int p3 = getJobIdValue(zoo);
     int p4 = getJobIdValue(baz);
     int w = (p1 * p2) / (p3 * p4);
     process(w);
     int rows = jobMapper.deleteJobById(jobId);
     ScheduleUtils.deleteScheduleJob(scheduler, jobId);
     return rows;
  }
}
