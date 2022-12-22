package com.huynguyen.springbatch.service;

import com.huynguyen.springbatch.request.JobParamRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class JobService {

    private final JobLauncher jobLauncher;

    @Qualifier("firstJob")
    private final Job firstJob;

    @Qualifier("secondJob")
    private final Job secondJob;

    @Async
    public void startJob(String jobName, List<JobParamRequest> paramRequests) {
        Map<String, JobParameter> params = new HashMap<>();
        paramRequests.forEach(param -> params.put(param.getParamKey(), new JobParameter(param.getParamValue())));

        JobParameters jobParameters = new JobParameters(params);
        try {
            JobExecution jobExecution = null;
            switch (jobName) {
                case "First Job":
                    jobExecution = jobLauncher.run(firstJob, jobParameters);
                    break;
                case "Second Job":
                    jobExecution = jobLauncher.run(secondJob, jobParameters);
                    break;
                default:
                    break;
            }
            System.out.println("Job Execution ID: " + jobExecution.getId());
        } catch (Exception e) {
            System.out.println("Exception while starting job");
        }
    }
}
