package com.huynguyen.springbatch.controller;

import com.huynguyen.springbatch.request.JobParamRequest;
import com.huynguyen.springbatch.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/job")
@RequiredArgsConstructor
public class JobController {

    private final JobService jobService;
    private final JobOperator jobOperator;

    @GetMapping("/start/{jobName}")
    public String startJob(@PathVariable String jobName, @RequestBody List<JobParamRequest> paramRequests) {
        jobService.startJob(jobName, paramRequests);
        return "Job Started...";
    }

    @GetMapping("/stop/{jobExecutionId}")
    public String stopJob(@PathVariable long jobExecutionId) {
        try {
            jobOperator.stop(jobExecutionId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return "Job Stopped...";
    }
}
