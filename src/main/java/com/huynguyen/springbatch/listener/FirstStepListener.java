package com.huynguyen.springbatch.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class FirstStepListener implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        System.out.println("Before step " + stepExecution.getStepName());
        System.out.println("Job execution context " + stepExecution.getJobExecution().getExecutionContext());
        System.out.println("Step execution context " + stepExecution.getExecutionContext());

        stepExecution.getExecutionContext().put("sec", "sec value");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        System.out.println("After step " + stepExecution.getStepName());
        System.out.println("Job execution context " + stepExecution.getJobExecution().getExecutionContext());
        System.out.println("Step execution context " + stepExecution.getExecutionContext());
        return null;
    }
}
