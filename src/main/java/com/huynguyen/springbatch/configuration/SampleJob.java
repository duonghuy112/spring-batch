package com.huynguyen.springbatch.configuration;

import com.huynguyen.springbatch.listener.FirstJobListener;
import com.huynguyen.springbatch.listener.FirstStepListener;
import com.huynguyen.springbatch.processor.FirstItemProcessor;
import com.huynguyen.springbatch.reader.FirstItemReader;
import com.huynguyen.springbatch.service.ThirdTasklet;
import com.huynguyen.springbatch.writer.FirstItemWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class SampleJob {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final ThirdTasklet thirdTasklet;
    private final FirstJobListener firstJobListener;
    private final FirstStepListener firstStepListener;
    private final FirstItemReader firstItemReader;
    private final FirstItemProcessor firstItemProcessor;
    private final FirstItemWriter firstItemWriter;

    //@Bean
    public Job firstJob() {
        return jobBuilderFactory.get("First Job")
                .start(firstStep())
                .incrementer(new RunIdIncrementer())
                .next(secondStep())
                .next(thirdStep())
                .listener(firstJobListener)
                .build();
    }

    private Step firstStep() {
        return stepBuilderFactory.get("First Step")
                .tasklet(firstTask())
                .listener(firstStepListener)
                .build();
    }

    private Tasklet firstTask() {
        return (stepContribution, chunkContext) -> {
            System.out.println("This is first tasklet step");
            System.out.println("SEC = " + chunkContext.getStepContext().getStepExecutionContext());
            return RepeatStatus.FINISHED;
        };
    }

    private Step secondStep() {
        return stepBuilderFactory.get("Second Step")
                .tasklet(secondTask())
                .build();
    }

    private Tasklet secondTask() {
        return (stepContribution, chunkContext) -> {
            System.out.println("This is second tasklet step");
            return RepeatStatus.FINISHED;
        };
    }

    private Step thirdStep() {
        return stepBuilderFactory.get("Third Step")
                .tasklet(thirdTasklet)
                .build();
    }

    @Bean
    public Job secondJob() {
        return jobBuilderFactory.get("Second Job")
                .incrementer(new RunIdIncrementer())
                .start(firstChuckStep())
                .next(firstStep())
                .build();
    }

    private Step firstChuckStep() {
        return stepBuilderFactory.get("First Chunk Step")
                .<Integer, Long>chunk(3)
                .reader(firstItemReader)
                .processor(firstItemProcessor)
                .writer(firstItemWriter)
                .build();
    }
}
