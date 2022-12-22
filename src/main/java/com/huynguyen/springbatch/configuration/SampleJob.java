package com.huynguyen.springbatch.configuration;

import com.huynguyen.springbatch.listener.FirstJobListener;
import com.huynguyen.springbatch.listener.FirstStepListener;
import com.huynguyen.springbatch.model.StudentCsv;
import com.huynguyen.springbatch.model.StudentJdbc;
import com.huynguyen.springbatch.model.StudentJson;
import com.huynguyen.springbatch.processor.FirstItemProcessor;
import com.huynguyen.springbatch.reader.FirstItemReader;
import com.huynguyen.springbatch.service.ThirdTasklet;
import com.huynguyen.springbatch.writer.FirstItemWriter;
import com.huynguyen.springbatch.writer.CsvItemWriter;
import com.huynguyen.springbatch.writer.JdbcItemWriter;
import com.huynguyen.springbatch.writer.JsonItemWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.io.File;

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
    private final CsvItemWriter csvItemWriter;
    private final JsonItemWriter jsonItemWriter;
    private final JdbcItemWriter jdbcItemWriter;
    private final DataSource dataSource;

    @Bean
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

    @Bean
    public Job thirdJob() {
        return jobBuilderFactory.get("Third Job")
                .incrementer(new RunIdIncrementer())
                .start(secondChuckStep())
                .build();
    }

    private Step secondChuckStep() {
        return stepBuilderFactory.get("Second Chunk Step")
                .<StudentCsv, StudentCsv>chunk(2)
                .reader(flatFileItemReader())
                .writer(csvItemWriter)
                .build();
    }

    public FlatFileItemReader<StudentCsv> flatFileItemReader() {
        FlatFileItemReader<StudentCsv> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new FileSystemResource(new File("D:\\01. Spring Batch\\spring-batch\\Input File\\student.csv")));
        flatFileItemReader.setLineMapper(new DefaultLineMapper<>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames("ID", "First Name", "Last Name", "Email");
                    }
                });

                setFieldSetMapper(new BeanWrapperFieldSetMapper<>() {
                    {
                        setTargetType(StudentCsv.class);
                    }
                });
            }
        });
        flatFileItemReader.setLinesToSkip(1);
        return flatFileItemReader;
    }

    @Bean
    public Job fourthJob() {
        return jobBuilderFactory.get("Fourth Job")
                .incrementer(new RunIdIncrementer())
                .start(thirdChuckStep())
                .build();
    }

    private Step thirdChuckStep() {
        return stepBuilderFactory.get("Third Chunk Step")
                .<StudentJson, StudentJson>chunk(2)
                .reader(jsonJsonItemReader())
                .writer(jsonItemWriter)
                .build();
    }

    public JsonItemReader<StudentJson> jsonJsonItemReader() {
        JsonItemReader<StudentJson> jsonJsonItemReader = new JsonItemReader<>();
        jsonJsonItemReader.setResource(new FileSystemResource(new File("D:\\01. Spring Batch\\spring-batch\\Input File\\student.json")));
        jsonJsonItemReader.setJsonObjectReader(new JacksonJsonObjectReader<>(StudentJson.class));
        return jsonJsonItemReader;
    }

    @Bean
    public Job fifthJob() {
        return jobBuilderFactory.get("Fifth Job")
                .incrementer(new RunIdIncrementer())
                .start(fourthChuckStep())
                .build();
    }

    private Step fourthChuckStep() {
        return stepBuilderFactory.get("Fourth Chunk Step")
                .<StudentJdbc, StudentJdbc>chunk(2)
                .reader(jdbcJdbcCursorItemReader())
                .writer(jdbcItemWriter)
                .build();
    }

    public JdbcCursorItemReader<StudentJdbc> jdbcJdbcCursorItemReader() {
        JdbcCursorItemReader<StudentJdbc> jdbcJdbcCursorItemReader = new JdbcCursorItemReader<>();
        jdbcJdbcCursorItemReader.setDataSource(dataSource);
        jdbcJdbcCursorItemReader.setSql("select id, first_name as firstName, last_name as lastName, email from student");
        jdbcJdbcCursorItemReader.setRowMapper(new BeanPropertyRowMapper<>() {
            {
                setMappedClass(StudentJdbc.class);
            }
        });
        return jdbcJdbcCursorItemReader;
    }
}
