package org.cmtoader.learn.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class JobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Step helloWorldStep() {
        Tasklet tasklet = (stepContribution, chunkContext) -> {
            System.out.println("Hello world");
            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("helloWorldStep")
                .tasklet(tasklet)
                .build();
    }

    @Bean
    public Job job1() {
        return jobBuilderFactory.get("helloWorldJob")
                         .start(helloWorldStep())
                         .build();
    }
}
