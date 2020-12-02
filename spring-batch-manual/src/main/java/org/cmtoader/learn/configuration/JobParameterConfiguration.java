package org.cmtoader.learn.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobParameterConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobParameterConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    @StepScope
    public Tasklet parameterizedTasklet(@Value("#{jobParameters['message']}") String message) {
        return (stepContribution, chunkContext) -> {
            System.out.println(message);
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Step parameterziedStep() {
        return stepBuilderFactory.get("parameterzied-step-1")
                .tasklet(parameterizedTasklet(null))
                .build();
    }

    @Bean
    public Job parameterizedJob() {
        return jobBuilderFactory.get("parameterized-job-1")
                .start(parameterziedStep())
                .build();
    }
}
