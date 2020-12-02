package org.cmtoader.learn.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TransitionConfiguration {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public TransitionConfiguration(StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    public Step step1() {
        Tasklet step1 = (stepContribution, chunkContext) -> {
            System.out.println("step1");
            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("step1")
                .tasklet(step1)
                .build();
    }

    @Bean
    public Step step2() {
        Tasklet step2 = (stepContribution, chunkContext) -> {
            System.out.println("step2");
            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("step2")
                .tasklet(step2)
                .build();
    }

    @Bean
    public Step step3() {
        Tasklet step3 = (stepContribution, chunkContext) -> {
            System.out.println("step3");
            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("step3")
                .tasklet(step3)
                .build();
    }

    @Bean
    public Job transitionJobSimpleNext() {
        return jobBuilderFactory.get("transitionNextJob")
                .start(step1())
                .next(step2())
                .next(step3())
                .build();
    }

    @Bean
    public Job transitionJobSimpleVerboseNext() {
        return jobBuilderFactory.get("transitionVerboseNextJob")
                .start(step1()).on("COMPLETED").to(step2())// this is based on exit code, not job status
                .from(step2()).on("COMPLETED").to(step3())
                .from(step3()).end()// marks last step in the flow
                .build();
    }

    @Bean
    public Job transitionJobStopAndRestartNext() {
        return jobBuilderFactory.get("transitionStopAndRestartNextJob")
                .start(step1()).on("COMPLETED").to(step2())// this is based on exit code, not job status
                .from(step2()).on("COMPLETED").stopAndRestart(step3())
                .from(step3()).end()// marks last step in the flow
                .build();
    }
}
