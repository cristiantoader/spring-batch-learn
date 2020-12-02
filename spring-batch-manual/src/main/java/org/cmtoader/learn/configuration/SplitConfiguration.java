package org.cmtoader.learn.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
public class SplitConfiguration {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public SplitConfiguration(StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    public Tasklet makeTasklet() {
        return  (stepContribution, chunkContext) -> {
            System.out.println(String.format("Hello world from step %s thread %s",
                    chunkContext.getStepContext().getStepName(),
                    Thread.currentThread().getName()));

            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Flow splitFlow1() {
        return new FlowBuilder<Flow>("flow1-split")
                .start(stepBuilderFactory.get("step1").tasklet(makeTasklet()).build())
                .build();
    }

    @Bean
    public Flow splitFlow2() {
        return new FlowBuilder<Flow>("flow2-split")
                .start(stepBuilderFactory.get("step2").tasklet(makeTasklet()).build())
                .next(stepBuilderFactory.get("step3").tasklet(makeTasklet()).build())
                .build();
    }

    @Bean
    public Job jobSplit(){
        return jobBuilderFactory.get("job1-split")
                .start(splitFlow1())
                .split(new SimpleAsyncTaskExecutor()).add(splitFlow2(), splitFlow2()).end()
                .build();
    }
}
