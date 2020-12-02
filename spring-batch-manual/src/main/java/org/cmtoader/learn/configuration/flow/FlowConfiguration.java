package org.cmtoader.learn.configuration.flow;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class FlowConfiguration {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public FlowConfiguration(StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    public Step step1Flow() {
        Tasklet step1Flow = (stepContribution, chunkContext) -> {
            System.out.println("step1Flow");
            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("step1Flow")
                .tasklet(step1Flow)
                .build();
    }

    @Bean
    public Step step2Flow() {
        Tasklet step2Flow = (stepContribution, chunkContext) -> {
            System.out.println("step2Flow");
            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("step2Flow")
                .tasklet(step2Flow)
                .build();
    }

    @Bean
    public Flow commonFlow() {
        FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("flow1");

        return flowBuilder.start(step1Flow())
                .next(step2Flow())
                .build();
    }


}
