package org.cmtoader.learn.configuration.flow;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class FlowLastConfiguration {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public FlowLastConfiguration(StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    public Step customStepForFlowDemoLast() {
        Tasklet customStepForFlowLast = (stepContribution, chunkContext) -> {
            System.out.println("customStepForFlowLast");
            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("customStepForFlowLast")
                .tasklet(customStepForFlowLast)
                .build();
    }

    @Bean
    public Job customJobForFlowLast(Flow commonFlow) {
        return jobBuilderFactory.get("customJobForFlowLast")
                .start(customStepForFlowDemoLast()).on("COMPLETED").to(commonFlow)
                .end()
                .build();
    }

}
