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
public class FlowFirstConfiguration {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public FlowFirstConfiguration(StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    public Step customStepForFlowDemo() {
        Tasklet customStepForFlowDemo = (stepContribution, chunkContext) -> {
            System.out.println("customStepForFlowDemo");
            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("customStepForFlowDemo")
                .tasklet(customStepForFlowDemo)
                .build();
    }

    @Bean
    public Job customJobForFlowFirst(Flow commonFlow) {
        return jobBuilderFactory.get("customJobForFlowFirst")
                .start(commonFlow)
                .next(customStepForFlowDemo())
                .end()
                .build();
    }


}
