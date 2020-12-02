package org.cmtoader.learn.errors;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class RestartErrorJobConfiguration {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public RestartErrorJobConfiguration(StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    @StepScope
    public Tasklet restartTasklet() {
        return (stepContribution, chunkContext) -> {
            Map<String, Object> stepExecutionContext = chunkContext.getStepContext().getStepExecutionContext();

            if (stepExecutionContext.containsKey("ran")) {
                System.out.println("This time we'll let it go.");
                return RepeatStatus.FINISHED;

            } else {
                System.out.println("I don't think so.");
                chunkContext.getStepContext().getStepExecution().getExecutionContext().put("ran", true);
                throw new RuntimeException("Not this time..");
            }
        };
    }

    @Bean
    public Step restartDemoStep1() {
        return stepBuilderFactory.get("restart-demo-step1")
                .tasklet(restartTasklet())
                .build();
    }

    @Bean
    public Step restartDemoStep2() {
        return stepBuilderFactory.get("restart-demo-step2")
                .tasklet(restartTasklet())
                .build();
    }

    @Bean
    public Job restartDemoJob() {
        return jobBuilderFactory.get("restart-demo-job")
                .start(restartDemoStep1())
                .next(restartDemoStep2())
                .build();
    }
}
