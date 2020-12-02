package org.cmtoader.learn.decider;

import org.cmtoader.learn.decider.OddDecider;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DeciderConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DeciderConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Step deciderStartStep() {
        Tasklet tasklet = (stepContribution, chunkContext) -> {
            System.out.println("Start step");
            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("decider-start-step1")
                .tasklet(tasklet)
                .build();
    }

    @Bean
    public Step evenStep() {
        Tasklet tasklet = (stepContribution, chunkContext) -> {
            System.out.println("Even step");
            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("decider-even-step")
                .tasklet(tasklet)
                .build();
    }

    @Bean
    public Step oddStep() {
        Tasklet tasklet = (stepContribution, chunkContext) -> {
            System.out.println("Odd step");
            return RepeatStatus.FINISHED;
        };

        return stepBuilderFactory.get("decider-odd-step")
                .tasklet(tasklet)
                .build();
    }

    @Bean
    public JobExecutionDecider decider() {
        return new OddDecider();
    }

    @Bean
    public Job deciderJob() {
        return jobBuilderFactory.get("decider-job")
                .start(deciderStartStep())
                .next(decider())
                    .from(decider()).on("ODD").to(oddStep())
                    .from(decider()).on("EVEN").to(evenStep())
                    .from(oddStep()).on("*").to(decider())
                    .from(evenStep()).on("*").to(decider())
                // not needed
//                    .from(decider()).on("ODD").to(oddStep())
//                    .from(decider()).on("EVEN").to(evenStep())
                    .end()
                .build();
    }
}
