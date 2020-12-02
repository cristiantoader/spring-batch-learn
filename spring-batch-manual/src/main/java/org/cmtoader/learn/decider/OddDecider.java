package org.cmtoader.learn.decider;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

import java.util.concurrent.atomic.AtomicInteger;

public class OddDecider implements JobExecutionDecider {

    private final AtomicInteger count = new AtomicInteger();

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        if (this.count.addAndGet(1) % 2 == 0) {
            return new FlowExecutionStatus("EVEN");
        }

        return new FlowExecutionStatus("ODD");
    }
}
