package org.cmtoader.learn.listener;

import org.springframework.batch.core.annotation.AfterChunk;
import org.springframework.batch.core.annotation.BeforeChunk;
import org.springframework.batch.core.scope.context.ChunkContext;

public class ChunkListener {

    @BeforeChunk
    public void beforeChunk(ChunkContext chunkContext) {
        System.out.println("Before chunk " + chunkContext.toString());
    }

    @AfterChunk
    public void afterChunk(ChunkContext chunkContext) {
        System.out.println("After chunk " + chunkContext.toString());
    }
}
