package com.tdw.trino.eventlistener;

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryListener implements EventListener {
    private static final Logger LOGGER = LogManager.getLogger(QueryListener.class);
    public QueryListener() {
    }

    @Override
    public void queryCompleted(final QueryCompletedEvent queryCompletedEvent) {
        LOGGER.info("Received query event: " + queryCompletedEvent.toString());
    }
}