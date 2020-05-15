package com.github.broker;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.jgit.lib.ProgressMonitor;

@Slf4j
class SimpleProgressMonitor implements ProgressMonitor {

        @Override
        public void start(int totalTasks) {
            LOGGER.debug("Starting work on " + totalTasks + " tasks");
        }

        @Override
        public void beginTask(String title, int totalWork) {
            LOGGER.debug("Start " + title + ": " + totalWork);
        }

        @Override
        public void update(int completed) {
            LOGGER.debug(completed + "-");
        }

        @Override
        public void endTask() {
            LOGGER.debug("Done");
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
}
