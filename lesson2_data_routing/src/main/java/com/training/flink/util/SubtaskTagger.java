package com.training.flink.util;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * A diagnostic map function that prefixes each record with the subtask index
 * that processed it. Lets you SEE how records are distributed across the
 * parallel subtasks of an operator.
 *
 * Example output:   "subtask=3 | ClickEvent{userId='u17', ...}"
 *
 * Use it like:
 *   stream.map(new SubtaskTagger<>())
 *
 * Internally uses RichMapFunction so it has access to RuntimeContext and
 * can read getIndexOfThisSubtask() once in open().
 */
public class SubtaskTagger<T> extends RichMapFunction<T, String> {

    private int subtask;

    @Override
    public void open(Configuration parameters) {
        this.subtask = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public String map(T value) {
        return "subtask=" + subtask + " | " + value;
    }
}
