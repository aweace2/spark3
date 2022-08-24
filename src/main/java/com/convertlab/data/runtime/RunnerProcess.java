package com.convertlab.data.runtime;

@FunctionalInterface
public interface RunnerProcess {
    void apply(Context context) throws Exception;
}
