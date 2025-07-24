package io.openlineage.spark.agent;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

public class NuOpenLineageSparkListener extends OpenLineageSparkListener {

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        if (event instanceof SparkListenerSQLExecutionEnd) {
            super.onOtherEvent(event);
        }
    }



    @Override
    public void onJobEnd(org.apache.spark.scheduler.SparkListenerJobEnd jobEnd) {
        super.onJobEnd(jobEnd);
    }
}
