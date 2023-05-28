package zht.functions;


import org.apache.flink.api.common.functions.MapFunction;
import zht.bean.WaterSensor;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class WaterSensorMapFunction implements MapFunction<String,WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] datas = value.split(",");
        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
    }
}
