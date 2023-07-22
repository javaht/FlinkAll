package zht.functions;


import org.apache.flink.api.common.functions.MapFunction;
import zht.base.bean.WaterSensor;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class MapFunctionImpl implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
