package com.handbook2.hive;


import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import java.util.ArrayList;
import java.util.List;

public class simpleUDAF extends UDAF {

    public static class maxevaluator implements  UDAFEvaluator {


        public void init() {
            partialResult = null;
        }


        private  DoubleWritable partialResult;



        public boolean iterate(DoubleWritable value){
            if(value == null){
                return  true;
            }
            if(partialResult == null){
                partialResult = new DoubleWritable();
            }

            partialResult.set(Math.max(value.get(),partialResult.get()));
            return  true;
        }

        public DoubleWritable terminatePartial(){
            return partialResult;
        }

        public boolean merge(DoubleWritable other) {
           return iterate(other);
        }


        public DoubleWritable terminate(){
            return partialResult;
        }

    }
}
