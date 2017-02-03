package com.handbook2.hive;


import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import java.util.ArrayList;
import java.util.List;

public class simpleUDAF2 extends UDAF {

    public static class stdevaluator implements  UDAFEvaluator {
        // write your code here

        public void init() {
            partialResult = null;
        }

        public static class partial_result{
            int number_of_terms;
            double sum_of_terms;
            List<Double> all_terms = new ArrayList<>();
        }

        private  partial_result partialResult;



        public boolean iterate(DoubleWritable value){
            if(value == null){
                return  true;
            }
            if(partialResult == null){
                partialResult = new partial_result();
            }
            partialResult.number_of_terms +=1;
            partialResult.sum_of_terms = partialResult.sum_of_terms + value.get();
            partialResult.all_terms.add(value.get());
            return  true;
        }

        public partial_result terminatePartial(){
            return partialResult;
        }

        public boolean merge(partial_result other){
            if(other == null){
                return  true;
            }
            if(partialResult == null){
                partialResult = new partial_result();
            }

            partialResult.all_terms.addAll(other.all_terms);
            partialResult.number_of_terms +=other.number_of_terms;
            partialResult.sum_of_terms +=  other.sum_of_terms;


            return  true;
        }
        public DoubleWritable terminate(){
            if(partialResult == null){return  null;}
            double mean = partialResult.sum_of_terms / partialResult.number_of_terms;
            double sum_of_squares=0;
            for (double term : partialResult.all_terms){
                sum_of_squares+=(term-mean)*(term-mean);
            }

            return new DoubleWritable(Math.sqrt(sum_of_squares/partialResult.number_of_terms) );

        }

    }
}
