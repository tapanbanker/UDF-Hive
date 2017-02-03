package com.handbook2.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * A user defined function that replaces all occurrences of a string within a text with another string
 */


@Description(
        name = "replaceText",
        value = "_FUNC_(text,str1,str2) - replaces all occurrences of a string within a text with another string",
        extended = "Example:\n" +
                "  > SELECT name,replaceText(name,'abc','def') FROM authors a;\n" +
                "  abcabc  defdef"
)
public class simpleUDF extends UDF {
    private Text result = new Text();

    public Text evaluate(String str, String str1, String str2) {

        String rep = str.replace(str1, str2);
        result.set(rep);
        return result;

    }

}
