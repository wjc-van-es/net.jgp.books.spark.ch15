package net.jgp.books.spark.ch15.lab100_orders;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

/**
 * Orders analytics.
 *
 * @author jgp
 */
public class OrderStatisticsApp {
    private static Logger log =
            LoggerFactory.getLogger(OrderStatisticsApp.class);

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        OrderStatisticsApp app = new OrderStatisticsApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Orders analytics")
                .master("local[*]")
                .getOrCreate();

        // Reads a CSV file with header, called orders.csv, stores it in a
        // dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("data/orders/orders.csv");

        // Calculating the orders info using the dataframe API
        Dataset<Row> apiDf = df
                .groupBy(col("firstName"), col("lastName"), col("state"))
                .agg(sum("quantity"), sum("revenue"), avg("revenue"))
                .withColumnRenamed("sum(quantity)", "Total quantity" ) // rename column
                .withColumn("Total Revenue", round(col("sum(revenue)"), 2))// add total revenue column rounding on 2 decimals
                .withColumn("Average Revenue", round(col("avg(revenue)"), 2)) // add average revenue column rounding on 2 decimals
                .drop("sum(revenue)", "avg(revenue)") //drop the columns
                .orderBy(desc("Total Revenue")); // in descending order of Total revenue
        apiDf.show(20);

        // Calculating the orders info using SparkSQL
        df.createOrReplaceTempView("orders");
        String sqlStatement = "SELECT " +
                "    firstName, " +
                "    lastName, " +
                "    state, " +
                "    SUM(quantity) as Total_Quantity, " +
                "    SUM(revenue) as Total_Revenue, " +
                "    AVG(revenue) as Average_Revenue" +
                "  FROM orders " +
                "  GROUP BY firstName, lastName, state" +
                "  ORDER BY Total_Revenue desc";
        Dataset<Row> sqlDf = spark.sql(sqlStatement);
        sqlDf.show(20);
    }
}
