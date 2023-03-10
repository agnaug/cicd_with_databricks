from spark.sql import DataFrame, SparkSession


class GoldAggregations:
  
    def total_num_orders(spark: SparkSession, orders: str) -> DataFrame:
        """Total number of orders"""
        sql = f"""
          SELECT 
            count(*) AS total_orders 
          FROM {orders}
        """
        
        return spark.sql(sql)

      
    def total_sales_amount_in_usd(spark: SparkSession, sales: str) -> DataFrame:
        """Total sales amount in USD"""
        sql = f"""
          SELECT 
            sum(sale_amount) AS total_sales
          FROM {sales} 
          WHERE currency = 'USD'
        """
        
        return spark.sql(sql)

      
    def top_10_best_selling_products(spark: SparkSession, sales: str, products: str) -> DataFrame:
        """Top 10 best selling products by sales amount"""
        sql = f"""
          SELECT 
            p.product_id, 
            p.product_category,
            sum(s.sale_amount) AS total_sales
          FROM {products} p
          JOIN {sales} s 
            ON p.product_id = s.product_id
          GROUP BY 
            p.product_id, 
            p.product_category
          ORDER BY 
            total_sales DESC
          LIMIT 10
        """
        
        return spark.sql(sql)

       
    def num_customers_by_state(spark: SparkSession, customers: str) -> DataFrame:
        """Number of customers by state"""
        sql = f"""
          SELECT 
            state, 
            count(distinct customer_id) AS total_customers
          FROM {customers}
          GROUP BY 
            state
        """
        
        return spark.sql(sql)


    def avg_sales_by_month(spark: SparkSession, sales: str) -> DataFrame:
        """Average sales amount by month"""
        sql = f"""
          SELECT
            year(sale_date) AS year,
            month(sale_date) AS month,
            avg(sale_amount) AS avg_sales
          FROM {sales}
          GROUP BY 
            year, 
            month
        """
        return spark.sql(sql)
