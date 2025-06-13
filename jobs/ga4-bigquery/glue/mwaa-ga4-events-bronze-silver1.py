import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StringType
from pyspark.sql.functions import current_timestamp, lit, date_format, concat, coalesce, col, sha2, substring, min, to_json, year, month, dayofmonth, to_date, format_string, when, last, first
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime, timedelta, timezone
from pyspark.sql.window import Window

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    logger.info("Creating Spark session")
    return (SparkSession.builder
            .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') # see https://hudi.apache.org/docs/quick-start-guide/
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') # see https://hudi.apache.org/docs/quick-start-guide/
            .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') # see https://hudi.apache.org/docs/quick-start-guide/
            # .config('spark.kryo.registrator', 'org.apache.spark.HoodieKryoRegistrar') # see https://hudi.apache.org/docs/quick-start-guide/ but it causes an errrrrrrrror
            .config('spark.sql.hive.convertMetastoreParquet', 'false')
            .config('spark.sql.adaptive.enabled', 'true')
            .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
            .config('spark.sql.adaptive.skewJoin.enabled', 'true')
            # .config('spark.dynamicAllocation.enabled', 'true') # DO NOT USE THIS ONE UNLESS YOU LIKE WASTING YOUR TIME DEBUGGING
            # .config('spark.shuffle.service.enabled', 'true') # # DO NOT USE THIS ONE TOO
            .config('spark.sql.files.maxPartitionBytes', '256m')
            .config('spark.default.parallelism', '80')
            .getOrCreate())

def init_job(spark):
    logger.info("Initializing Glue job")
    logger.info(f'Using spark version {spark.version}')
    sc = spark.sparkContext
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    return glue_context, job

def get_job_args():
    logger.info("Getting job arguments")
    return getResolvedOptions(sys.argv, ['JOB_NAME', 'last_run_dt'])

def get_date_info(last_run_dt_str):
    last_run_dt = datetime.fromisoformat(last_run_dt_str.rstrip('Z')).replace(tzinfo=timezone.utc)
    current_run_dt = datetime.utcnow()
    return {
        'start_date': last_run_dt.strftime('%Y%m%d'),
        'start_date_year': last_run_dt.strftime('%Y'),
        'start_date_month': last_run_dt.strftime('%m'),
        'start_date_day': last_run_dt.strftime('%d'),
        'end_date': current_run_dt.strftime('%Y-%m-%d')
    }
    
def read_source_data(spark, glue_context, date_info):
    logger.info(f"Reading data from {date_info['start_date']} partition")
    
    source_df = spark.sql(f"""
        select * 
        from 
            tdp_bronze.ga4_bigquery_events_analytics_408060011 
        where 
            year >= {date_info['start_date_year']} 
        and month >= {date_info['start_date_month']} 
        and day >= {date_info['start_date_day']}
        """)

    logger.info(f"Number of records to be processed: {source_df.count()}")
    return DynamicFrame.fromDF(source_df, glue_context, 'source_dyf')
    
def get_min_event_date(dyf):
    logger.info(f"Retrieving min event_date for partition data")
    df = dyf.toDF()
    min_date = df.agg(min(df.event_date)).collect()[0][0]
    min_date_formatted = f"{min_date[:4]}-{min_date[4:6]}-{min_date[6:]}"
    logger.info(f"Minimum event_date in current partition: {min_date_formatted}")
    return min_date_formatted
    
def transform_data(glue_context, dyf):
    logger.info(f"Executing query transformations")
    # we convert the dynamic frame into a dataframe to take advtange of the temp view method so we can apply our transformations in sql
    df = dyf.toDF()
    df.createOrReplaceTempView("tdp_bronze_ga4_bigquery_events")

    query =  f"""
    SELECT DISTINCT
        concat(
            cast(event_params['ga_session_id'].int_value as varchar(50))
            , '_'
            , user_pseudo_id
        )                                                       as unique_session_id
    ,   cast(event_date as int)                                 as event_date
    ,   event_timestamp                                         as event_timestamp
    ,   event_name                                              as event_name
    ,   event_params['affiliation'].string_value                as event_affiliation
    ,   event_params['batch_ordering_id'].int_value             as event_batch_ordering_id
    ,   event_params['batch_page_id'].int_value                 as event_batch_page_id
    ,   event_params['campaign'].string_value                   as event_campaign
    ,   event_params['campaign_id'].string_value                as event_campaign_id
    ,   event_params['content'].string_value                    as event_content
    ,   event_params['content_group'].string_value              as event_content_group
    ,   event_params['currency'].string_value                   as event_currency
    ,   event_params['debug_mode'].int_value                    as event_debug_mode
    ,   event_params['engaged_session_event'].int_value         as event_engaged_session_event
    ,   event_params['engagement_time_msec'].int_value          as event_engagement_time_msec
    ,   event_params['entrances'].string_value                  as event_entrances
    ,   event_params['faq_category'].string_value               as event_faq_category
    ,   event_params['file_extension'].string_value             as event_file_extension
    ,   event_params['file_name'].string_value                  as event_file_name
    ,   event_params['firebase_conversion'].int_value           as event_firebase_conversion
    ,   event_params['ga_session_id'].int_value                 as event_ga_session_id
    ,   event_params['ga_session_number'].int_value             as event_ga_session_number
    ,   event_params['gclid'].string_value                      as event_string_value
    ,   event_params['ignore_referrer'].string_value            as event_ignore_referrer
    ,   event_params['item_list_id'].string_value               as event_item_list_id
    ,   event_params['item_list_name'].string_value             as event_item_list_name
    ,   event_params['link_text'].double_value                  as event_link_text_double
    ,   event_params['link_text'].int_value                     as event_link_text_int
    ,   event_params['link_text'].string_value                  as event_link_text_str
    ,   event_params['link_url'].string_value                   as event_link_url
    ,   event_params['medium'].string_value                     as event_medium
    ,   event_params['navigation_type'].string_value            as event_navigation_type
    ,   event_params['newsletter_region'].int_value             as event_newsletter_region_int
    ,   event_params['newsletter_region'].string_value          as event_newsletter_region_str
    ,   event_params['page_location'].string_value              as event_page_location
    ,   event_params['page_referrer'].string_value              as event_page_referrer
    ,   event_params['page_title'].string_value                 as event_page_title
    ,   event_params['price'].double_value                      as event_price_double
    ,   event_params['price'].int_value                         as event_price_int
    ,   event_params['search_term'].double_value                as event_search_term_double
    ,   event_params['search_term'].int_value                   as event_search_term_int
    ,   event_params['search_term'].string_value                as event_search_term_str
    ,   event_params['session_engaged'].int_value               as event_session_engaged_int
    ,   event_params['session_engaged'].string_value            as event_session_engaged_str
    ,   event_params['shipping'].int_value                      as event_shipping
    ,   event_params['source'].string_value                     as event_source
    ,   event_params['store_region'].string_value               as event_store_region
    ,   event_params['tax'].int_value                           as event_tax
    ,   event_params['term'].string_value                       as event_term
    ,   event_params['transaction_id'].string_value             as event_transaction_id
    ,   event_params['type'].string_value                       as event_type
    ,   event_params['unique_search_term'].int_value            as event_unique_search_term
    ,   event_params['value'].double_value                      as event_value_double
    ,   event_params['value'].int_value                         as event_value_int
    ,   event_params['video_current_time'].int_value            as event_video_current_time
    ,   event_params['video_duration'].int_value                as event_video_duration
    ,   event_params['video_percent'].int_value                 as event_video_percent
    ,   event_params['video_provider'].string_value             as event_video_provider
    ,   event_params['video_title'].string_value                as event_video_title
    ,   event_params['video_url'].string_value                  as event_video_url
    ,   event_params['visible'].string_value                    as event_visible
    ,   event_previous_timestamp                                as event_previous_timestamp
    ,   event_value_in_usd                                      as event_value_in_usd
    ,   event_bundle_sequence_id                                as event_bundle_sequence_id
    ,   event_server_timestamp_offset                           as event_server_timestamp_offset
    ,   ''                                                      as event_user_id
    ,   user_id                                                 as user_id
    ,   user_pseudo_id                                          as user_pseudo_id
    ,   privacy_info.analytics_storage                          as privacy_analytics_storage
    ,   privacy_info.ads_storage                                as privacy_ads_storage
    ,   privacy_info.uses_transient_token                       as privacy_uses_transient_token
    ,   user_properties['shopping_method'].string_value         as user_shopping_method
    ,   user_properties['store_id'].int_value                   as user_store_id
    ,   user_properties['store_id'].set_timestamp_micros        as user_store_id_timestamp
    ,   user_properties['store_name'].string_value              as user_store_name
    ,   user_properties['store_name'].set_timestamp_micros      as user_store_name_timestamp
    ,   user_properties['store_region'].string_value            as user_store_region
    ,   user_properties['store_region'].set_timestamp_micros    as user_store_region_timestamp
    ,   user_first_touch_timestamp                              as user_first_touch_timestamp
    ,   user_ltv.revenue                                        as user_ltv_revenue
    ,   user_ltv.currency                                       as user_ltv_currency
    ,   device.category                                         as device_category
    ,   device.mobile_brand_name                                as device_mobile_brand_name
    ,   device.mobile_model_name                                as device_mobile_model_name
    ,   device.mobile_marketing_name                            as device_mobile_marketing_name
    ,   device.mobile_os_hardware_model                         as device_mobile_os_hardware_model
    ,   device.operating_system                                 as device_operating_system
    ,   device.operating_system_version                         as device_operating_system_version
    ,   device.vendor_id                                        as device_vendor_id
    ,   device.advertising_id                                   as device_advertising_id
    ,   device.language                                         as device_language
    ,   device.is_limited_ad_tracking                           as device_is_limited_ad_tracking
    ,   device.time_zone_offset_seconds                         as device_time_zone_offset_seconds
    ,   device.browser                                          as device_browser
    ,   device.browser_version                                  as device_browser_version
    ,   device['web_info'].browser                              as device_web_info_browser
    ,   device['web_info'].browser_version                      as device_web_info_browser_version
    ,   device['web_info'].hostname                             as device_web_info_hostname
    ,   geo.city                                                as geo_city
    ,   geo.country                                             as geo_country
    ,   geo.continent                                           as geo_continent
    ,   geo.region                                              as geo_region
    ,   geo.sub_continent                                       as geo_sub_continent
    ,   geo.metro                                               as geo_metro
    ,   app_info.id                                             as app_id
    ,   app_info.version                                        as app_version
    ,   app_info.install_store                                  as app_install_store
    ,   app_info.firebase_app_id                                as app_firebase_app_id
    ,   app_info.install_source                                 as app_install_source
    ,   traffic_source.name                                     as traffic_source_name
    ,   traffic_source.medium                                   as traffic_source_medium
    ,   traffic_source.source                                   as traffic_source
    ,   stream_id                                               as stream_id
    ,   platform                                                as platform
    ,   event_dimensions.hostname                               as hostname
    ,   ecommerce.total_item_quantity                           as ecommerce_total_item_quantity
    ,   ecommerce.purchase_revenue_in_usd                       as ecommerce_purchase_revenue_in_usd
    ,   ecommerce.purchase_revenue                              as ecommerce_purchase_revenue
    ,   ecommerce.refund_value_in_usd                           as ecommerce_refund_value_in_usd
    ,   ecommerce.refund_value                                  as ecommerce_refund_value
    ,   ecommerce.shipping_value_in_usd                         as ecommerce_shipping_value_in_usd
    ,   ecommerce.shipping_value                                as ecommerce_shipping_value
    ,   ecommerce.tax_value_in_usd                              as ecommerce_tax_value_in_usd
    ,   ecommerce.tax_value                                     as ecommerce_tax_value
    ,   ecommerce.unique_items                                  as ecommerce_unique_items
    ,   ecommerce.transaction_id                                as ecommerce_transaction_id
    ,   collected_traffic_source.manual_campaign_id             as collected_traffic_manual_campaign_id
    ,   collected_traffic_source.manual_campaign_name           as collected_traffic_manual_campaign_name
    ,   collected_traffic_source.manual_source                  as collected_traffic_manual_source
    ,   collected_traffic_source.manual_medium                  as collected_traffic_manual_medium
    ,   collected_traffic_source.manual_term                    as collected_traffic_manual_term
    ,   collected_traffic_source.manual_content                 as collected_traffic_manual_content
    ,   collected_traffic_source.manual_source_platform         as collected_traffic_manual_source_platform
    ,   collected_traffic_source.manual_creative_format         as collected_traffic_manual_creative_format
    ,   collected_traffic_source.manual_marketing_tactic        as collected_traffic_manual_marketing_tactic
    ,   collected_traffic_source.gclid                          as collected_traffic_gclid
    ,   collected_traffic_source.dclid                          as collected_traffic_dclid
    ,   collected_traffic_source.srsltid                        as collected_traffic_srsltid
    ,   is_active_user                                          as is_active_user
    ,   batch_event_index                                       as batch_event_index
    ,   batch_page_id                                           as batch_page_id
    ,   batch_ordering_id                                       as batch_ordering_id
    ,   session_traffic_source_last_click['manual_campaign'].campaign_id       as session_last_click_manual_campaign_campaign_id
    ,   session_traffic_source_last_click['manual_campaign'].campaign_name     as session_last_click_manual_campaign_campaign_name
    ,   session_traffic_source_last_click['manual_campaign'].source            as session_last_click_manual_campaign_source
    ,   session_traffic_source_last_click['manual_campaign'].medium            as session_last_click_manual_campaign_medium
    ,   session_traffic_source_last_click['manual_campaign'].term              as session_last_click_manual_campaign_term
    ,   session_traffic_source_last_click['manual_campaign'].content           as session_last_click_manual_campaign_content
    ,   session_traffic_source_last_click['manual_campaign'].source_platform   as session_last_click_manual_campaign_source_platform
    ,   session_traffic_source_last_click['manual_campaign'].creative_format   as session_last_click_manual_campaign_creative_format
    ,   session_traffic_source_last_click['manual_campaign'].marketing_tactic  as session_last_click_manual_campaign_marketing_tactic
    ,   session_traffic_source_last_click['google_ads_campaign'].customer_id   as session_traffic_google_ads_campaign_customer_id
    ,   session_traffic_source_last_click['google_ads_campaign'].account_name  as session_traffic_google_ads_campaign_account_name
    ,   session_traffic_source_last_click['google_ads_campaign'].campaign_id   as session_traffic_google_ads_campaign_campaign_id
    ,   session_traffic_source_last_click['google_ads_campaign'].campaign_name as session_traffic_google_ads_campaign_campaign_name
    ,   session_traffic_source_last_click['google_ads_campaign'].ad_group_id   as session_traffic_google_ads_campaign_ad_group_id
    ,   session_traffic_source_last_click['google_ads_campaign'].ad_group_name as session_traffic_google_ads_campaign_ad_group_name
    ,   schema
    ,   source_system
    ,   original_source
    FROM tdp_bronze_ga4_bigquery_events
    """
    
    transformed_df = glue_context.spark_session.sql(query)
    logger.info(f"Number of records after transformation: {transformed_df.count()}")
    return DynamicFrame.fromDF(transformed_df, glue_context, "transformed_dyf")
    
def apply_backfill_to_user_id(df):
    logger.info(f"Applying backfill logic to user_id records")
    window_spec = Window.partitionBy("user_pseudo_id").orderBy("event_timestamp")
    return df.withColumn(
        "event_user_id",
        when(col("user_id").isNotNull(), col("user_id"))
        .otherwise(
            coalesce(
                last("user_id", ignorenulls=True).over(window_spec.rowsBetween(Window.unboundedPreceding, 0)),
                first("user_id", ignorenulls=True).over(window_spec.rowsBetween(0, Window.unboundedFollowing))
            )
        )
    )

def add_partition_columns(df):
    logger.info("Adding partition columns")
    return (df
        .withColumn("created_dt", current_timestamp())
        .withColumn("year", year(to_date(df.event_date, 'yyyyMMdd')).cast(StringType()))
        .withColumn("month", format_string("%02d", month(to_date(df.event_date, 'yyyyMMdd'))))
        .withColumn("day", format_string("%02d", dayofmonth(to_date(df.event_date, 'yyyyMMdd'))))
    )

def create_composite_key(df):
    logger.info("Creating composite key")
    return df.withColumn(
        'unique_record_hash',
        sha2(
            concat(
                col('event_timestamp'),
                lit('_'),
                col('event_name'),
                lit('_'),
                col('user_pseudo_id'),
                lit('_'),
                coalesce(col('user_id'), lit('UNKNOWN')),
                lit('_'),
                coalesce(col('event_ga_session_id'), lit('UNKNOWN')),
                lit('_'),
                coalesce(col('event_batch_ordering_id'), lit('UNKNOWN')),
                lit('_'),
                coalesce(col('event_batch_page_id'), lit('UNKNOWN')),
                lit('_'),
                coalesce(col('event_navigation_type'), lit('UNKNOWN')),
                lit('_'),
                coalesce(col('event_link_url'), lit('UNKNOWN')),
                lit('_'),
                coalesce(col('event_link_text_str'), col('event_link_text_int'), lit('UNKNOWN')),
                lit('_'),
                coalesce(col('event_engagement_time_msec'), lit('UNKNOWN')),
                lit('_'),
                coalesce(col('event_session_engaged_int'), col('event_session_engaged_str'), lit('UNKNOWN')),
                lit('_'),
                coalesce(col('event_page_title'), lit('UNKNOWN'))
            ),
            256
        )
    )
    
def get_hudi_options():
    return {
        'className'                                             : 'org.apache.hudi',
        'hoodie.table.name'                                     : 'ga4_bigquery_events_analytics_408060011',
        'hoodie.database.name'                                  : 'tdp_silver',
        'hoodie.consistency.check.enabled'                      : 'true',
        'hoodie.datasource.hive_sync.enable'                    : 'true',
        'hoodie.datasource.hive_sync.table'                     : 'ga4_bigquery_events_analytics_408060011',
        'hoodie.datasource.hive_sync.database'                  : 'tdp_silver',
        'hoodie.datasource.hive_sync.partition_fields'          : 'year,month,day',
        'hoodie.datasource.hive_sync.use_jdbc'                  : 'false',
        'hoodie.datasource.hive_sync.mode'                      : 'hms',
        'hive.metastore.schema.verification'                    : 'false',
        
        'hoodie.datasource.write.operation'                     : 'bulk_insert', 
        'hoodie.datasource.write.table.type'                    : 'COPY_ON_WRITE',
        'hoodie.datasource.write.recordkey.field'               : 'unique_record_hash', 
        'hoodie.datasource.write.precombine.field'              : 'created_dt',
        
        'hoodie.datasource.write.partitionpath.field'           : 'year,month,day',
        'hoodie.datasource.write.hive_style_partitioning'       : 'true', 
        'hoodie.datasource.hive_sync.partition_extractor_class' : 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
        'hoodie.parquet.compression.codec'                      : 'snappy',
        
        'hoodie.bulkinsert.shuffle.parallelism'                 : 80,
        'hoodie.upsert.shuffle.parallelism'                     : 20,
        'hoodie.insert.shuffle.parallelism'                     : 20,
    }

def write_to_hudi(df, output_path, hudi_options):
    logger.info(f"Writing data to Hudi: {output_path}")
    df.write.format('hudi').options(**hudi_options).mode("append").save(output_path)

def main():
    # init
    logger.info("Starting Glue")
    spark = create_spark_session()
    glue_context, job = init_job(spark)
    args = get_job_args()
    job.init(args['JOB_NAME'], args)
    
    # setup 
    date_info = get_date_info(args['last_run_dt'])
    
    # extract 
    source_dyf = read_source_data(spark, glue_context, date_info)

    # transform
    transformed_dyf = transform_data(glue_context, source_dyf)
    transformed_df = transformed_dyf.toDF()
    df_with_user_id_backfill = apply_backfill_to_user_id(transformed_df)
    df_with_partitions = add_partition_columns(df_with_user_id_backfill)
    df_with_composite_key = create_composite_key(df_with_partitions)
    
    # load
    hudi_options = get_hudi_options()
    output_path = 's3://cloudformation-glue-test/silver/ga4-bigquery/events/analytics_408060011/'
    write_to_hudi(df_with_composite_key, output_path, hudi_options)
    
    # end
    job.commit()
    logger.info("Glue job completed successfully")

if __name__ == "__main__":
    main()