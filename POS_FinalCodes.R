options(java.parameters = "-Xmx64048m") # 64048 is 64 GB
#install.packages("odbc")
#install.packages("RMariaDB")
library(RMariaDB)
# Connect to a MariaDB version of a MySQL database
con <- dbConnect(RMariaDB::MariaDB(), host="datamine.rcac.purdue.edu", port=3306
                 , dbname="*********"
                 , user="vb_user", password="Fashion2020")
# list of db tables
dbListTables(con)

transactional_web_data <- dbGetQuery(con, "select * from transactional_web_data")

web <- transactional_web_data

web$DIM_ORDER_ENTRY_METHOD_KEY <- as.factor(web$DIM_ORDER_ENTRY_METHOD_KEY)
web$ORDER_NUMBER <- as.factor(web$ORDER_NUMBER)
web$DIM_SKU_KEY <- as.factor(web$DIM_SKU_KEY)
web$ORDER_DATE <- as.Date(web$ORDER_DATE, '%Y-%m-%d')
web$ORDER_LINE_NUMBER <- as.numeric(web$ORDER_LINE_NUMBER)

class(web$WEB_DISCOUNT_AMOUNT)

library(sqldf)

#Pulling required columns from the web transactions table

web_21 <- sqldf('select 
                  DIM_SKU_KEY,
                  ORDER_DATE,
                  ORDER_NUMBER,
                  ORDER_LINE_NUMBER,
                  AMOUNT,
                  STANDARD_COST_AT_TRANSACTION,
                  RETAIL_PRICE_AT_TRANSACTION,
                  CONFIRMED_QUANTITY_BASE_UNIT,
                  WEB_DISCOUNT_AMOUNT
                from web 
                where DIM_ORDER_ENTRY_METHOD_KEY = 21')

#Removing transactions with negative quantity and amount
negative_amount_qty_order <- sqldf('select 
          order_number 
        from 
          (select
            order_number,
            sum(amount) 
          from web_21 
          group by order_number 
          having sum(amount) < 0) 
      union
      select 
          order_number 
      from 
          (select
            order_number,
            sum(CONFIRMED_QUANTITY_BASE_UNIT) 
          from web_21 
          group by order_number 
          having sum(CONFIRMED_QUANTITY_BASE_UNIT) < 0) ')

negative_amount_qty_order$order_number <- as.factor(negative_amount_qty_order$order_number)

web_21_pos_amt <- sqldf('select A.*, B.order_number as order_number_neg from web_21 as A left join negative_amount_qty_order as B on A.order_number = B.order_number where B.order_number is  NULL')

#sqldf('select count(distinct order_number) from web_21')
#sqldf('select count(distinct order_number) from negative_amount_qty_order')
#sqldf('select count(distinct order_number) from web_21_pos_amt')

Transaction_POS_Data <- dbGetQuery(con, "select 
                                    DIM_STORE_KEY,
                                   DIM_SKU_KEY,
                                   SALE_DATE,
                                   SALE_QUANTITY, 
                                   STANDARD_COST_AT_TRANSACTION,
                                   RETAIL_PRICE_AT_TRANSACTION, 
                                   NET_SALE_AMOUNT, 
                                   SALE_DISCOUNT_AMOUNT, 
                                   MISC_DISCOUNT_AMOUNT, 
                                   TAX_AMOUNT, 
                                   POS_SLIPKEY_SLIP_LVL, 
                                   POS_SLIPKEY_LINE_LVL  
                                   from Transaction_POS_Data")

pos <- Transaction_POS_Data

#sqldf('select count(distinct POS_SLIPKEY_SLIP_LVL) from pos')
#sqldf('select count(*) from (select POS_SLIPKEY_SLIP_LVL,sum(NET_SALE_AMOUNT) from pos group by POS_SLIPKEY_SLIP_LVL having sum(NET_SALE_AMOUNT) < 0) ')
#sqldf('select count(*) from (select POS_SLIPKEY_SLIP_LVL,sum(SALE_QUANTITY) from pos group by POS_SLIPKEY_SLIP_LVL having sum(SALE_QUANTITY) < 0) ')

#Converting to correct data types
pos$DIM_STORE_KEY <- as.factor(pos$DIM_STORE_KEY)
pos$DIM_SKU_KEY <- as.factor(pos$DIM_SKU_KEY)
pos$SALE_DATE <- as.Date(pos$SALE_DATE, '%Y-%m-%d')

str(pos)

#Identifying distinct order numbers where either the amount or quantity is negative @order-level
library('sqldf')

#Removing transactions with negative quantity and amount
negative_amount_qty_order_pos <- sqldf('select 
          POS_SLIPKEY_SLIP_LVL 
        from 
          (select
            POS_SLIPKEY_SLIP_LVL,
            sum(NET_SALE_AMOUNT) 
          from pos 
          group by POS_SLIPKEY_SLIP_LVL 
          having sum(NET_SALE_AMOUNT) < 0) 
      union
      select 
          POS_SLIPKEY_SLIP_LVL 
      from 
          (select
            POS_SLIPKEY_SLIP_LVL,
            sum(SALE_QUANTITY) 
          from pos 
          group by POS_SLIPKEY_SLIP_LVL 
          having sum(SALE_QUANTITY) < 0) ')

negative_amount_qty_order_pos$POS_SLIPKEY_SLIP_LVL <- as.factor(negative_amount_qty_order_pos$POS_SLIPKEY_SLIP_LVL)

str(negative_amount_qty_order_pos)

#Joining back to the pos data and removing orders having negative amount/quantity
pos_positive_amt <- sqldf('select 
                              A.*, 
                              B.POS_SLIPKEY_SLIP_LVL as order_number_neg 
                            from pos as A 
                            left join negative_amount_qty_order_pos as B 
                            on A.POS_SLIPKEY_SLIP_LVL = B.POS_SLIPKEY_SLIP_LVL 
                            where B.POS_SLIPKEY_SLIP_LVL is NULL')

#sqldf('select count(distinct POS_SLIPKEY_SLIP_LVL) from pos')
#sqldf('select count(distinct POS_SLIPKEY_SLIP_LVL) from negative_amount_qty_order_pos')
#sqldf('select count(distinct POS_SLIPKEY_SLIP_LVL) from pos_positive_amt')

dbWriteTable(con,name='pos_line_positive_amt',value=pos_positive_amt,row.names=FALSE)

#*==========================================================================================*#

# SKU Table		
skus <- dbGetQuery(con, "select DIM_SKU_KEY, STYLE_ID, STYLE_DESCRIPTION,COLOR_ID,COLOR_DESCRIPTION, LAUNCH_DATE, 		
                          RELEASE_SEASON_ID,	RETIREMENT_DATE,	RETIREMENT_SEASON_ID, MERCHANT_DEPARTMENT,
                          MERCHANT_CLASS, PLM_PRIMARY_FABRICATION, WEB_SKU_ACTIVE_YN, PLM_COLOR_FAMILY,		
                          PLM_COLOR_SEASON		
                          from SKUs")		


str(skus)		

# Changing the datatypes of variables		
skus$STYLE_ID <- as.factor(skus$STYLE_ID)		
skus$STYLE_DESCRIPTION <- as.factor(skus$STYLE_DESCRIPTION)		
skus$STYLE_ID <- as.factor(skus$STYLE_ID)		
skus$STYLE_DESCRIPTION <- as.factor(skus$STYLE_DESCRIPTION)		
skus$COLOR_DESCRIPTION <- as.factor(skus$COLOR_DESCRIPTION)		
skus$COLOR_ID <- as.factor(skus$COLOR_ID)		
skus$MERCHANT_CLASS <- as.factor(skus$MERCHANT_CLASS)		
skus$MERCHANT_DEPARTMENT <- as.factor(skus$MERCHANT_DEPARTMENT)		
skus$WEB_SKU_ACTIVE_YN <- as.factor(skus$WEB_SKU_ACTIVE_YN)		
skus$PLM_COLOR_FAMILY <- as.factor(skus$PLM_COLOR_FAMILY)		
skus$PLM_COLOR_SEASON <- as.factor(skus$PLM_COLOR_SEASON)		
skus$PLM_PRIMARY_FABRICATION <- as.factor(skus$PLM_PRIMARY_FABRICATION)		
skus$RETIREMENT_DATE <- as.Date(skus$RETIREMENT_DATE, '%Y-%m-%d')		
skus$LAUNCH_DATE <- as.Date(skus$LAUNCH_DATE , '%Y-%m-%d')		
skus$RETIREMENT_SEASON_ID <- as.factor(skus$RETIREMENT_SEASON_ID)		
skus$RELEASE_SEASON_ID <- as.factor(skus$RELEASE_SEASON_ID)		
skus$DIM_SKU_KEY <- as.character(skus$DIM_SKU_KEY)		

# TRANSACTIONAL_WEB_DATA		
# importing only the required columns		
transactional_web_data <- dbGetQuery(con, "select ORDER_NUMBER ,		
                                                  ORDER_LINE_NUMBER ,		
                                                  ORDER_DATE ,		
                                                  DIM_SKU_KEY,		
                                                  DIM_ORDER_ENTRY_METHOD_KEY ,		
                                                  AMOUNT ,		
                                                  STANDARD_COST_AT_TRANSACTION ,RETAIL_PRICE_AT_TRANSACTION ,		
                                                  ORDERED_QUANTITY_BASE_UNIT,		
                                                  CONFIRMED_QUANTITY_BASE_UNIT  ,		
                                                  WEB_RETAIL_PRICE_AT_TRANSACTION ,		
                                                  WEB_RETAIL_FULL_PRICE_AT_TRANSACTION ,		
                                                  WEB_DISCOUNT_AMOUNT ,		
                                                  WEB_ORDERED_QUANTITY_BASE_UNIT		
                                                  from transactional_web_data")

str(transactional_web_data)		

# Changing the datatypes of variables		
transactional_web_data$ORDER_DATE <- as.Date(transactional_web_data$ORDER_DATE, '%Y-%m-%d')		
transactional_web_data$DIM_SKU_KEY <- as.character(transactional_web_data$DIM_SKU_KEY)		

library(sqldf)		

# subsetting only for the available SKUs in TRANSACTIONAL_WEB_DATA		
skus_1 <- sqldf("select * from skus where DIM_SKU_KEY in (select distinct DIM_SKU_KEY from transactional_web_data)")		

# TRANSACTIONAL_POS_DATA		
# selecting only the required columns
transaction_pos_data <- dbGetQuery(con, "select POS_SLIPKEY_SLIP_LVL, 		
                                        POS_SLIPKEY_LINE_LVL ,		
                                        DIM_STORE_KEY ,		
                                        DIM_SKU_KEY ,		
                                        SALE_DATE ,		
                                        SALE_QUANTITY ,		
                                        STANDARD_COST_AT_TRANSACTION,		
                                        RETAIL_PRICE_AT_TRANSACTION ,		
                                        NET_SALE_AMOUNT ,		
                                        SALE_DISCOUNT_AMOUNT, 		
                                        MISC_DISCOUNT_AMOUNT,		
                                        TAX_AMOUNT from Transaction_POS_Data")		

# # filtering only for the available SKUs in both WEB and POS Data		
skus_2 <- sqldf("select * from skus where DIM_SKU_KEY in (select distinct DIM_SKU_KEY from transactional_web_data 		
                                                          union all		
                                                          select distinct DIM_SKU_KEY from transaction_pos_data)")		

#dbWriteTable(conn = con , name = 'skus_filtered' , value = skus_2, row.names = FALSE)

# Changing the datatypes of variables		
skus_2$RELEASE_SEASON_ID <- as.factor(skus_2$RELEASE_SEASON_ID)		
skus_2$RETIREMENT_SEASON_ID <- as.factor(skus_2$RETIREMENT_SEASON_ID)		
skus_2$MERCHANT_DEPARTMENT <- as.factor(skus_2$MERCHANT_DEPARTMENT)		
skus_2$MERCHANT_CLASS <- as.factor(skus_2$MERCHANT_CLASS)		
skus_2$PLM_PRIMARY_FABRICATION <- as.factor(skus_2$PLM_PRIMARY_FABRICATION)		
skus_2$WEB_SKU_ACTIVE_YN <- as.factor(skus_2$WEB_SKU_ACTIVE_YN)		
skus_2$PLM_COLOR_FAMILY <- as.factor(skus_2$PLM_COLOR_FAMILY)		
skus_2$PLM_COLOR_SEASON <- as.factor(skus_2$PLM_COLOR_SEASON)		

str(skus_2)

# using the jeffrey table for aggregating web_data
web <- dbGetQuery(con, "select * from web_line_positive_amt")
skus_filtered <- dbGetQuery(con, "select * from skus_filtered")

library(sqldf)

agg_transaction_web_date <- sqldf("select A.DIM_SKU_KEY		
                                    , ORDER_DATE		
                                    , CASE WHEN C.launch_date is null or C.launch_date > B.min_order_date then B.min_order_date else C.launch_date end as 'min_launch_date'
                                    , CASE WHEN WEB_DISCOUNT_AMOUNT is null or WEB_DISCOUNT_AMOUNT = 0 then 0 else 1 END as WEB_DISCOUNT_FLAG		
                                    , sum(AMOUNT) as totalsales		
                                    , count(distinct ORDER_NUMBER) as trans		
                                    , SUM(CONFIRMED_QUANTITY_BASE_UNIT) as units		
                                    , sum(AMOUNT-standard_cost_at_transaction) as margins		
                                    , sum(RETAIL_PRICE_AT_TRANSACTION*CONFIRMED_QUANTITY_BASE_UNIT) as price		
                                    from web A		
                                    join 		
                                    (		
                                      select DIM_SKU_KEY		
                                      , MIN(ORDER_DATE) as min_order_date		
                                      from web			
                                      group by DIM_SKU_KEY		
                                    ) B on B.DIM_SKU_KEY = A.DIM_SKU_KEY
                                    left join 
                                    (
                                      select DIM_SKU_KEY,
                                      launch_date
                                      from skus_filtered
                                    ) C on C.DIM_SKU_KEY = B.DIM_SKU_KEY
                                    group by A.DIM_SKU_KEY		
                                    , ORDER_DATE		
                                    , CASE WHEN C.launch_date is null or C.launch_date > B.min_order_date then B.min_order_date else C.launch_date end		
                                    , CASE WHEN WEB_DISCOUNT_AMOUNT is null or WEB_DISCOUNT_AMOUNT = 0 then 0 else 1 END 		
                                    ")		

str(agg_transaction_web_date)		

# changing date variable to date format
agg_transaction_web_date$min_launch_date <- as.Date(agg_transaction_web_date$min_launch_date, origin = "1970-01-01") 		

# Date difference 		
agg_transaction_web_date$datediff <- as.numeric(agg_transaction_web_date$ORDER_DATE-agg_transaction_web_date$min_launch_date)		

# binning the sales into different weeks, months		
# set up cut-off values 		
breaks <- c(0,7,14,21,28,60,90,120,150,180,Inf)		
# specify interval/bin labels		
tags <- c("1W","2W", "3W", "4W", "2M", "3M","4M", "5M","6M", ">6M")		

# bucketing values into bins		
group_tags <- cut(agg_transaction_web_date$datediff, 		
                  breaks=breaks, 		
                  include.lowest=TRUE, 		
                  right=FALSE, 		
                  labels=tags)		

agg_transaction_web_date <- cbind(agg_transaction_web_date , group_tags)		

agg_1 <- sqldf("select DIM_SKU_KEY		
                      , min_launch_date
                      , WEB_DISCOUNT_FLAG
                      , group_tags
                      , sum(totalsales) as totalsales		
                      , sum(trans) as trans		
                      , SUM(units) as units		
                      , sum(margins) as margins		
                      , sum(price) as price		
                      from agg_transaction_web_date
                      group by 
                      DIM_SKU_KEY		
                      , min_launch_date
                      , WEB_DISCOUNT_FLAG
                      , group_tags")

# joining sku table to get the features 
agg_web_final <- sqldf("select A.*
                         , B.STYLE_ID
                         , B.STYLE_DESCRIPTION
                         , B.COLOR_DESCRIPTION
                         , B.RELEASE_SEASON_ID
                         , B.RETIREMENT_SEASON_ID
                         , B.RETIREMENT_DATE
                         , B.MERCHANT_DEPARTMENT
                         , B.PLM_PRIMARY_FABRICATION
                         , B.PLM_COLOR_FAMILY
                         , B.PLM_COLOR_SEASON
                         from agg_1 A
                         left join skus_filtered B on B.DIM_SKU_KEY = A.DIM_SKU_KEY
                         WHERE B.MERCHANT_DEPARTMENT != 'Marketing' ")

#dbWriteTable(conn = con , name = 'agg_web_sku_final' , value = agg_web_final, row.names = FALSE)

#############################Transaction_POS_DATA###################################		

pos <- dbGetQuery(con, "select * from pos_line_positive_amt")

# logic for using min(order_date) if launch_date is null
launch_date <- sqldf("		
                      select A.DIM_SKU_KEY,		
                      CASE when C.LAUNCH_DATE is NULL or C.LAUNCH_DATE > A.min_order_date then min_order_date else C.LAUNCH_DATE end as 'LaunchDate'		
                      from 		
                      (		
                      select DIM_SKU_KEY		
                      , MIN(SALE_DATE) as min_order_date		
                      from pos 		
                      group by DIM_SKU_KEY 		
                      ) A		
                      left join 		
                      (		
                        select DIM_SKU_KEY,		
                        LAUNCH_DATE		
                        from skus_filtered		
                      ) C ON C.DIM_SKU_KEY = A.DIM_SKU_KEY")		


launch_date$DIM_SKU_KEY <- as.character(launch_date$DIM_SKU_KEY)		
launch_date$LaunchDate <- as.Date(launch_date$LaunchDate, origin = "1970-01-01")		


# Aggregating POS Data at SKU and Order_date level		
agg_pos_date <- sqldf("select      A.DIM_SKU_KEY 		
                                    , A.DIM_STORE_KEY  		
                                    , SALE_DATE		
                                    , CASE WHEN SALE_DISCOUNT_AMOUNT + MISC_DISCOUNT_AMOUNT > 0  then 1 else 0 END as Discount_FLag		
                                    , sum(NET_SALE_AMOUNT) as totalsales		
                                    , count(distinct POS_SLIPKEY_SLIP_LVL) as trans		
                                    , SUM(SALE_QUANTITY) as units		
                                    , sum(NET_SALE_AMOUNT-STANDARD_COST_AT_TRANSACTION) as margins		
                                    , sum(RETAIL_PRICE_AT_TRANSACTION*SALE_QUANTITY) as price		
                                    from pos A		
                                    group by 		
                                      A.DIM_SKU_KEY		
                                    , A.DIM_STORE_KEY		
                                    , SALE_DATE		
                                    , CASE WHEN SALE_DISCOUNT_AMOUNT + MISC_DISCOUNT_AMOUNT > 0  then 1 else 0 END                                   		
                      ")		

agg_1 <- sqldf("select A.*, B.LaunchDate
               from agg_pos_date A
               left join launch_date B on B.DIM_SKU_KEY = A.DIM_SKU_KEY")

agg_1$datediff <- as.numeric(agg_1$SALE_DATE-agg_1$LaunchDate)		

# binning the datediff column
breaks <- c(0,7,14,21,28,60,90,120,150,180,Inf)		
# specify interval/bin labels		
tags <- c("1W","2W", "3W", "4W", "2M", "3M","4M", "5M","6M", ">6M")		

# bucketing values into bins		
group_tags <- cut(agg_1$datediff, 		
                  breaks=breaks, 		
                  include.lowest=TRUE, 		
                  right=FALSE, 		
                  labels=tags)		

agg_1 <- cbind(agg_1 , group_tags)

agg_2 <- sqldf("select DIM_SKU_KEY 		
                      , DIM_STORE_KEY
                      , LaunchDate
                      , Discount_Flag
                      , group_tags
                      , sum(totalsales) as totalsales
                      , sum(trans) as trans
                      , sum(units) as units
                      , sum(margins) as margins
                      , sum(price) as price
                      from agg_1
                      group by 
                      DIM_SKU_KEY 		
                      , DIM_STORE_KEY
                      , LaunchDate
                      , Discount_Flag
                      , group_tags ")


store <- dbGetQuery(con, "select * from store")
store$DIM_STORE_KEY <- as.character(store$DIM_STORE_KEY)

# joining SKU and store table to get the details of SKU and store
agg_pos_sku_store_final <- sqldf("select A.*  
                                , B.STYLE_ID
                                , B.STYLE_DESCRIPTION
                                , B.COLOR_DESCRIPTION
                                , B.RELEASE_SEASON_ID
                                , B.RETIREMENT_SEASON_ID
                                , B.RETIREMENT_DATE
                                , B.MERCHANT_DEPARTMENT
                                , B.PLM_PRIMARY_FABRICATION
                                , B.PLM_COLOR_FAMILY
                                , B.PLM_COLOR_SEASON
                                , C.DISTRICT_ID
                                , C.DISTRICT_DESCRIPTION
                                , C.ADDRESS_LATITUDE
                                , C.ADDRESS_LONGITUDE
                                , C.ACTIVE_YN
                                , C.RETAIL_SQUARE_FOOTAGE
                                from agg_2 A
                                left join skus_filtered B on B.DIM_SKU_KEY = A.DIM_SKU_KEY
                                left join store C on C.DIM_STORE_KEY = A.DIM_STORE_KEY 
                                where C.STORE_TYPE_CHARACTERISTIC_DESCRIPTION in ('Full Line')
                                      ") 

store_1 <- subset(store, store$STORE_TYPE_CHARACTERISTIC_DESCRIPTION == 'Full line')

store$STORE_TYPE_CHARACTERISTIC_DESCRIPTION <- as.factor(store$STORE_TYPE_CHARACTERISTIC_DESCRIPTION)
table(store$STORE_TYPE_CHARACTERISTIC_DESCRIPTION)

agg_pos_sku_store_final$COLOR_DESCRIPTION <- as.factor(agg_pos_sku_store_final$COLOR_DESCRIPTION)
agg_pos_sku_store_final$MERCHANT_DEPARTMENT <- as.factor(agg_pos_sku_store_final$MERCHANT_DEPARTMENT)
table(agg_pos_sku_store_final$MERCHANT_DEPARTMENT)

#dbWriteTable(conn = con, name = 'agg_pos_sku_store_final', value = agg_pos_sku_store_final, row.names = FALSE)

#*==========================================================================================*#


pos_agg <- dbGetQuery(con, "Select * from agg_pos_sku_store_final")
web_agg <- dbGetQuery(con, "Select * from agg_web_sku_final")

#removing recrods from disney orlando store
pos_agg_disney_rem <- subset (pos_agg,DIM_SKU_KEY!= 179)

#filtering records made only
pos_agg_final <- sqldf(" select * from pos_agg_disney_rem where RELEASE_SEASON_ID in ('F17','F18','F19','M17','M18','M19','S17','S18','S19','W17','W18','W19') ")
web_agg_final <- sqldf(" select * from web_agg where RELEASE_SEASON_ID in ('F17','F18','F19','M17','M18','M19','S17','S18','S19','W17','W18','W19') ")


pos_agg_final$LaunchDate <- as.Date(pos_agg_final$LaunchDate, origin = "1970-01-01")
web_agg_final$min_launch_date <- as.Date(web_agg_final$min_launch_date, origin = "1970-01-01")


#sqldf("select group_tags,count(*) from web_agg_final group by group_tags")

pos_agg_date_final <- subset(pos_agg_final,LaunchDate >= "2017-02-01")
web_agg_date_final <- subset(web_agg_final,min_launch_date >= "2017-02-01")


#sqldf("select * from pos_agg_date_final where DIM_SKU_KEY in (select distinct DIM_SKU_KEY from pos_agg_date_final where group_tags not in ('1W') and group_tags in ('2W') ")
#sqldf("select distinct DIM_SKU_KEY from pos_agg_date_final where group_tags not in ('1W')  ")

web_pos_append_1wk <- sqldf("
      SELECT
        'WEB' AS CHANNEL,
        DIM_SKU_KEY,
        NULL AS DIM_STORE_KEY,        
        NULL AS DISTRICT_ID,
        NULL AS DISTRICT_DESCRIPTION,
        NULL AS ADDRESS_LATITUDE,
        NULL AS ADDRESS_LONGITUDE,
        NULL AS ACTIVE_YN,
        NULL AS RETAIL_SQUARE_FOOTAGE,
        min_launch_date as LAUNCH_DATE,
        WEB_DISCOUNT_FLAG as DISCOUNT_FLAG,
        group_tags,
        totalsales,
        trans,
        units,
        margins,
        price,
        STYLE_ID,
        STYLE_DESCRIPTION,
        COLOR_DESCRIPTION,
        RELEASE_SEASON_ID as LAUNCH_SEASON,
        RETIREMENT_SEASON_ID as RETIREMENT_SEASON,
        RETIREMENT_DATE,
        MERCHANT_DEPARTMENT
      FROM web_agg_date_final
      
      UNION
      
      SELECT
      'POS' AS CHANNEL,
      DIM_SKU_KEY,
      DIM_STORE_KEY,        
      DISTRICT_ID,
      DISTRICT_DESCRIPTION,
      ADDRESS_LATITUDE,
      ADDRESS_LONGITUDE,
      ACTIVE_YN,
      RETAIL_SQUARE_FOOTAGE,
      LaunchDate as LAUNCH_DATE,
      Discount_FLag as DISCOUNT_FLAG,
        group_tags,
        totalsales,
        trans,
        units,
        margins,
        price,
        STYLE_ID,
        STYLE_DESCRIPTION,
        COLOR_DESCRIPTION,
        RELEASE_SEASON_ID as LAUNCH_SEASON,
        RETIREMENT_SEASON_ID as RETIREMENT_SEASON,
        RETIREMENT_DATE,
        MERCHANT_DEPARTMENT
  from pos_agg_date_final
      ")

#dbRemoveTable(con,"web_pos_append_1wk")
#dbWriteTable(con,name='web_pos_append_1wk',value=web_pos_append_1wk,row.names=FALSE)

web_pos_append_1wk <- dbGetQuery(con,"select * from web_pos_append_1wk")


names(web_pos_append_1wk)

#nrow(web_pos_append_1wk)

library("sqldf")
web_pos_append_1wk <- sqldf ("select A.*,
                                      B.MERCHANT_CLASS
                             FROM web_pos_append_1wk AS A
                             LEFT JOIN skus  AS B
                             ON A.DIM_SKU_KEY = B.DIM_SKU_KEY
                             ")

top_pattern_sales <- read.csv("top_colorpatterns.csv",header = TRUE)
top_pattern_sales$pattern <- as.character(top_pattern_sales$pattern)

library("sqldf")
web_pos_append_top_pattern <- sqldf("select A.* from web_pos_append_1wk as A inner join top_pattern_sales as B  on A.COLOR_DESCRIPTION  = B.pattern")

web_pos_append_top_pattern <- sqldf(" select *,  CASE when LAUNCH_SEASON like 'M%' then 'Summer'
                           when LAUNCH_SEASON like 'S%' then 'Spring'
                           when LAUNCH_SEASON like 'W%' then 'Winter'
                           when LAUNCH_SEASON like 'F%' then 'Fall'
                      END as 'Season' from web_pos_append_top_pattern ")

#================================================================================#

#Pull data from the web and pos transactions filtered for the top 150 patterns
web_pos_append_top_pattern <- dbGetQuery(con,"select * from web_pos_append_top_pattern")

web_pos_append_top_pattern$DISTRICT_DESCRIPTION <- as.factor(web_pos_append_top_pattern$DISTRICT_DESCRIPTION)

#Removing Model Stores
web_pos_append_top_pattern <- subset (web_pos_append_top_pattern,web_pos_append_top_pattern$DISTRICT_DESCRIPTION != "Model Store")

store <- dbGetQuery(con,"select * from store")

#Converting columns to the relavant data types
web_pos_append_top_pattern$MERCHANT_CLASS <- as.character(web_pos_append_top_pattern$MERCHANT_CLASS)
web_pos_append_top_pattern$COLOR_DESCRIPTION <- as.factor(web_pos_append_top_pattern$COLOR_DESCRIPTION)
web_pos_append_top_pattern$STYLE_DESCRIPTION <- as.factor(web_pos_append_top_pattern$STYLE_DESCRIPTION)
web_pos_append_top_pattern$CHANNEL <- as.factor(web_pos_append_top_pattern$CHANNEL)

library("sqldf")

#Filtering only the top merchant classes transactions
web_pos_append_top_pattern_class <- sqldf("select * from web_pos_append_top_pattern where merchant_class in ('Crossbodies','Backpacks','Travel Bags','Totes','IDs/Keychains','Wristlets','Cosmetics','Travel/Packing Accessories','Textiles',
'Wallets','Lunch Bags','Satchels','Rolling Luggage','Laptop/Tablet Accessories','Baby Bags','Necklaces')")

web_pos_append_top_pattern_class$MERCHANT_CLASS <- as.factor(web_pos_append_top_pattern_class$MERCHANT_CLASS)

#Filtering only POS stores 
web_pos_append_top_pattern_class_pos <- subset (web_pos_append_top_pattern_class, CHANNEL == "POS")

#Removing unnecessary columns
web_pos_append_top_pattern_class_pos$ADDRESS_LATITUDE <- NULL
web_pos_append_top_pattern_class_pos$ADDRESS_LONGITUDE <- NULL
web_pos_append_top_pattern_class_pos$ACTIVE_YN <- NULL
web_pos_append_top_pattern_class_pos$RETAIL_SQUARE_FOOTAGE <- NULL
web_pos_append_top_pattern_class_pos$DISCOUNT_FLAG <- NULL
web_pos_append_top_pattern_class_pos$STYLE_ID <- NULL
web_pos_append_top_pattern_class_pos$RETIREMENT_DATE <- NULL
web_pos_append_top_pattern_class_pos$MERCHANT_DEPARTMENT <- NULL
web_pos_append_top_pattern_class_pos$CHANNEL <- NULL

web_pos_append_top_pattern_class_pos$group_tags <- as.factor(web_pos_append_top_pattern_class_pos$group_tags)

#pulling state ID from store table
web_pos_append_top_pattern_class_pos_state <- sqldf("select A.*, B.STATE_ID 
                                                    from web_pos_append_top_pattern_class_pos as A
                                                    left join store as B
                                                    on A.DIM_STORE_KEY = B.DIM_STORE_KEY")

#Converting columns to the relavant data types
web_pos_append_top_pattern_class_pos_state$STATE_ID <- as.factor(web_pos_append_top_pattern_class_pos_state$STATE_ID)
web_pos_append_top_pattern_class_pos_state$group_tags <- as.character(web_pos_append_top_pattern_class_pos_state$group_tags)

#Filtering metrics for the first 3 weeks only
web_pos_append_top_pattern_class_pos_state <- subset(web_pos_append_top_pattern_class_pos_state,group_tags == '1W' | group_tags == '2W' | group_tags == '3W')

web_pos_append_top_pattern_class_pos_state$group_tags <- as.factor(web_pos_append_top_pattern_class_pos_state$group_tags)

#Filtering recordw where Launch Date before Q4 2019 since these launches won't have many transaction details 
web_pos_append_top_pattern_class_pos_state <- subset(web_pos_append_top_pattern_class_pos_state, LAUNCH_DATE <= "2019-07-07")

web_pos_append_top_pattern_class_pos_state$STATE_ID <- as.factor(web_pos_append_top_pattern_class_pos_state$STATE_ID)

#Aggregating at pattern and region level
library("sqldf")
agg <- sqldf("select
             COLOR_DESCRIPTION,
             DISTRICT_DESCRIPTION,
             COUNT(DISTINCT MERCHANT_CLASS) AS COUNT_MERCHANT_CLASS ,
             COUNT(DISTINCT DIM_SKU_KEY) AS COUNT_SKU,
             COUNT(DISTINCT STYLE_DESCRIPTION) AS COUNT_STYLES,
             COUNT(DISTINCT DIM_STORE_KEY) AS COUNT_STORES,
             SUM(TOTALSALES) AS TOTALSALES,
             SUM(UNITS) AS UNITS,
             SUM(MARGINS) AS MARGINS,
             SUM(price) / SUM(UNITS) as AVG_PRICE                 
             FROM web_pos_append_top_pattern_class_pos_state
             where DISTRICT_DESCRIPTION in ('Mid Atlantic','Midwest','Northeast','West') 
             
             GROUP BY 
             DISTRICT_DESCRIPTION,
             COLOR_DESCRIPTION
             ")

color_description_count_metrics <- sqldf ("select 
        color_description,
        sum(COUNT_MERCHANT_CLASS) as COUNT_MERCHANT_CLASS,
        sum(COUNT_SKU) as COUNT_SKU, 
        sum(COUNT_STYLES) as COUNT_STYLES
       from agg 
       group by 
        color_description")

str(agg$DISTRICT_DESCRIPTION)
#agg$DISTRICT_DESCRIPTION <- as.factor(agg$DISTRICT_DESCRIPTION)

#Renaming mid atlantic without space
agg$DISTRICT_DESCRIPTION <- ifelse(agg$DISTRICT_DESCRIPTION == "Mid Atlantic","MidAtlantic",agg$DISTRICT_DESCRIPTION)

#Replace missing values by zeroes
library("dplyr")
agg <- agg %>%
  mutate(TOTALSALES = coalesce(TOTALSALES, 0),
         MARGINS = coalesce(MARGINS, 0),
         UNITS = coalesce(UNITS, 0),
         AVG_PRICE = coalesce(AVG_PRICE, 0))

agg$COUNT_MERCHANT_CLASS <- NULL
agg$COUNT_SKU <- NULL
agg$COUNT_STYLES <- NULL

library(tidyr)

#Transposing the table
agg_transpose <-  pivot_wider (data = agg, names_from=DISTRICT_DESCRIPTION, values_from = c("TOTALSALES","UNITS","MARGINS","AVG_PRICE","COUNT_STORES"))
agg_transpose <- as.data.frame(agg_transpose)

#Converting columns to the relavant data types
agg_transpose$COUNT_STORES_MidAtlantic <- as.numeric(agg_transpose$COUNT_STORES_MidAtlantic)
agg_transpose$COUNT_STORES_Midwest <- as.numeric(agg_transpose$COUNT_STORES_Midwest)
agg_transpose$COUNT_STORES_Northeast <- as.numeric(agg_transpose$COUNT_STORES_Northeast)
agg_transpose$COUNT_STORES_West <- as.numeric(agg_transpose$COUNT_STORES_West)

#Replace NA's with zeroes
library("dplyr")
agg_transpose <- agg_transpose %>%
  mutate(TOTALSALES_MidAtlantic = coalesce(TOTALSALES_MidAtlantic, 0),
         TOTALSALES_Midwest = coalesce(TOTALSALES_Midwest, 0),
         TOTALSALES_Northeast = coalesce(TOTALSALES_Northeast, 0),
         TOTALSALES_West = coalesce(TOTALSALES_West, 0),
         MARGINS_MidAtlantic = coalesce(MARGINS_MidAtlantic, 0),
         MARGINS_Midwest = coalesce(MARGINS_Midwest, 0),
         MARGINS_Northeast = coalesce(MARGINS_Northeast, 0),
         MARGINS_West = coalesce(MARGINS_West, 0),
         UNITS_MidAtlantic = coalesce(UNITS_MidAtlantic, 0),
         UNITS_Midwest = coalesce(UNITS_Midwest, 0),
         UNITS_Northeast = coalesce(UNITS_Northeast, 0),
         UNITS_West = coalesce(UNITS_West, 0),
         AVG_PRICE_MidAtlantic = coalesce(AVG_PRICE_MidAtlantic, 0),
         AVG_PRICE_Midwest = coalesce(AVG_PRICE_Midwest, 0),
         AVG_PRICE_Northeast = coalesce(AVG_PRICE_Northeast, 0),
         AVG_PRICE_West = coalesce(AVG_PRICE_West, 0),
         COUNT_STORES_MidAtlantic = coalesce(COUNT_STORES_MidAtlantic, 0),
         COUNT_STORES_Midwest = coalesce(COUNT_STORES_Midwest, 0),
         COUNT_STORES_Northeast = coalesce(COUNT_STORES_Northeast, 0),
         COUNT_STORES_West = coalesce(COUNT_STORES_West, 0))

agg_transpose_final <- sqldf ("
          SELECT
            A.*,
            B.COUNT_MERCHANT_CLASS,
            B.COUNT_SKU,
            B.COUNT_STYLES
          from agg_transpose AS A
          join color_description_count_metrics as B
              ON A.COLOR_DESCRIPTION = B.COLOR_DESCRIPTION")

agg_transpose_final$COLOR_DESCRIPTION <- as.character(agg_transpose_final$COLOR_DESCRIPTION)

#Remove black patterns from the table
agg_transpose_final <- subset (agg_transpose_final,COLOR_DESCRIPTION != "Black")
agg_transpose_final <- subset (agg_transpose_final,COLOR_DESCRIPTION != "Classic Black")

subset(agg_transpose_final,COLOR_DESCRIPTION == "Classic Black")

#####################################################################################

# Taking the required numeric columns for clustering 
df <- agg_transpose_final[,2:ncol(agg_transpose_final)]

# z-score standardize for these variable
dfz <- scale(df)
dfz <- data.frame(scale(df))

cost_df <- data.frame() #accumulator for cost results
cost_df

for(k in 1:15){
  # allow up to 50 iterations to obtain convergence, and do 20 random starts
  kmeans_tr <- kmeans(x=dfz, centers=k, nstart=100, iter.max=100)
  #Combine cluster number and cost together, write to cost_df
  cost_df <- rbind(cost_df, cbind(k, kmeans_tr$tot.withinss))
}

# the cost_df data.frame contains the # of clusters k and the Mean Squared Error
# (MSE) for each cluster
names(cost_df) <- c("cluster", "tr_cost")
cost_df

# create an elbow plot
par(mfrow=c(1,1))
cost_df[,2] <- cost_df[,2]/1000
plot(x=cost_df$cluster, y=cost_df$tr_cost, main="k-Means Elbow Plot"
     , col="blue", pch=19, type="b", cex.lab=1.2
     , xlab="Number of Clusters", ylab="MSE (in 1000s)")
points(x=cost_df$cluster, y=cost_df$te_cost, col="green")

library(cluster)

km3 <- kmeans(x=dfz, centers=4, nstart=50, iter.max=100)
dist3 <- dist(dfz, method="euclidean")
sil3 <- silhouette(km3$cluster, dist3)
plot(sil3, col=c("black","red","green","blue"), main="Silhouette plot (k=4) K-means (withoutseasons)", border=NA)

#library(factoextra)
#fviz_cluster(kmeans_tr, data = df)
#rm(agg_transpose_final_cluster)
agg_transpose_final_cluster <- cbind (agg_transpose_final,km3$cluster )

#dbRemoveTable(con,"agg_transpose_final_cluster")
#write.csv(agg_transpose_final_cluster,"agg_transpose_final_cluster.csv")
#dbWriteTable(con,"agg_transpose_final_cluster",agg_transpose_final_cluster)

head(agg_transpose_final)

colnames(agg_transpose_final_cluster)[25] <- "cluster"

###########################################################################################################

#Aggregating at region, merchant class, pattern, week number level (include cluster of the pattern as well)
model_agg <- sqldf("select
      DISTRICT_DESCRIPTION,
      A.MERCHANT_CLASS,
      A.COLOR_DESCRIPTION,
      B.cluster,
      GROUP_TAGS,
      MERCHANT_CLASS,
      SUM(TOTALSALES) AS TOTALSALES,
      SUM(UNITS) AS UNITS,
      SUM(MARGINS) AS MARGINS,
      SUM(price) / SUM(UNITS) as AVG_PRICE,
      COUNT(DISTINCT STYLE_DESCRIPTION) AS COUNT_STYLES,
      COUNT(DISTINCT DIM_STORE_KEY) AS COUNT_STORES
      
      FROM web_pos_append_top_pattern_class_pos AS A 
      LEFT JOIN agg_transpose_final_cluster AS B
      ON A.COLOR_DESCRIPTION = B.COLOR_DESCRIPTION
      
      GROUP BY 
      
      DISTRICT_DESCRIPTION,
      A.COLOR_DESCRIPTION,
      GROUP_TAGS,
      B.cluster,
      MERCHANT_CLASS
            ")

#Converting columns to the relavant data types
model_agg$cluster <- as.factor(model_agg$cluster)
model_agg$MERCHANT_CLASS <- as.factor(model_agg$MERCHANT_CLASS)

library(tidyr)
#pivoting the dataset to put the features as columns

model_agg_pivot <-  pivot_wider (data = model_agg, names_from=group_tags, values_from = c("TOTALSALES","UNITS","MARGINS","AVG_PRICE","COUNT_STYLES","COUNT_STORES"))

#Converting above tibble back to dataframe
model_agg_pivot <- as.data.frame(model_agg_pivot)

#Converting columns to the relavant data typesmodel_agg_pivot$COUNT_STYLES_1W <- as.numeric(model_agg_pivot$COUNT_STYLES_1W)
model_agg_pivot$COUNT_STYLES_2W <- as.numeric(model_agg_pivot$COUNT_STYLES_2W)
model_agg_pivot$COUNT_STYLES_3W <- as.numeric(model_agg_pivot$COUNT_STYLES_3W)

model_agg_pivot$COUNT_STORES_1W <- as.numeric(model_agg_pivot$COUNT_STORES_1W)
model_agg_pivot$COUNT_STORES_2W <- as.numeric(model_agg_pivot$COUNT_STORES_2W)
model_agg_pivot$COUNT_STORES_3W <- as.numeric(model_agg_pivot$COUNT_STORES_3W)

library("dplyr")
model_agg_pivot <- model_agg_pivot %>%
  mutate(TOTALSALES_1W = coalesce(TOTALSALES_1W, 0),
         TOTALSALES_2W = coalesce(TOTALSALES_2W, 0),
         TOTALSALES_3W = coalesce(TOTALSALES_3W, 0),
         UNITS_1W = coalesce(UNITS_1W, 0),
         UNITS_2W = coalesce(UNITS_2W, 0),
         UNITS_3W = coalesce(UNITS_3W, 0),
         UNITS_4W = coalesce(UNITS_4W, 0),
         UNITS_2M = coalesce(UNITS_2M, 0),
         UNITS_3M = coalesce(UNITS_3M, 0),
         AVG_PRICE_1W = coalesce(AVG_PRICE_1W, 0),
         AVG_PRICE_2W = coalesce(AVG_PRICE_2W, 0),
         AVG_PRICE_3W = coalesce(AVG_PRICE_3W, 0),
         MARGINS_1W = coalesce(MARGINS_1W, 0),
         MARGINS_2W = coalesce(MARGINS_2W, 0),
         MARGINS_3W = coalesce(MARGINS_3W, 0),
         COUNT_STORES_1W = coalesce(COUNT_STORES_1W, 0),
         COUNT_STORES_2W = coalesce(COUNT_STORES_2W, 0),
         COUNT_STORES_3W = coalesce(COUNT_STORES_3W, 0),
         COUNT_STYLES_1W = coalesce(COUNT_STYLES_1W, 0),
         COUNT_STYLES_2W = coalesce(COUNT_STYLES_2W, 0),
         COUNT_STYLES_3W = coalesce(COUNT_STYLES_3W, 0))

#Creating cumulative features for the model
model_agg_pivot$CumulativeUnits <- model_agg_pivot$UNITS_4W + model_agg_pivot$UNITS_2M + model_agg_pivot$UNITS_3M
model_agg_pivot$CumulativeThreeWeekSales <- model_agg_pivot$TOTALSALES_1W + model_agg_pivot$TOTALSALES_2W + model_agg_pivot$TOTALSALES_3W
model_agg_pivot$CumulativeThreeWeekStyles <- model_agg_pivot$COUNT_STYLES_1W + model_agg_pivot$COUNT_STYLES_2W + model_agg_pivot$COUNT_STYLES_3W
model_agg_pivot$CumulativeThreeWeekStores <- model_agg_pivot$COUNT_STORES_1W + model_agg_pivot$COUNT_STORES_2W + model_agg_pivot$COUNT_STORES_3W
model_agg_pivot$CumulativeThreeWeekMargins <- model_agg_pivot$MARGINS_1W + model_agg_pivot$MARGINS_2W + model_agg_pivot$MARGINS_3W

#############################################################################

#Cannibalization features

class(web_pos_append_top_pattern$MERCHANT_CLASS)

web_pos_append_top_pattern$LAUNCH_DATE <- as.Date(web_pos_append_top_pattern$LAUNCH_DATE, origin = "1970-01-01") 

#Finding the launch date of pattern & merchant class combination. If a pattern and merchant class has multiple launch dates, pick the earliest launch date
can_1 <- sqldf("select 
      color_description, 
      merchant_class,
      min(launch_date) as launch_date 
      from web_pos_append_top_pattern
      where merchant_class in ('Crossbodies','Backpacks','Travel Bags','Totes','IDs/Keychains','Wristlets','Cosmetics','Travel/Packing Accessories','Textiles',
'Wallets','Lunch Bags','Satchels','Rolling Luggage','Laptop/Tablet Accessories','Baby Bags','Necklaces')
      group by 
        color_description,
        merchant_class")

can_1$launch_date <- as.Date(can_1$launch_date, origin = "1970-01-01") 

#write.csv(can_1,"can_1.csv")

#Self join on the above table to find the number of launches for the same merchant class but differnt pattern
can_2 <- sqldf("
select A.color_description, 
		A.merchant_class, 
		A.launch_date
          , count(distinct B.color_description) as existing_3M
          , count(distinct C.color_description) as existing_2M
          , count(distinct D.color_description) as existing_1M
           from can_1 A
           left join can_1 B on B.color_description != A.color_description
           and A.merchant_class = B.merchant_class
           and B.launch_date between A.launch_date -90 and A.launch_date - 60
            
           left join can_1 C on C.color_description != A.color_description
           and C.merchant_class = A.merchant_class
           and C.launch_date between A.launch_date -60 and A.launch_date -30
          
           left join can_1 D on D.color_description != A.color_description
           and D.merchant_class = A.merchant_class
           and D.launch_date between A.launch_date -30 and A.launch_date -1
          
           group by  A.color_description, A.merchant_class, A.launch_date
           ")

#joining back the cannibalization features to the model dataset
library("sqldf")
model_agg_pivot <- sqldf ("select A.*,
       B.existing_3M,
       B.existing_2M,
       B.existing_1M
       FROM model_agg_pivot AS A 
       LEFT JOIN can_2 as B
       on 
       A.COLOR_DESCRIPTION = B.COLOR_DESCRIPTION and
       A.MERCHANT_CLASS = b.MERCHANT_CLASS
       ")

#############################################################################

#Extracting the required features
mod1_data <- model_agg_pivot[,c("CumulativeUnits","CumulativeThreeWeekSales","UNITS_1W","UNITS_2W","UNITS_3W","AVG_PRICE_1W","AVG_PRICE_2W","AVG_PRICE_3W","DISTRICT_DESCRIPTION","COLOR_DESCRIPTION","cluster","MERCHANT_CLASS","CumulativeThreeWeekMargins","CumulativeThreeWeekStores","CumulativeThreeWeekStyles","existing_1M","existing_2M","existing_3M")]

model_agg_pivot$MERCHANT_CLASS <- as.factor(model_agg_pivot$MERCHANT_CLASS)

sapply(mod1_data, function(x) sum(is.na(x)))

library('caret')
library("scales")

#Filtering only for records with positive metrics
mod1_data_positive <- subset(mod1_data , UNITS_3W >= 0 & UNITS_2W >= 0 & UNITS_1W >= 0 & CumulativeThreeWeekSales >=0 & CumulativeUnits >=0 & CumulativeThreeWeekMargins >=0 )

sapply(mod1_data_positive, function(x) sum(is.na(x)))

#rm(mod3_data)
mod3_data <- mod1_data_positive

#Removing solid black patterns from the analysis
mod3_data <- subset(mod3_data,COLOR_DESCRIPTION != "Classic Black")
mod3_data <- subset(mod3_data,COLOR_DESCRIPTION != "Black")
mod3_data <- subset(mod3_data,COLOR_DESCRIPTION != "Charcoal")

sapply(mod3_data, function(x) sum(is.na(x)))

#Creating month of launch date feature and merge it back with the model dataset
can_1$month <- format(as.POSIXct(can_1$launch_date),"%B")

mod3_data <- sqldf("
      select
      A.*,
      B.month
      from mod3_data as A
      left join can_1 as B
      on 
      A.color_description = B.color_description and 
      A.merchant_class = B.merchant_class
    ")

mod3_data$month <- as.factor(mod3_data$month)

sku <- dbGetQuery(con,"select * from SKUs")

#Extracting PLM_COLOR_FAMILY from sku to find if the pattern is a sold or flag
mod3_data <- sqldf("select 
distinct
A.*,
B.PLM_COLOR_FAMILY
from mod3_data    as A
left join sku as B
on A. color_description = B. color_description and
      A.merchant_class = B.merchant_class
")

mod3_data$PLM_COLOR_FAMILY <- as.factor(mod3_data$PLM_COLOR_FAMILY)

#If pattern column is not populates, assign it as floral
mod3_data$solid_flag <- ifelse(mod3_data$PLM_COLOR_FAMILY == "Solid","Solid","FloralPattern")

mod3_data$solid_flag <- as.factor(mod3_data$solid_flag)

table(mod3_data$solid_flag)

subset(mod3_data,is.na(mod3_data$solid_flag) == TRUE)

mod3_data$solid_flag[is.na(mod3_data$solid_flag)] <- "FloralPattern"

mod3_data$PLM_COLOR_FAMILY <- NULL

#================================================================================#

mod3_data$cluster <- as.factor(mod3_data$cluster)

#Remove pattern column from modelling
mod3_data <- subset(mod3_data, select = - c(COLOR_DESCRIPTION))

library('scales')

#log transformation of predictor variables

mod3_data$CumulativeUnits <- log(mod3_data$CumulativeUnits + 1)

mod3_data$CumulativeThreeWeekSales <- log(mod3_data$CumulativeThreeWeekSales + 1)

mod3_data$UNITS_1W <- log(mod3_data$UNITS_1W + 1)
mod3_data$UNITS_2W <- log(mod3_data$UNITS_2W + 1)
mod3_data$UNITS_3W <- log(mod3_data$UNITS_3W + 1)

mod3_data$CumulativeThreeWeekMargins <- log(mod3_data$CumulativeThreeWeekMargins + 1)

mod3_data$AVG_PRICE_1W <- log(mod3_data$AVG_PRICE_1W + 1)
mod3_data$AVG_PRICE_2W <- log(mod3_data$AVG_PRICE_2W + 1)
mod3_data$AVG_PRICE_3W <- log(mod3_data$AVG_PRICE_3W + 1)

mod3_data$CumulativeThreeWeekStores <- log(mod3_data$CumulativeThreeWeekStores + 1)
mod3_data$CumulativeThreeWeekStyles <- log(mod3_data$CumulativeThreeWeekStyles + 1)

mod3_data$existing_1M <- log(mod3_data$existing_1M + 1)
mod3_data$existing_2M <- log(mod3_data$existing_2M + 1)
mod3_data$existing_3M <- log(mod3_data$existing_3M + 1)

#Capping outliers

mod3_data$CumulativeUnits <- squish(mod3_data$CumulativeUnits, quantile(mod3_data$CumulativeUnits, c(.01, .99)))
mod3_data$CumulativeThreeWeekSales <- squish(mod3_data$CumulativeThreeWeekSales, quantile(mod3_data$CumulativeThreeWeekSales, c(.01, .99)))
mod3_data$UNITS_1W <- squish(mod3_data$UNITS_1W, quantile(mod3_data$UNITS_1W, c(.01, .99)))
mod3_data$UNITS_2W <- squish(mod3_data$UNITS_2W, quantile(mod3_data$UNITS_2W, c(.01, .99)))
mod3_data$UNITS_3W <- squish(mod3_data$UNITS_3W, quantile(mod3_data$UNITS_3W, c(.01, .99)))
mod3_data$CumulativeThreeWeekMargins <- squish(mod3_data$CumulativeThreeWeekMargins, quantile(mod3_data$CumulativeThreeWeekMargins, c(.01, .99)))
mod3_data$AVG_PRICE_1W <- squish(mod3_data$AVG_PRICE_1W, quantile(mod3_data$AVG_PRICE_1W, c(.01, .99)))
mod3_data$AVG_PRICE_2W <- squish(mod3_data$AVG_PRICE_2W, quantile(mod3_data$AVG_PRICE_2W, c(.01, .99)))
mod3_data$AVG_PRICE_3W <- squish(mod3_data$AVG_PRICE_3W, quantile(mod3_data$AVG_PRICE_3W, c(.01, .99)))
mod3_data$CumulativeThreeWeekStores <- squish(mod3_data$CumulativeThreeWeekStores, quantile(mod3_data$CumulativeThreeWeekStores, c(.01, .99)))
mod3_data$CumulativeThreeWeekStyles <- squish(mod3_data$CumulativeThreeWeekStyles, quantile(mod3_data$CumulativeThreeWeekStyles, c(.01, .99)))
mod3_data$existing_1M <- squish(mod3_data$existing_1M, quantile(mod3_data$existing_1M, c(.01, .99)))
mod3_data$existing_2M <- squish(mod3_data$existing_2M, quantile(mod3_data$existing_2M, c(.01, .99)))
mod3_data$existing_3M <- squish(mod3_data$existing_3M, quantile(mod3_data$existing_3M, c(.01, .99)))

mod3_data <- subset (mod3_data,is.na(cluster)==FALSE)

library('caret')

sapply(mod3_data, function(x) sum(is.na(x)))

#creating dummies for factor columns using dummyVars()
dummies <- dummyVars(CumulativeUnits ~ ., data = mod3_data)
ex <- data.frame(predict(dummies, newdata = mod3_data))
mod3_data <- cbind(mod3_data$CumulativeUnits,ex)
names(mod3_data)[1] <- "CumulativeUnits"

#removing highly correlated variables using Pearson's correlation formula

#descrCor <- cor(mod3_data[,2:ncol(mod3_data)])
#highCorr <- sum(abs(descrCor[upper.tri(descrCor)]) > 0.85)
#summary(descrCor[upper.tri(descrCor)])

#highlyCorDescr <- findCorrelation(descrCor,cutoff = 0.85)
#filteredDescr <- mod3_data[,2:ncol(mod3_data)][,-highlyCorDescr]

#mod3_data <- cbind(mod3_data$CumulativeUnits ,filteredDescr)
#names(mod3_data)[1] <- "CumulativeUnits"

#Linear combos 

CumulativeUnits <- mod3_data$CumulativeUnits

mod3_data <- cbind(rep(1,nrow(mod3_data)),mod3_data[2:ncol(mod3_data)])
names(mod3_data[1]) <- "ones"
comboInfo  <- findLinearCombos(mod3_data)
mod3_data <- mod3_data[,-comboInfo$remove]
mod3_data <- mod3_data[,c(2:ncol(mod3_data))]
mod3_data <- cbind(CumulativeUnits , mod3_data)

#Removing variables with very low variation

nzv <- nearZeroVar(mod3_data, saveMetrics = TRUE)
mod3_data <- mod3_data[,c(TRUE,!nzv$zeroVar[2:ncol(mod3_data)])]

#Partition into train & test dataset

set.seed(333)
inTrain <- createDataPartition(y = (mod3_data$CumulativeUnits) , p = 0.7, list = F)
train <- mod3_data[inTrain,]
test <- mod3_data[-inTrain,]

#Cross-validation design - 3-fold cross validation

ctrl <- trainControl(method = "cv" ,
                     number = 10,
                     classProbs = F,
                     summaryFunction = defaultSummary,
                     allowParallel = T)

model3_lm <- train(CumulativeUnits ~ .,
                   data = train,
                   method = "lm",
                   trControl = ctrl)

summary(model3_lm)

library("caret")

#Prediction
pred_train <- predict(model3_lm,train)
pred_train <- exp(pred_train)

predictedVal <- predict(model3_lm,test)
predictedVal <- exp(predictedVal)

modelvalues <- data.frame(obs = exp(test$CumulativeUnits) , pred = predictedVal)
defaultSummary(modelvalues)

library("Metrics")
rmse(exp(train$CumulativeUnits),pred_train)
rmse(exp(test$CumulativeUnits),predictedVal)

mae(exp(train$CumulativeUnits),pred_train)
mae(exp(test$CumulativeUnits),predictedVal)

#variable importance
(varImp(model3_lm))

plot(exp(test$CumulativeUnits) ,predictedVal)
abline(coef = c(0,1), col = "blue")

####################### lasso #################################

install.packages('elasticnet')
library(elasticnet)

ctrl_lasso <- trainControl(method = "cv" ,
                           number = 10,
                           allowParallel = T,
                           verboseIter = T)

lasso_grid <- expand.grid(fraction=c(1,0.1,0.01,0.001))

sapply(train, function(x) sum(is.na(x)))

head(train)

model3_lasso <- train(CumulativeUnits ~ . ,
                      data = train,
                      method = "lasso",
                      tuneGrid = lasso_grid,
                      trControl = ctrl_lasso)

model3_lasso$results
model3_lasso$best

predictedVal_train_lasso <- predict(model3_lasso, train)
modelvalues_train_lasso <- data.frame(obs = exp(train$CumulativeUnits) , pred = exp(predictedVal_train_lasso))
defaultSummary(modelvalues_train_lasso)

predictedVal_test_lasso <- predict(model3_lasso,test)
modelvalues_test_lasso <- data.frame(obs = exp(test$CumulativeUnits) , pred = exp(predictedVal_test_lasso))
defaultSummary(modelvalues_test_lasso)

####################### lmStepAIC #################################

install.packages('MASS')
library(MASS)

ctrl <- trainControl(method = "cv" ,
                     number = 10,
                     allowParallel = T,
                     verboseIter = T)

model3_lmStepAIC <- train(CumulativeUnits ~ . ,
                          data = train,
                          method = "lmStepAIC",
                          trControl = ctrl)

summary(model3_lmStepAIC)

predictedVal_train_lmStepAIC <- predict(model3_lmStepAIC, train)
modelvalues_train_lmStepAIC <- data.frame(obs = exp(train$CumulativeUnits) , pred = exp(predictedVal_train_lmStepAIC))
defaultSummary(modelvalues_train_lmStepAIC)

predictedVal_test_lmStepAIC <- predict(model3_lmStepAIC,test)
modelvalues_test_lmStepAIC <- data.frame(obs = exp(test$CumulativeUnits) , pred = exp(predictedVal_test_lmStepAIC))
defaultSummary(modelvalues_test_lmStepAIC)

###############################################################################