options(java.parameters = "-Xmx64048m") # 64048 is 64 GB
#install.packages("odbc")
#install.packages("RMariaDB")
library(RMariaDB)
# Connect to a MariaDB version of a MySQL database
con <- dbConnect(RMariaDB::MariaDB(), host="datamine.rcac.purdue.edu", port=3306
                 , dbname="***********"
                 , user="", password="")
# list of db tables
dbListTables(con)

#####################DATA CLEANING AND UNDERSTANDING######################################

# understanding the data and changing the datatypes of the variables
# importing the transactional_web_data table and removing the negative order quantity 
web <- dbGetQuery(con, "select * from transactional_web_data")

# changing the datatypes
web$DIM_ORDER_ENTRY_METHOD_KEY <- as.factor(web$DIM_ORDER_ENTRY_METHOD_KEY)
web$ORDER_NUMBER <- as.factor(web$ORDER_NUMBER)
web$DIM_SKU_KEY <- as.factor(web$DIM_SKU_KEY)
web$ORDER_DATE <- as.Date(web$ORDER_DATE, '%Y-%m-%d')
web$ORDER_LINE_NUMBER <- as.numeric(web$ORDER_LINE_NUMBER)

library(sqldf)

# selecting only the required columns for analysis and only 
# for DIM_ORDER_ENTRY_METHOD_KEY in '21'
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

# removing those orders with negative quantity
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

# creating positive amount table and writing it into the database
web_21_pos_amt <- sqldf('select A.*, B.order_number as order_number_neg 
                        from web_21 as A 
                        left join negative_amount_qty_order as B on A.order_number = B.order_number 
                        where B.order_number is  NULL')

dbWriteTable(con,name='web_line_positive_amt',value=web_21_pos_amt,row.names=FALSE)
##### We will be further using 'web_line_positive_amt' table for our analysis

# Importing transaction POS Data
# selecting only the required columns for analysis  
pos <- dbGetQuery(con, "select 
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

#Converting to correct data types
pos$DIM_STORE_KEY <- as.factor(pos$DIM_STORE_KEY)
pos$DIM_SKU_KEY <- as.factor(pos$DIM_SKU_KEY)
pos$SALE_DATE <- as.Date(pos$SALE_DATE, '%Y-%m-%d')

#Identifying distinct order numbers where either the amount or quantity is negative @order-level
library('sqldf')
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

# writing this table back to the data base 
dbWriteTable(con,name='pos_line_positive_amt',value=pos_positive_amt,row.names=FALSE)

######## this cleaned table will be further used for our analysis ##################

############################## DATA CLEANING SECTION ENDS#####################################


############################### AGGREGATING THE DATA #########################################

# importing web transactions where orders amount > 0 
web <- dbGetQuery(con, "select * from web_line_positive_amt")
# importing SKU Table
skus <- dbGetQuery(con, "select * from skus_filtered") 

# Web-data table aggregated at SKU and date-level
library(sqldf)

# taking minimum date of the launch if SKU table doesn't have launch date or 
# in some cases launch date is greater than minimum launch date
agg_transaction_web_date <- sqldf("select A.DIM_SKU_KEY		
                                    , ORDER_DATE		
                                    , CASE WHEN C.launch_date is null or C.launch_date > B.min_order_date then B.min_order_date else C.launch_date end as 'min_launch_date'
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
                                    inner join 
                                    (
                                      select DIM_SKU_KEY,
                                      launch_date
                                      from skus
                                    ) C on C.DIM_SKU_KEY = B.DIM_SKU_KEY
                                    group by A.DIM_SKU_KEY		
                                    , ORDER_DATE		
                                    , CASE WHEN C.launch_date is null or C.launch_date > B.min_order_date then B.min_order_date else C.launch_date end		
                                    ")

agg_transaction_web_date$min_launch_date <- as.Date(agg_transaction_web_date$min_launch_date, origin = "1970-01-01") 		

# importing POS table where orders>0
pos <- dbGetQuery(con, "select * from pos_line_positive_amt")
library(sqldf)

# imputing launch date as min(sale_date) where launch date is null
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
                      inner join 		
                      (		
                        select DIM_SKU_KEY,		
                        LAUNCH_DATE		
                        from skus		
                      ) C ON C.DIM_SKU_KEY = A.DIM_SKU_KEY")		


# aggregating POS transactions at SKU, Store, Date level
# removing disney store from the analysis
agg_pos_date <- sqldf("select A.DIM_SKU_KEY 		
                                    , A.DIM_STORE_KEY  		
                                    , SALE_DATE		
                                    , sum(NET_SALE_AMOUNT) as totalsales		
                                    , count(distinct POS_SLIPKEY_SLIP_LVL) as trans		
                                    , SUM(SALE_QUANTITY) as units		
                                    , sum(NET_SALE_AMOUNT-STANDARD_COST_AT_TRANSACTION) as margins		
                                    , sum(RETAIL_PRICE_AT_TRANSACTION*SALE_QUANTITY) as price		
                                    from pos A	
                                    where A.DIM_STORE_KEY != '179'
                                    group by 		
                                      A.DIM_SKU_KEY		
                                    , A.DIM_STORE_KEY		
                                    , SALE_DATE		
                                    ")	

# importing store table
store <- dbGetQuery(con, "select * from store")

# aggregating POS transactions at SKU, Region (West, East), saledate level
agg_pos_date_1 <- sqldf("select A.DIM_SKU_KEY
                                    , S.district_description
                                    , SALE_DATE		
                                    , sum(totalsales) as totalsales		
                                    , sum(trans) as trans		
                                    , SUM(units) as units		
                                    , sum(margins) as margins	
                                    , sum(price) as price		
                                    from agg_pos_date A	
                                    left join store s on s.DIM_STORE_KEY = A.DIM_STORE_KEY
                                    where A.DIM_STORE_KEY != '179' 
                                    and s.STORE_TYPE_CHARACTERISTIC_DESCRIPTION in ('Full Line')
                                    group by 	
                                      A.DIM_SKU_KEY	
                                    , S.district_description  
                                    , SALE_DATE	
                                    ")

# joining the launch_date table to get the minimum launchdate
agg_1 <- sqldf("select A.*, B.LaunchDate
               from agg_pos_date_1 A
               left join launch_date B on B.DIM_SKU_KEY = A.DIM_SKU_KEY")
agg_1$LaunchDate <- as.Date(agg_1$LaunchDate, origin = "1970-01-01")

# combining pos and web transactions using union statement
agg_2 <- sqldf("select channel
              , dim_sku_key              
              , order_date as sale_date 
              , min(min_launch_date) as LaunchDate, 
              sum(totalsales) as totalsales, 
              sum(trans) as trans,
              sum(units) as units, 
              sum(margins) as margins,
              sum(price) as price
              from 
              (
              select 'Web' as Channel, A.* 
              from agg_transaction_web_date A
              where DIM_SKU_KEY > 0
              union all 
              select 'Pos' as Channel
              , dim_sku_key
              , SALE_DATE as order_date
              , LaunchDate as min_launch_date
              , totalsales
              , trans
              , units
              , margins
              , price
              from agg_1
              )
              where price > 0 
              group by channel
               , dim_sku_key
               , sale_date")

agg_2$LaunchDate <- as.Date(agg_2$LaunchDate, origin = "1970-01-01") 	

# importing the SKU Table 
skus <- dbGetQuery(con, "select * from SKUs")

# joining the SKU table to get the features related to SKU(ex- color/pattern, merchant_class)
agg_3 <- sqldf("select A.*  
                        , B.STYLE_DESCRIPTION
                        , B.COLOR_DESCRIPTION
                        , B.RELEASE_SEASON_ID
                        , B.RETIREMENT_DATE
                        , B.MERCHANT_DEPARTMENT
                        , B.MERCHANT_CLASS
                        , B.PLM_PRIMARY_COLOR
                        , B.PLM_SECONDARY_COLOR
                        , B.PLM_COLOR_FAMILY
                         from agg_2 A
                        inner join skus B on B.DIM_SKU_KEY = A.DIM_SKU_KEY
                        where B.MERCHANT_CLASS != 'Marketing'
                        ")

# writing this table back to the database 
# this is the table which will be used for clustering and modeling
dbWriteTable(conn = con, name = "web_pos_saledate", value = agg_3, row.names = FALSE)

############################### AGGREGATING THE DATA ENDS ################################

#### The table written back to the database is used for data modelling and clustering 


############################### Clustering and Modelling section #########################

# importing web-pos-salesdate table
agg_3 <- dbGetQuery(con, "select * from web_pos_saledate")
agg_3$sale_date <- as.Date(agg_3$sale_date, origin = '1970-01-01')

# filtering only for the SKU's released in past 3 years
agg_4 <- subset(agg_3, agg_3$RELEASE_SEASON_ID == 'F17' | 
                  agg_3$RELEASE_SEASON_ID == 'F18' |
                  agg_3$RELEASE_SEASON_ID == 'F19' |
                  agg_3$RELEASE_SEASON_ID == 'M17' |
                  agg_3$RELEASE_SEASON_ID == 'M18' |
                  agg_3$RELEASE_SEASON_ID == 'M19' |
                  agg_3$RELEASE_SEASON_ID == 'S17' |
                  agg_3$RELEASE_SEASON_ID == 'S18' |
                  agg_3$RELEASE_SEASON_ID == 'S19' |
                  agg_3$RELEASE_SEASON_ID == 'S20' |
                  agg_3$RELEASE_SEASON_ID == 'W17' |
                  agg_3$RELEASE_SEASON_ID == 'W18' |
                  agg_3$RELEASE_SEASON_ID == 'W19' )

# filtering for the SKU's released after Feb-2017 as transaction table has only records after Feb-2017
agg_4 <- subset(agg_4, agg_4$LaunchDate >= '2017-02-01')
agg_4 <- subset(agg_4, agg_4$STYLE_DESCRIPTION != 'E-Gift Card')

# adding solid_flag to identify if a pattern is solid/pattern
agg_4$Solid_Flag <- ifelse(agg_4$PLM_COLOR_FAMILY == 'Solid' , 'Solid' , 'Non-Solid')
agg_4$Solid_Flag <- replace(agg_4$Solid_Flag , is.na(agg_4$Solid_Flag) , 'Non-Solid')

# identifying top-150 patterns based on total sales
library(sqldf)
top_patterns <- sqldf("select color_description, sum(totalsales) as totalsales
                      from agg_4
                      group by color_description
                      order by totalsales desc")
top_patterns <- head(top_patterns, 150)

# binning the variables into 1W, 2W, 1M, 2M sale buckets based on difference 
# between sale_date and launch_date
agg_4$sale_date <- as.Date(agg_4$sale_date, origin = '1970-01-01')

agg_4$datediff <- as.Date(agg_4$sale_date) - as.Date(agg_4$LaunchDate)
agg_4$datediff <- as.numeric(agg_4$datediff)

# binning the datediff column
breaks <- c(0,7,14,21,60,90,120,150,180,Inf)		
# specify interval/bin labels		
tags <- c("1W","2W", "3W", "2M", "3M","4M", "5M","6M", ">6M")		

# bucketing values into bins		
group_tags <- cut(agg_4$datediff, 		
                  breaks=breaks, 		
                  include.lowest=TRUE, 		
                  right=FALSE, 		
                  labels=tags)		

agg_4 <- cbind(agg_4 , group_tags)

############### CLUSTERING THE PATTERNS ##########################

# Clustering the patterns based on different parameters like 
# no.of merchant classes, no of SKU's, no.of styles, total sales, units sold,
# margins, avg_price
# Clustering is performed only based on first 3 weeks sales

clustering <- sqldf("select color_description, count(distinct dim_sku_key) as count_sku
                    , count(distinct style_description) as count_styles
                    , count(distinct merchant_class) as count_merchantclass
                    , sum(totalsales) as totalsales
                    , sum(units) as units
                    , sum(margins) as total_margins
                    , sum(margins)/sum(units) as avg_margins
                    , sum(price)/sum(units) as avg_price
                    from agg_4
                    where group_tags in ('1W', '2W', '3W')
                    group by color_description")

# Taking only top 150 patterns 
clustering_1 <- sqldf("select A.*
                      from clustering A
                      inner join top_patterns B on B.color_description = A.color_description")

# Taking the required numeric columns for clustering
df <- clustering_1[,c(2,3,4,5,6,8,9)]

# z-score standardize for these variable
dfz <- scale(df)
dfz <- data.frame(scale(df))

cost_df <- data.frame() #accumulator for cost results
cost_df

for(k in 1:15){
  # allow up to 50 iterations to obtain convergence, and do 20 random starts
  kmeans_tr <- kmeans(x=dfz, centers=k, nstart=20, iter.max=100)
  #Combine cluster number and cost together, write to cost_df
  cost_df <- rbind(cost_df, cbind(k, kmeans_tr$tot.withinss))
}

# the cost_df data.frame contains the # of clusters k and the Mean Squared Error
# (MSE) for each cluster
names(cost_df) <- c("cluster", "tr_cost")
cost_df

# create an elbow plot to validate the optimal number of clusters
par(mfrow=c(1,1))
cost_df[,2] <- cost_df[,2]
plot(x=cost_df$cluster, y=cost_df$tr_cost, main="k-Means Elbow Plot"
     , col="blue", pch=19, type="b", cex.lab=1.2
     , xlab="Number of Clusters", ylab="MSE (in 1000s)")
points(x=cost_df$cluster, y=cost_df$te_cost, col="green")

library(cluster)
# creating Silhouette plot
km3 <- kmeans(x=dfz, centers=3, nstart=20, iter.max=100)
dist3 <- dist(dfz, method="euclidean")
sil3 <- silhouette(km3$cluster, dist3)
plot(sil3, col=c("black","red","green"), main="Silhouette plot (k=3) K-means (withoutseasons)", border=NA)

# concatenating the cluster back to table
clustering_1 <- cbind(clustering_1, km3$cluster)
colnames(clustering_1)[10] <- 'cluster'

######### CLUSTERING ENDS #####################################

############ Considering cannibalisation effects ######################
## including cannibalisation factors by taking into account no.of patterns launched in the 
# past 3,2,1 months respectively

# considering only top 150 patterns
effects <- sqldf("select A.* 
                 from agg_4 A
                 inner join top_patterns B on B.color_description = A.color_description")

# aggregating for top 150 patterns at pattern, channel, merchantclass and daily level
effects_1 <- sqldf("select channel 
                    , color_description
                    , merchant_class
                    , Solid_Flag
                    , MIN(LaunchDate) over (partition by color_description, Merchant_class,channel) as min_launch_date
                    , sale_date
                    , sum(totalsales) as totalsales
                    , sum(units) as units
                    , sum(margins) as margins
                    , sum(price) as price
                    from effects 
                    group by 
                    channel 
                    , color_description
                    , merchant_class
                    , Solid_Flag
                    , sale_date
                   ")

# changing the datatypes
effects_1$min_launch_date <- as.Date(effects_1$min_launch_date, origin = '1970-01-01') 
colnames(effects_1)[5] <- 'launchdate'

# adding cannibalization features # no.of patterns launched 
# in the past 3,2,1 months and their sales in the first three weeks of 
# a new launch (# of units, avg.price, total sales)
test <- sqldf("select channel, color_description, merchant_class, launchdate, solid_flag
              from effects_1
              group by channel, color_description, merchant_class, launchdate, solid_flag")

# cannibalization features for 3Month
can_1 <- sqldf("select A.channel, A.color_description, A.merchant_class, A.launchdate
               , A.solid_flag
               , count(distinct B.color_description) as launched_3M
               , sum(B.totalsales) as existing_3M_totalsales
               , sum(B.units) as existing_3M_units
               , sum(B.margins)/sum(B.units) as existing_3M_avgmargins
               , sum(B.totalsales)/sum(B.units) as existing_3M_avg_price
               from test A
               left join effects_1 B on B.color_description != A.color_description
                                   and B.Merchant_class = A.Merchant_class and 
                                   B.channel = A.channel and
                                   B.solid_flag = A.solid_flag and
                                   B.launchdate between A.LaunchDate-90 and A.LaunchDate -1
                                   and B.sale_date between A.launchDate and A.launchDate + 21 
                group by A.channel, A.color_description, A.merchant_class, A.launchdate, A.solid_flag
               ")

# cannibalization features for 2Month
can_2 <- sqldf("select A.channel, A.color_description, A.merchant_class, A.launchdate
                , A.solid_flag
                , count(distinct C.color_description) as launched_2M
                , sum(C.totalsales) as existing_2M_totalsales
                , sum(C.units) as existing_2M_units
                , sum(C.margins)/sum(C.units) as existing_2M_avgmargins
                , sum(C.totalsales)/sum(C.units) as existing_2M_avg_price
               from test A
               left join effects_1 C on C.color_description != A.color_description
                                   and C.Merchant_class = A.Merchant_class and 
                                   C.channel = A.channel and
                                   C.solid_flag = A.solid_flag and
                                   C.launchdate between A.LaunchDate-60 and A.LaunchDate -1
                                   and C.sale_date between A.launchDate and A.launchDate + 21 
                group by A.channel, A.color_description, A.merchant_class
                , A.launchdate, A.solid_flag
               ")

# cannibalization features for 1Month
can_3 <- sqldf("select A.channel, A.color_description, A.merchant_class, A.launchdate
                , A.solid_flag
                , count(distinct D.color_description) as launched_1M
                , sum(D.totalsales) as existing_1M_totalsales
                , sum(D.units) as existing_1M_units
                , sum(D.margins)/sum(D.units) as existing_1M_avgmargins
                , sum(D.totalsales)/sum(D.units) as existing_1M_avg_price
                from test A
                left join effects_1 D on D.color_description != A.color_description
                and D.Merchant_class = A.Merchant_class and 
                D.channel = A.channel and
                D.solid_flag = A.solid_flag and
                D.launchdate between A.LaunchDate -30 and A.LaunchDate -1
                and D.sale_date between A.launchDate and A.launchDate + 21
                group by A.channel, A.color_description, A.merchant_class, A.launchdate
                , A.solid_flag
               ")

# joining all the cannibalization features to one table for each pattern
# joining cannibalisation 1month and 2month features to a single table
can <- sqldf("select A.* , B.launched_2M
              , B.existing_2M_totalsales
              , B.existing_2M_units
              , B.existing_2M_avgmargins
              , B.existing_2M_avg_price
              from can_1 A 
              left join can_2 B on B.channel = A.channel and B.merchant_class = A.merchant_class
              and B.color_description = A.color_description and B.launchdate = A.launchdate
              and B.solid_flag = A.solid_flag")

# joining cannibalisation 3month features
can <- sqldf("select A.* 
              , B.launched_1M
              , B.existing_1M_totalsales
              , B.existing_1M_units
              , B.existing_1M_avgmargins
              , B.existing_1M_avg_price
              from can A
              left join can_3 B on B.channel = A.channel and B.merchant_class = A.merchant_class
              and B.color_description = A.color_description and B.launchdate = A.launchdate
              and B.solid_flag = A.solid_flag")

############### considering cannibalisation effects ends ################################

############### MODELLING DATA SET #######################################
# adding seasonality features like Spring, Summer
model <- sqldf("select Channel, COLOR_DESCRIPTION, MERCHANT_CLASS
               , CASE when Release_Season_ID like 'M%' then 'SUMMER'
                      when Release_Season_ID like 'S%' then 'Spring'
                      when Release_Season_ID like 'W%' then 'Winter'
                      when Release_Season_ID like 'F%' then 'Fall'
               END as Season
               , Solid_Flag
               , min(LaunchDate)
               , group_tags
               , sum(totalsales) as totalsales
               , sum(units) as units
               , sum(margins)/sum(units) as avg_margins
               , sum(price)/sum(units) as avg_price
               from agg_4
               group by Channel
               , Solid_Flag
               , CASE when Release_Season_ID like 'M%' then 'SUMMER'
                      when Release_Season_ID like 'S%' then 'Spring'
                      when Release_Season_ID like 'W%' then 'Winter'
                      when Release_Season_ID like 'F%' then 'Fall'
                 END
               , COLOR_DESCRIPTION
               , MERCHANT_CLASS
               , group_tags")

# restricting to only 150-patterns
model <- sqldf("select A.*
               from model A
               inner join top_patterns B on B.COLOR_DESCRIPTION = A.COLOR_DESCRIPTION")

colnames(model)[6] <- 'LaunchDate'
model$LaunchDate <- as.Date(model$LaunchDate, origin = '1970-01-01')

#transposing - since week tags need to be used as features
library(tidyverse)
library(dplyr)
model_transpose <-  pivot_wider(data = model, names_from=group_tags, 
                                values_from = c("totalsales","units","avg_margins","avg_price"))

# imputing missing values by zero
model_transpose_1 <- model_transpose %>%
  mutate(totalsales_1W = coalesce(totalsales_1W, 0),
         totalsales_2W = coalesce(totalsales_2W, 0),
         totalsales_3W = coalesce(totalsales_3W, 0),
         totalsales_2M = coalesce(totalsales_2M, 0),
         totalsales_3M = coalesce(totalsales_3M, 0),
         units_1W = coalesce(units_1W, 0),
         units_2W = coalesce(units_2W, 0),
         units_3W = coalesce(units_3W, 0),
         units_2M = coalesce(units_2M, 0),
         units_3M = coalesce(units_3M, 0),
         avg_margins_1W = coalesce(avg_margins_1W, 0),
         avg_margins_2W = coalesce(avg_margins_2W, 0),
         avg_margins_3W = coalesce(avg_margins_3W, 0),
         avg_margins_2M = coalesce(avg_margins_2M, 0),
         avg_margins_3M = coalesce(avg_margins_3M, 0),
         avg_price_1W = coalesce(avg_price_1W, 0),
         avg_price_2W = coalesce(avg_price_2W, 0),
         avg_price_3W = coalesce(avg_price_3W, 0),
         avg_price_2M = coalesce(avg_price_2M, 0),
         avg_price_3M = coalesce(avg_price_3M, 0))

# taking only columns required for analysis
# taking the cumulative_units
model_transpose_2 <- sqldf("select  Channel, COLOR_DESCRIPTION, MERCHANT_CLASS, Season, Solid_Flag,
                         LaunchDate,  
                         totalsales_1W, totalsales_2W, totalsales_3W,
                         units_1W, units_2W, units_3W, (units_2M+units_3M) as cumulative_units,
                         avg_margins_1W, avg_margins_2W, avg_margins_3W,
                         avg_price_1W, avg_price_2W, avg_price_3W
                        from model_transpose_1")

# Cleaning data, removing records where cumulative sales are 0 and 1W/2W/3W sales are zero
model_transpose_3 <- subset(model_transpose_2, model_transpose_2$totalsales_1W+model_transpose_2$totalsales_2W+model_transpose_2$totalsales_3W > 0)
model_transpose_3 <- subset(model_transpose_3 , model_transpose_3$cumulative_units > 0)

# joining the cluster and canibalisation information 
model_transpose_4 <- sqldf("select A.*
                            , B.launched_1M
                            , B.existing_1M_totalsales
                            , B.existing_1M_units
                            , B.existing_1M_avgmargins
                            , B.existing_1M_avg_price
                            , B.launched_2M
                            , B.existing_2M_totalsales
                            , B.existing_2M_units
                            , B.existing_2M_avgmargins
                            , B.existing_2M_avg_price
                            , B.launched_3M
                            , B.existing_3M_totalsales
                            , B.existing_3M_units
                            , B.existing_3M_avgmargins
                            , B.existing_3M_avg_price
                           from model_transpose_3 A 
                           left join can B on B.channel = A.channel 
                           and B.merchant_class = A.merchant_class
                           and B.color_description = A.color_description 
                           and B.launchdate = A.launchdate
                           and B.solid_flag = A.solid_flag")

# adding clusters
model_transpose_4 <- sqldf("select A.*, B.cluster 
                           from model_transpose_4 A
                           left join clustering_1 B on B.color_description = A.color_description")

# calculating avg price and margin for first 3 weeks
model_transpose_4$avgprice <- (model_transpose_4$units_1W*model_transpose_4$avg_price_1W+model_transpose_4$units_2W*model_transpose_4$avg_price_2W+model_transpose_4$units_3W*model_transpose_4$avg_price_3W)/(model_transpose_4$units_1W+model_transpose_4$units_2W+model_transpose_4$units_3W)
model_transpose_4$avgmargin <-(model_transpose_4$units_1W*model_transpose_4$avg_margins_1W+model_transpose_4$units_2W*model_transpose_4$avg_margins_2W+model_transpose_4$units_3W*model_transpose_4$avg_margins_3W)/(model_transpose_4$units_1W+model_transpose_4$units_2W+model_transpose_4$units_3W)

# removing variable not required
model_transpose_4$avg_margins_1W <- NULL
model_transpose_4$avg_margins_2W <- NULL
model_transpose_4$avg_margins_3W <- NULL

model_transpose_4$avg_price_1W <- NULL
model_transpose_4$avg_price_2W <- NULL
model_transpose_4$avg_price_3W <- NULL

# imputing the nulls by 0 
model_transpose_4 <- model_transpose_4 %>%
  mutate(existing_1M_totalsales = coalesce(existing_1M_totalsales, 0),
         existing_2M_totalsales = coalesce(existing_2M_totalsales, 0),
         existing_3M_totalsales = coalesce(existing_3M_totalsales, 0),
         existing_1M_units = coalesce(existing_1M_units, 0),
         existing_2M_units = coalesce(existing_2M_units, 0),       
         existing_3M_units = coalesce(existing_3M_units, 0),
         existing_1M_avgmargins = coalesce(existing_1M_avgmargins, 0),
         existing_2M_avgmargins = coalesce(existing_2M_avgmargins, 0),
         existing_3M_avgmargins = coalesce(existing_3M_avgmargins, 0),
         existing_1M_avg_price = coalesce(existing_1M_avg_price, 0),
         existing_2M_avg_price = coalesce(existing_2M_avg_price, 0),  
         existing_3M_avg_price = coalesce(existing_3M_avg_price, 0)) 

str(model_transpose_4)
model_transpose_4$launched_1M <- as.numeric(model_transpose_4$launched_1M)
model_transpose_4$launched_2M <- as.numeric(model_transpose_4$launched_2M)
model_transpose_4$launched_3M <- as.numeric(model_transpose_4$launched_3M)

# imputing nulls and missing by zero
model_transpose_4 <- model_transpose_4 %>%
  mutate(launched_1M = coalesce(launched_1M, 0),
         launched_2M = coalesce(launched_2M, 0),
         launched_3M = coalesce(launched_3M, 0))

# removing the columns that are correlated 
# margin and price were highly correlated so removed margins from the analysis
model_transpose_4$existing_1M_avgmargins <- NULL
model_transpose_4$existing_2M_avgmargins <- NULL
model_transpose_4$existing_3M_avgmargins <- NULL
model_transpose_4$avgmargin <- NULL

##################imputing the values #############################

## imputing avg prices of cannibalization and number of patterns launched 
## in past 3M for those launched before '2017-05-02' 
impute_3M <- subset(model_transpose_4, model_transpose_4$LaunchDate > '2017-05-02')
values_3M <- sqldf("select channel, merchant_class
                   , solid_flag, CEIL(avg(launched_3M)) as launched_3M
                   , avg(existing_3M_avg_price) as existing_3M_avg_price
                   from impute_3M
                   group by channel, merchant_class
                   , solid_flag")

model_transpose_4$LaunchDate <- as.numeric(model_transpose_4$LaunchDate)
as.numeric(as.Date('2017-05-02'))  #17288

model_transpose_5 <- sqldf("select A.Channel, A.COLOR_DESCRIPTION, A.MERCHANT_CLASS,
                           A.Season, A.Solid_Flag, A.LaunchDate, A.totalsales_1W,
                           A.totalsales_2W, A.totalsales_3W, A.units_1W, A.units_2W, A.units_3W,
                           A.cumulative_units, A.launched_1M, A.existing_1M_totalsales, 
                           A.existing_1M_units, A.existing_1M_avg_price, A.launched_2M, 
                           A.existing_2M_totalsales, A.existing_2M_units, A.existing_2M_avg_price, A.cluster,
                           A.avgprice, A.existing_3M_totalsales, A.existing_3M_units
                           , CASE WHEN A.LaunchDate <= 17288 then B.launched_3M 
                                  ELSE A.launched_3M END as launched_3M
                           , CASE WHEN A.LaunchDate <= 17288 then B.existing_3M_avg_price
                                  ELSE A.existing_3M_avg_price end as existing_3M_avg_price
                           from model_transpose_4 A
                           left join values_3M B on B.channel = A.Channel and B.merchant_class = A.MERCHANT_CLASS
                           and B.solid_flag = A.Solid_Flag
                           ") 

## imputing avg prices of cannibalization and number of patterns launched 
## in past 2M for those launched before '2017-04-02' 
model_transpose_4$LaunchDate <- as.Date(model_transpose_4$LaunchDate, origin = "1970-01-01")

impute_2M <- subset(model_transpose_4, model_transpose_4$LaunchDate > '2017-04-02')
values_2M <- sqldf("select channel, merchant_class
                   , solid_flag, CEIL(avg(launched_2M)) as launched_2M
                   , avg(existing_2M_avg_price) as existing_2M_avg_price
                   from impute_2M
                   group by channel, merchant_class
                   , solid_flag")

model_transpose_4$LaunchDate <- as.numeric(model_transpose_4$LaunchDate)
as.numeric(as.Date('2017-04-02'))  #17258

model_transpose_5 <- sqldf("select A.Channel, A.COLOR_DESCRIPTION, A.MERCHANT_CLASS,
                           A.Season, A.Solid_Flag, A.LaunchDate, A.totalsales_1W,
                           A.totalsales_2W, A.totalsales_3W, A.units_1W, A.units_2W, A.units_3W,
                           A.cumulative_units, A.launched_1M, A.existing_1M_totalsales, 
                           A.existing_1M_units, A.existing_1M_avg_price, A.launched_3M, 
                           A.existing_3M_totalsales, A.existing_3M_units, A.existing_3M_avg_price, A.cluster,
                           A.avgprice
                           , A.existing_2M_totalsales, A.existing_2M_units
                           , CASE WHEN A.LaunchDate <= 17258 then B.launched_2M 
                                  ELSE A.launched_2M END as launched_2M
                           , CASE WHEN A.LaunchDate <= 17258 then B.existing_2M_avg_price
                                  ELSE A.existing_2M_avg_price end as existing_2M_avg_price
                           from model_transpose_5 A
                           left join values_2M B on B.channel = A.Channel and B.merchant_class = A.MERCHANT_CLASS
                           and B.solid_flag = A.Solid_Flag
                           ") 

## imputing avg prices of cannibalization and number of patterns launched 
## in past 1M for those launched before '2017-03-02' 
model_transpose_4$LaunchDate <- as.Date(model_transpose_4$LaunchDate, origin = "1970-01-01")

impute_1M <- subset(model_transpose_4, model_transpose_4$LaunchDate > '2017-03-02')
values_1M <- sqldf("select channel, merchant_class
                   , solid_flag, CEIL(avg(launched_1M)) as launched_1M
                   , avg(existing_1M_avg_price) as existing_1M_avg_price
                   from impute_1M
                   group by channel, merchant_class
                   , solid_flag")

as.numeric(as.Date('2017-03-02'))  #17227

model_transpose_5 <- sqldf("select A.Channel, A.COLOR_DESCRIPTION, A.MERCHANT_CLASS,
                           A.Season, A.Solid_Flag, A.LaunchDate, A.totalsales_1W,
                           A.totalsales_2W, A.totalsales_3W, A.units_1W, A.units_2W, A.units_3W,
                           A.cumulative_units, A.launched_2M, A.existing_2M_totalsales, 
                           A.existing_2M_units, A.existing_2M_avg_price, A.launched_3M, 
                           A.existing_3M_totalsales, A.existing_3M_units, A.existing_3M_avg_price, A.cluster,
                           A.avgprice
                           , A.existing_1M_totalsales, A.existing_1M_units
                           , CASE WHEN A.LaunchDate <= 17227 then B.launched_1M 
                                  ELSE A.launched_1M END as launched_1M
                           , CASE WHEN A.LaunchDate <= 17227 then B.existing_1M_avg_price
                                  ELSE A.existing_1M_avg_price end as existing_1M_avg_price
                           from model_transpose_5 A
                           left join values_1M B on B.channel = A.Channel and B.merchant_class = A.MERCHANT_CLASS
                           and B.solid_flag = A.Solid_Flag
                           ") 
###################### imputing ends #######################################

plot(data$units_3W+data$units_2W+data$units_1W , data$cumulative_units)
plot(log(data$units_3W), log(data$cumulative_units))
model_transpose_5$LaunchDate <- as.Date(model_transpose_5$LaunchDate, origin = "1970-01-01")

# subsetting the data only for top 15 classes 
data <- subset(model_transpose_5, model_transpose_5$MERCHANT_CLASS == 'Crossbodies' |
                 model_transpose_5$MERCHANT_CLASS == 'Backpacks' |
                 model_transpose_5$MERCHANT_CLASS == 'Travel Bags' |
                 model_transpose_5$MERCHANT_CLASS == 'Totes' |
                 model_transpose_5$MERCHANT_CLASS == 'IDs/Keychains' |
                 model_transpose_5$MERCHANT_CLASS == 'Wristlets' |
                 model_transpose_5$MERCHANT_CLASS == 'Cosmetics' |
                 model_transpose_5$MERCHANT_CLASS == 'Travel/Packing Accessories' |
                 model_transpose_5$MERCHANT_CLASS == 'Textiles' |
                 model_transpose_5$MERCHANT_CLASS == 'Wallets' |
                 model_transpose_5$MERCHANT_CLASS == 'Lunch Bags' |
                 model_transpose_5$MERCHANT_CLASS == 'Satchels' |
                 model_transpose_5$MERCHANT_CLASS == 'Rolling Luggage' |
                 model_transpose_5$MERCHANT_CLASS == 'Laptop/Tablet Accessories'|
                 model_transpose_5$MERCHANT_CLASS == 'Other Handbag Accessories')

# removing outliers ex - holiday patterns which had higher sales in the 
# first three weeks alone and no sales in the subsequent months
data <- subset(data, !(data$cumulative_units <= 1000 & data$units_3W+data$units_2W+data$units_1W > 1000))

# creating new feature i.e the relative price of the substitutable item
data$relativeprice <- ifelse(data$existing_3M_avg_price == 0, 1, data$avgprice/data$existing_3M_avg_price)

# changing the data types
data$MERCHANT_CLASS <- as.factor(as.character(data$MERCHANT_CLASS))

str(data)

data$Channel <- as.factor(data$Channel)
data$MERCHANT_CLASS <- as.factor(data$MERCHANT_CLASS)
data$Season <- as.factor(data$Season)
data$Solid_Flag <- as.factor(data$Solid_Flag)
data$LaunchDate <- as.Date(data$LaunchDate, origin = "1970-01-01")
data$cluster <- as.factor(data$cluster)

data$month <- strftime(data$LaunchDate, '%B')
data$month <- as.factor(data$month)

# subsetting data as 3M units sold will not be available for launches after '2019-07-07'
data <- subset(data, data$LaunchDate <= '2019-07-07')

# removing the columns not required for analysis 
data$existing_1M_totalsales <- NULL
data$existing_2M_totalsales <- NULL
data$existing_3M_totalsales <- NULL
data$existing_1M_units <- NULL
data$existing_2M_units <- NULL
data$existing_3M_units <- NULL

data$existing_1M_avg_price <- NULL
data$existing_2M_avg_price <- NULL
data$existing_3M_avg_price <- NULL

# creating this ID Variables to study the predictions
data$COLOR_DESCRIPTION -> color_ID
data$MERCHANT_CLASS -> merchant_ID
data$Season -> Season_ID
data$LaunchDate -> LaunchDate_ID
data$month -> month_ID

data$COLOR_DESCRIPTION <- NULL
data$LaunchDate <- NULL
data$Season <- NULL

str(data)

# using CARET library for model building
library(caret)
# model building 
#creating dummies for factor columns using dummyVars()
dummies <- dummyVars(cumulative_units ~ ., data = data)
ex <- data.frame(predict(dummies, newdata = data))

data <- cbind(data$cumulative_units,ex)
colnames(data)[1] <- 'cumulative_units'

#Linear combos - removing one of the created dummy variable to avoid multicollinearity
CumulativeUnits <- data$cumulative_units

data <- cbind(rep(1,nrow(data)),data[2:ncol(data)])
names(data[1]) <- "ones"
comboInfo  <- findLinearCombos(data)
data <- data[,-comboInfo$remove]
data <- data[,c(2:ncol(data))]
data <- cbind(CumulativeUnits , data)

#Removing variables with very low variation
nzv <- nearZeroVar(data, saveMetrics = TRUE)
data <- data[,c(TRUE,!nzv$zeroVar[2:ncol(data)])]

#checking distributions of quantitative variables
# most of the variables are right skewed and log transformations were done
hist(data$CumulativeUnits)
hist(log(data$CumulativeUnits))
hist(log(data$totalsales_1W))
hist(log(data$totalsales_2W))
hist(log(data$totalsales_3W))
hist(log(data$units_1W))
hist(log(data$units_2W))
hist(log(data$units_3W))
hist(data$launched_1M)
hist(data$launched_2M)
hist(data$launched_3M)

hist(log(data$existing_1M_units))
hist(log(data$existing_2M_units))
hist(log(data$existing_3M_units))

hist(data$existing_1M_avg_price)
hist(data$existing_2M_avg_price)
hist(data$existing_3M_avg_price)

# checking for skewness
skewness(data$existing_1M_units)
skewness(data$existing_2M_units)
skewness(data$existing_3M_units)

skewness(data$existing_1M_avg_price)
skewness(data$existing_2M_avg_price)
skewness(data$existing_3M_avg_price)

# # transforming the variable
data$totalsales_1W <- log(data$totalsales_1W+0.001)
data$totalsales_2W <- log(data$totalsales_2W+0.001)
data$totalsales_3W <- log(data$totalsales_3W+0.001)

data$units_1W <- log(data$units_1W+0.001)
data$units_2W <- log(data$units_2W+0.001)
data$units_3W <- log(data$units_3W+0.001)

data$CumulativeUnits <- log(data$CumulativeUnits)

data <- cbind(color_ID, LaunchDate_ID, merchant_ID, month_ID, Season_ID, data)

# splitting the data set into train-test (70/30 split)
set.seed(1234)
inTrain <- createDataPartition(y = data$CumulativeUnits , p = 0.7, list = F)
train <- data[inTrain,]
test <- data[-inTrain,]

train$color_ID <- NULL
train$LaunchDate_ID <- NULL
train$merchant_ID <- NULL
train$month_ID <- NULL
train$Season_ID <- NULL

test$color_ID <- NULL
test$LaunchDate_ID <- NULL
test$merchant_ID <- NULL
test$month_ID <- NULL
test$Season_ID <- NULL

#Cross-validation design - 5-fold cross validation
ctrl <- trainControl(method = "cv" ,
                     number = 5,
                     classProbs = F,
                     summaryFunction = defaultSummary,
                     allowParallel = T)

#######################Linear regression###########################

model <- train(CumulativeUnits ~ . ,
               data = train,
               method = "lm",
               trControl = ctrl)

summary(model)

# Evaluating the predictions
predictedVal_train <- predict(model, train)
modelvalues_train <- data.frame(obs = exp(train$CumulativeUnits) , pred = exp(predictedVal_train))
defaultSummary(modelvalues_train)

predictedVal_test <- predict(model,test)
modelvalues_test <- data.frame(obs = exp(test$CumulativeUnits) , pred = exp(predictedVal_test))
defaultSummary(modelvalues_test)

# plotting actual vs predictions graphs
plot(modelvalues_test$obs ,modelvalues_test$pred)
abline(coef = c(0,1), col = "blue")

########### Random Forests
library(ranger)

model2_rf <- train(CumulativeUnits ~ . ,
                   data = train,
                   method = "ranger",
                   trControl = ctrl,
                   metric = 'MAE',
                   tuneLength  = 15,
)

# evaluating the predictions
predictedVal_train <- predict(model2_rf, train)
modelvalues_train <- data.frame(obs = exp(train$CumulativeUnits) , pred = exp(predictedVal_train))
defaultSummary(modelvalues_train)

predictedVal_test <- predict(model2_rf,test)
modelvalues_test <- data.frame(obs = exp(test$CumulativeUnits) , pred = exp(predictedVal_test))
defaultSummary(modelvalues_test)

plot(modelvalues_test$obs ,modelvalues_test$pred)
abline(coef = c(0,1), col = "blue")

# XGBoost
library(xgboost)
# tuning parameters for XGBoost
xgb.grid <- expand.grid(nrounds = 450,
                        max_depth = 7,
                        eta = 0.03,
                        gamma = 0.5,
                        colsample_bytree = 0.5,
                        min_child_weight= 7,
                        subsample = 0.7)

xgb_tune <-train(CumulativeUnits ~.,
                 data= train,
                 method="xgbTree",
                 metric = "MAE",
                 trControl=ctrl,
                 tuneGrid=xgb.grid)

# Evaluating the model
predictedVal_train <- predict(xgb_tune, train)
modelvalues_train <- data.frame(obs = exp(train$CumulativeUnits) , pred = exp(predictedVal_train))
defaultSummary(modelvalues_train)

predictedVal_test <- predict(xgb_tune,test)
modelvalues_test <- data.frame(obs = exp(test$CumulativeUnits) , pred = exp(predictedVal_test))
defaultSummary(modelvalues_test)

plot(modelvalues_test$obs ,modelvalues_test$pred)
abline(coef = c(0,1), col = "blue")

# plotting the variable importance plots
caret_imp <- varImp(xgb_tune)
plot(caret_imp, top = 20)

caret_imp <- varImp(model)
plot(caret_imp, top = 15)

##################### decision tree bagging approach #####################
#install.packages("rpart")
library(rpart)

model_DT <- train(
  CumulativeUnits ~.
  , data = train
  , method = "treebag"
  ,trControl = ctrl
  ,importance = TRUE)

predictedVal_train <- predict(model_DT, train)
modelvalues_train <- data.frame(obs = exp(train$CumulativeUnits) , pred = exp(predictedVal_train))
defaultSummary(modelvalues_train)

predictedVal_test <- predict(model_DT,test)
modelvalues_test <- data.frame(obs = exp(test$CumulativeUnits) , pred = exp(predictedVal_test))
defaultSummary(modelvalues_test)

plot(modelvalues_test$obs ,modelvalues_test$pred)
abline(coef = c(0,1), col = "blue")
















