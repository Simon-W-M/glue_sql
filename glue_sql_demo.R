# dynamic sql in R using janitor

library(glue)
library(DBI)

con_udal <- dbConnect(
  drv = odbc::odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = serv, # add your server and
  database = db, # database here
  authentication = "ActiveDirectoryInteractive"
)

# standard query, nothing special

dat <- DBI::dbGetQuery(
  conn = con_udal,
  statement = glue_sql("

SELECT  [Period]
      ,[Month]
      ,[OrgRef]
      ,[OrgName]
      ,[Region_Name]
      ,[STP_Code]
      ,[STP_Name]
      ,[Service]
      ,[ServiceType]
      ,[MetricID]
      ,[MetricDescription] as Question
      ,[MetricValue]
FROM [Reporting_SEFT_Sitreps_Published].[CommunityHealthServicesSitRep]
WHERE month > DATEADD(mm,-5,GETDATE())

")
)

# add a variable for a where clause
region <- "SOUTH WEST"

query <- glue_sql("

SELECT  [Period]
      ,[Month]
      ,[OrgRef]
      ,[OrgName]
      ,[Region_Name]
FROM [Reporting_SEFT_Sitreps_Published].[CommunityHealthServicesSitRep]
WHERE month > DATEADD(mm,-5,GETDATE())
AND [Region_Name] = {region}

", .con = con_udal)

dat <- dbGetQuery(conn = con_udal, query)

# lets use a variable to pick a column
sql_col <- "Service"


query <- glue_sql("

SELECT  [Period]
      ,[Month]
      ,[OrgRef]
      ,[OrgName]
      ,[Region_Name]
      {sql_col}

FROM [Reporting_SEFT_Sitreps_Published].[CommunityHealthServicesSitRep]
WHERE month > DATEADD(mm,-5,GETDATE())
AND [Region_Name] = {region}

                  ", .con = con_udal)

dat <- dbGetQuery(conn = con_udal, query)

# a little bit of a r and sql mas up when want to pull in a vector of col names
sql_vector_columns <- c(
  "Period",
  "Month",
  "Region_Name",
  "Service"
)

query <- glue_sql("

SELECT  {`sql_vector_columns`*}

FROM [Reporting_SEFT_Sitreps_Published].[CommunityHealthServicesSitRep]
WHERE month > DATEADD(mm,-5,GETDATE())
AND [Region_Name] = {region}

", .con = con_udal)

dat <- dbGetQuery(
  conn = con_udal,
  query
)

# add a date filter
date_filter <- "2025-03-01"

sql_vector_columns <- c(
  "Period",
  "Month",
  "Region_Name",
  "Service"
)

query <- glue_sql("

SELECT  {`sql_vector_columns`*}

FROM [Reporting_SEFT_Sitreps_Published].[CommunityHealthServicesSitRep]
WHERE month > {date_filter}
AND [Region_Name] = {region}

", .con = con_udal)

dat <- dbGetQuery(
  conn = con_udal,
  query
)

# so the potential for this is to make dynamic sql queries triggered by 
# other parts of the pipeline
# in my use case I wanted to query a large table that was 
# pulling back loads of data
# I only wanted the historic data when current performance was failing
# thus I could write an initial query that would identify the cohort I
# I wanted and then feed that into a query that pulled back the historic data 
# for just them

