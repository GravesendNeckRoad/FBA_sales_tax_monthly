UPD 12/18/24

Updated this old script to run on an Azure Function App. It now generates a tax/revenue report by first fetching the orders data from the 'Reports' SP-API, and then formatting the final report, before uploading to a blob container. 

example URI: https://<app_name>.azurewebsites.net/api/<func_name>?account_name=<acc_initials>&start_date=09-12-2024&end_date=12/15/2024

I set it up to run on the first day of every month with a Logic App (by omitting the start/end date query params, the program defaults to the previous months date range). The Logic App then generates/sends an email to accounting with the report as an attachment.

You can pass your own start/end dates. This program works around Amazons 31-day limit for the 'Reports' API - you can use any date range - it will sequentially generate the orders data in chunks, and then create the tax report. The only limitation is that this API only stores data for up to around 2 years, so you cant go too far back. And, you will almost certainly need a Premium Function App plan to circumvent the timeouts, for longer date-range reports. Other than that, it scales - has been tested locally on accounts with >100k sales per month, and for date ranges of >1Y.

You can also bypass the API (for example, if you already have the orders data on your local machine, you can pass it as a parameter in this class).

Note; will clean the underlying report_tools.py in future updates, its getting to be quite bloated.