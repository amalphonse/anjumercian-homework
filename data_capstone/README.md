# City Data Dashboard Project

### Goal of the Project

This project is for a Data Engineer.io bootcamp capstone project. I am building a public dashboard to replicate the website 
https://www.city-data.com/; instead of the data dump, my goal is to have a dashboard with graphs and charts that is 
more visually appealing, so for example, anyone looking to move to and live in San Jose City in California they would get a dashboard with statistics that represents the crime rates, the property worth, the homelessness rate, the rental situation. The expected output will be a dashboard that would showcase the comparison of property rates across different features of a home, like for a 3 bedroom 2 bath house, the price and the sqft, and also the crime rates comparison to different cities in a particular state.

The project follows the following steps:

- Step 1: Scope the Project and Gather Data 
- Step 2: Explore and Assess the Data 
- Step 3: Define the Data Model 
- Step 4: Run ETL to Model the Data 
- Step 5: Conclusion

## Step 1: Scope the Project and gather the data.

In this project, I will be gathering data regarding housing and crime rates and placing them into fact and dimensional tables accordion to my data model attached  below. And running queries like:
How does the location affect the property rate?
What are the different features of the home affecting property price?
Property types available in particular postal code
Crime rates across different cities
Types of crime in different cities per state.
This dashboard will be looking at the past 5 year data. 

The technologies I will be using for this project are Pandas, Seaborn, Matplotlib, Google Collab Google Cloud for Storage, and Tableau Public to publish the dashboard. I am using these technologies because I am starting out on a small scale, so using Google Cloud Storage and Google Colab will be easy to handle and free. I am using Tableau Public which is also free. Pandas has great libraries for all the exploratory Data Analysis. Seaborn is a very good data visualization library that I am using within Colab to visualize my data and create charts. I am currently manually uplaoding the datasets to tableau public for dashboard. Images added .

### Data Sources

Crime data: https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi
Housing Data: https://www.car.org/marketdata/data
The data sources I have chosen are government data sources; why I selected these is because they are going to be the most legitimate data I can find, but also the problem is I cant have the latest data, I have to wait for them to upload the data.

## Step 3: Data Model.

I am using the **Snowflake Data Model**, there is one fact table for the dashboard and there is another fact table table for the property details with it dimensional tables and another side there is another fact table for the crime data and dimensional details for the crime related details, I chose this model because I have plans to extend the data model to show homelessness data and rental data, it would be easier to add another fact and dimensional table and add another column to the centralized fact table. The architecture that way will be more flexible and have more granularity. 

![Data Model](https://github.com/amalphonse/anjumercian-homework/blob/anjumercian_capstone_proecject/data_capstone/images/dashboard_data_model.jpeg)


## Step 4: The data Pipelines.

I have attached the ETL steps taken for the [properties data](propertyDataAnalysis_NY_updated.ipynb) and the [crime data](crime_data_CA.ipynb). The crime data had clean data there was not much cleaning needed but had to transform to what the was required. 

I chose the properties data for NY and crime data for CA, to showcase the two states.

### Dashboard will look like this with the graphs.

![Dashboard 1](https://github.com/amalphonse/anjumercian-homework/blob/anjumercian_capstone_proecject/data_capstone/images/dashboard_1.png)

![Dashboard 2](https://github.com/amalphonse/anjumercian-homework/blob/anjumercian_capstone_proecject/data_capstone/images/dashboard_2.png)

![Dashboard 3](https://github.com/amalphonse/anjumercian-homework/blob/anjumercian_capstone_proecject/data_capstone/images/dashboard_3.png)

![Dashboard 4]](https://github.com/amalphonse/anjumercian-homework/blob/anjumercian_capstone_proecject/data_capstone/images/dashboard_4.png)


## Conclusion
I am planning on extending this dashb oard to include two more datasets with home lessness data and rental data for which I will be moving over to spark because Spark can handle 100x more data efficiently. 

Also use Apache Flink to stream the data when it is available. First stream the data and store for batch processing. 

Using airflow we can orchestrate the data pipelines to be ready for a 9am executive dashboard viewing.


