---
layout: single
classes: wide
author_profile: true
---

# Data Engineering Project: An End-to-End ELT Pipeline for Soccer Analysis

This project is written from a hypothetical perspective, where I act as a consultant for my favorite soccer club, Arsenal. Arsenal is in desperate need of an attacker—an offensive forward whose main job is to score goals—but they are facing budgetary constraints. My task is to identify an ideal value candidate: a young, highly efficient, up-and-coming player who does not command a high transfer fee from the selling club.

To do this I built the pipeline below, which handles batch match data and leverages various services in Google Cloud Platform (GCP).
1. Python and Pandas scrape data from the web, save it as a CSV file, and upload it to Google Cloud Storage (GCS).
2. GCS stores a copy of the raw data before processing, to maintain data integrity.
3. Dataflow processes hundreds of CSV files and loads the data into BigQuery.
4. BigQuery serves as the data warehouse, where transformations are made and the dataset is exported to Power BI.
5. Power BI enables data analysis and visualizations to present the findings.

![alt]({{ site.url }}{{ site.baseurl }}/assets/images/pipeline_diagram-cropped.svg)

All code and data used in this project can be found on my GitHub: [github.com/johna-lee/striker-project](https://github.com/johna-lee/striker-project){:target="_blank"}

#### The Data
The data I selected is from the past three seasons of two European soccer tournaments: the UEFA Champions League (UCL) and the UEFA Europa League (UEL), resulting in six initial tables comprising an aggregated dataset. I chose these competitions because, although each country in Europe has a professional soccer league, the quality and competitiveness vary drastically. For example, someone who scores 30 goals in the Dutch Eredivisie is doing so against weaker opposition compared to the English Premier League, making it difficult to draw conclusions across leagues. However, in the UCL and UEL, the top teams from each country compete against one another, so performance should theoretically be more predictive. All match data was scraped from FBref.com, the most comprehensive free soccer database on the web.

#### Python and Pandas
To scrape the data, two separate scripts were needed. The first script ([scrape_urls.py](https://github.com/johna-lee/striker-project/blob/main/scrape_urls.py){:target="_blank"}) was used to scrape the URLs for each match in each competition. As seen on the webpage for the 2022–2023 UCL ([FBref](https://fbref.com/en/comps/8/2022-2023/schedule/2022-2023-Champions-League-Scores-and-Fixtures){:target="_blank"}), each match and result for the competition is listed in chronological order, with the stats found by clicking the "Match Report" link. This is the link the script targets for extracting the URL.

The script works by looping through each row in the schedule table, finding the Match Report URL, saving it to an array, printing the array to a Pandas DataFrame, and saving the DataFrame as a CSV file. The resulting URLs for the six competitions can be found here ([match report URLs](https://github.com/johna-lee/striker-project/tree/main/match%20report%20URLs){:target="_blank"}), with the current 2024–2025 competitions included as of May 3rd, 2025.

I utilized Claude.ai to refine the script by adding a request delay and headers, as the script was initially rate-limited and blocked.

The second script ([fbref_scraper.py](https://github.com/johna-lee/striker-project/blob/main/fbref_scraper.py){:target="_blank"}) was used to iterate through the match report URL CSVs and extract data for each player. As seen on the webpage for the first 2022–2023 UCL match ([FBref](https://fbref.com/en/matches/07f058d4/Dinamo-Zagreb-Chelsea-September-6-2022-Champions-League){:target="_blank"}), the main summary data is included in two tables—one for each team. However, these tables are sortable and contain multiple tabs of stats, which proved very challenging when defining the data I was looking for within the complex HTML structure.

With some refinements using Claude.ai, I was able to identify the relevant tables based on key column headers. The script works by iterating through the URL CSVs, extracting the match ID from each URL, locating the relevant tables, and extracting player data from both teams into a single DataFrame. It then inserts additional columns for match ID and team name, saves the DataFrame as a CSV locally, uploads the CSV to a Google Cloud Storage (GCS) bucket, and generates a processing report detailing the result of each upload ([GCS processing reports](https://github.com/johna-lee/striker-project/tree/main/GCS%20processing%20reports){:target="_blank"}). The script includes minor data cleaning as well as request delays and headers to prevent rate limiting.

![alt]({{ site.url }}{{ site.baseurl }}/assets/images/python+pandas GCS upload.PNG)

#### Google Cloud Storage
After each competition’s data was scraped and uploaded to the GCS bucket, I moved the CSV files into named folders. This serves both as a record of the original files and as a way to provide a single path for Dataflow to target when loading the data into BigQuery.

![alt]({{ site.url }}{{ site.baseurl }}/assets/images/GCS bucket data folders.PNG)

#### Dataflow
With the data in GCS, the next step was to load the roughly 900 CSV files into a BigQuery data warehouse using Dataflow. However, two things needed to be done before that could happen.

First, I created a schema file ([bigquery_schema.json](https://github.com/johna-lee/striker-project/blob/main/bigquery_schema.json){:target="_blank"}) which defines the column names and data types of the output tables.

Second, a total of seven output tables were created in BigQuery—one for each of the six competitions' successfully loaded data, and one for errors or data that was not loaded. The diagram below shows the load data flow, with the errors table on the bottom left titled "Insert bad records into Bigquery" and the competition table on the bottom right titled "Insert good records into Bigquery."

![alt]({{ site.url }}{{ site.baseurl }}/assets/images/Dataflow diagram.PNG)

A Dataflow job was created for the initial competition and then cloned for the remaining ones. By targeting the competition folder in the GCS bucket, Dataflow read each file and loaded the data into the respective BigQuery tables. No errors occurred during the Dataflow jobs.

#### BigQuery
After the data was loaded into BigQuery tables, simple transformations were performed. The first was creating two new columns—one titled "competition" and another "season"—as these are not found in the scraped data. The screenshot below shows the SQL query used, which created the columns and set the values for each record. This query was then cloned and adjusted for each respective table.

![alt]({{ site.url }}{{ site.baseurl }}/assets/images/bigquery transformations.PNG)

The next action performed in BigQuery was to merge all six tables into a single table. The screenshot below shows the SQL query used, which unions the data from each table. The query results were saved as a new table named "match_data" using the console interface.

![alt]({{ site.url }}{{ site.baseurl }}/assets/images/bigquery merge tables.PNG)

Lastly, the age column had to be transformed from a string in year-days format to an integer in years. The screenshot below shows the SQL queries used: a temporary column was created, nulls were assigned a value of 0, the string was split on the "-" delimiter, the original age column was deleted, and the updated column was renamed to age.

![alt]({{ site.url }}{{ site.baseurl }}/assets/images/age transformations-cropped.svg)

#### Power BI Desktop
With the dataset now complete, the final step in the data pipeline was to import the merged "match_data" table into Power BI Desktop for analysis, as shown below.

![alt]({{ site.url }}{{ site.baseurl }}/assets/images/powerbi load table.PNG)

#### The Analysis
Although the data has been extracted, loaded, transformed, and is ready for analysis, my job of finding an attacker is just beginning! The following bullet points highlight each stage of my analysis and the thought process behind it. Since I am familiar with pivot table formats, I chose a matrix table for my initial visualization.

#### Initial Exclusion Criteria
* Player and Age – The first fields added to the matrix were "player" and a filter on age.
* Filtering on Age – Age is one of the main criteria I’ve been tasked with in my search for the ideal player. While age is highly subjective, with players peaking at different times, the general consensus is that attackers reach their prime in their mid-to-late 20s. As such, I set the Age summarization to Maximum and filtered for players under 27 years old.
* Filtering on "W" – As mentioned, I am looking for an attacker. Luckily, attackers in the forward line all contain a "W" in their position descriptions: forwards are labeled "FW", while left and right wingers are "LW" and "RW", respectively. To ensure all relevant data is captured, an advanced filter containing "W" is applied to the position field.

![alt]({{ site.url }}{{ site.baseurl }}/assets/images/age less than 27.PNG)

![alt]({{ site.url }}{{ site.baseurl }}/assets/images/powerbi contains w.PNG)

#### Rate-Volume Analysis
* Total Goals – I added a sum of goals and sorted the column in descending order to bring potential candidates into view within the matrix table.
* Goals per 90 – Goals per 90 is a metric used to determine the average number of goals scored per 90 minutes—the length of a full soccer match. This metric is important because players can be substituted, and relying solely on the number of games played can be misleading. For example, if a player comes into two consecutive games in the 85th minute and scores a goal in each, it is technically not inaccurate to say they averaged one goal per game. However, scoring two goals in 10 minutes of total gametime is far more impressive than scoring two goals over two full games (approximately 180 minutes).
* To add a Goals per 90 column, a new measure was created using the following calculation:
      `goals_per_90 = ((SUM(match_data[goal]) / SUM(match_data[minute])) * 90)`
* While Goals per 90 is a powerful metric, it cannot be relied on exclusively. As shown below, outliers can skew the data when minutes played are not also taken into consideration.

![alt]({{ site.url }}{{ site.baseurl }}/assets/images/goals per 90 incorrect.PNG)

* Goals per 90 by Minutes – By plotting Goals per 90 against total Minutes, an interesting visual emerges. For the vast majority of players, the more total minutes played, the lower their Goals per 90 tends to be. However, there are a small number of players who do not follow this trend (circled below), and these are the players I want to focus on. They are maintaining a consistently high Goals per 90 despite an increasing number of total minutes. New filters of ≥1,000 Minutes and ≥0.5 Goals per 90 were applied to the dataset.

![alt]({{ site.url }}{{ site.baseurl }}/assets/images/scatter.PNG)

#### Efficiency Analysis
* Goals to Expected Goals Ratio – Expected Goals (xG) is a metric that measures the quality of a goal-scoring opportunity by calculating the likelihood that it will be scored, based on data from similar shots in the past. In other words, a shot with an xG of 0.5 is expected to be scored half the time. Comparing the number of goals scored to xG provides a measure of efficiency, with a ratio greater than 1 indicating that a player is overperforming, while a ratio less than 1 indicates underperforming.
* To add a Goals to Expected Goals Ratio column, a new measure was created with the following calculation:
      `goals_to_xg_ratio = SUM(match_data[goal]) / SUM(match_data[expected_goal])`

**Note:** The Goals to Expected Goals Ratio is not adjusted to “per 90,” since calculating a "Goals per 90 to Expected Goals per 90" ratio yields the same result.

* A new filter of ≥1.00 Goals to Expected Goals Ratio was applied to the dataset.

xg screenshot here

* Shot Conversion Rate – Shot Conversion Rate is the percentage of shots that result in goals. This measures an attacker’s clinical finishing ability and is another indicator of efficiency in front of goal.

* To add a Shot Conversion Rate column, a new measure was created with the following calculation:
      `shot_conversion_rate = (SUM(match_data[goal]) / SUM(match_data[shot])) * 100`

shot conversion screenshot here
