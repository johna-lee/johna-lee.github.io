---
layout: single
classes: wide
author_profile: true
---

# Data Engineering Project: An End-to-End ELT Pipeline for Soccer Analysis

This project is written from a hypothetical perspective, where I act as a consultant for my favorite soccer club, Arsenal. Arsenal is in desperate need of an attacker—an offensive forward whose main job is to score goals—but they are facing budgetary constraints. My task is to identify an ideal value candidate: a young, highly efficient, up-and-coming player who does not command a high transfer fee from the selling club.

To do this I built the pipeline below, which handles batch match data and leverages various services in Google Cloud Platform (GCP).
1. Python and Pandas scrape data from the web, save it as a CSV file, and upload it to Google Cloud Storage (GCS)
2. GCS stores a copy of the raw data before processing, to maintain data integrity
3. Dataflow processes hundreds of CSV files and loads the data into BigQuery
4. BigQuery serves as the data warehouse, where transformations are made and the dataset is exported to Power BI
5. Power BI enables data analysis and visualizations to present the findings

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
