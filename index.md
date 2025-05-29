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
