---
layout: single
author_profile: true
---
### Data Engineering Project: An End-to-End ELT Pipeline for Soccer Analysis

This project is done from a hyptothetical perspective where I am a consultant for my favorite soccer club, Arsenal. Arsenal are in desperate need of an "attacker", an offensive forward whose main job is to score goals, but they have budgetary constraints. My job is to identify the ideal value candidate; a player who is young, highly efficient, and up-and-coming, who does not command a big transfer fee from the selling club.

To do this, I built the pipeline below which handles batch match data and leverages various services in Google Cloud Platform (GCP).
* Python+Pandas scrapes data from the web, saves as CSV, and uploads to Google Cloud Storage (GCS)
* GCS stores a copy of the raw data before processing to maintain data integrity
* Dataflow processes the hundreds of CSV files and loads the data in to BigQuery
* BigQuery serves as the data warehouse where transformations are made and the dataset is exported to PowerBI
* PowerBI allows for data analysis and data visualizations to present findings
