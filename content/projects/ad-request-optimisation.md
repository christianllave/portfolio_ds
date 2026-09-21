---
title: Ad Request Optimisation
subheading: Increased profit margins by limiting ad requests to profitable observations.
weight: 40
tags:
# skills
- machine-learning
# tools
- python
- lightgbm
- airflow
- docker
- aws
- optuna
---

Supply-Side Platforms manage ad space by deciding which instances of web traffic would be good to show ads to; however, not all traffic is profitable, and sending these instances to the auction (real-time bidding) can be costly. The nature of the data and potential target variables were akin to the marketing funnel that had preceding steps that informed the latter. This project aimed to drop unprofitable traffic to save on costs, while retaining as much revenue as possible. As a solution in the digital ad industry, it must deliver the predictions in a fraction of a second.

Given the nature of the data being mostly categorical, the solution made use of categorical features. I explored three ensemble frameworks commonly used for the said data type. Given the funnel-like nature of the data, I created three different architectures:

- **Simple:** directly predict each candidate target variable using identified features. This was simple and effective.
- **Narrowing:** have target variables learn only based on observations present in the preceding step.
- **Cascading:** predict succeeding target variables using outputs from previous steps of the funnel as features. This mimicked the real-time bidding behaviour most closely.

Additionally, this project had also undergone the production process by creating the accompanying assets: threshold seeker, hyperparameter tuning, modules, tests, a docker image, Airflow DAG and deploying as a cloud service.

#### Main Takeaways

- **Categorical handling:** Exploratory analyses, transformations, imputation, and modelling.
- **Data engineering:** Performing ETL on data extracted from a cloud source.
- **Data science:** Decision-making and experimenting on different models, infrastructure, and methodologies.
- **Data analysis:** Visualisation and presentation of the solution's value to key stakeholders.
- **Software engineering:** Experiencing the process of putting code into production.
