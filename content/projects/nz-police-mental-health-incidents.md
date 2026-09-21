---
title: Exploring mental health incidents in New Zealand Police Data
subheading: Explore trends in mental health report misclassifications to the
  police as indicators of supplementary social services.
weight: 60
tags:
  - exploratory-analysis
  - reporting
  - r
external_url: https://github.com/christianllave/nz-police-data
external_label: View on GitHub
draft: false
---
In 2021, the New Zealand Police expressed a need to approach call-outs related to mental health issues. To free up the workload of the police force in that regard, it would be good to explore existing trends on mental health-related issues.

Using the NZ Police's Demand and Activity as the main data source, I used R to group and visualise police reports related to mental health based on the activity's classifications, time, and date.

This allowed trends of mental health reports to surface, showing which aspects of social services could be focused on to provide better mental health support.

### Main takeaways

- There is value in being able to engineer additional columns from existing ones (feature engineering) even for exploratory analyses like this.
- This is a good exercise for squeezing out options to find trends in data.
- For exploratory tasks, there is value in starting from line items with a high proportion of observations as a filter when drilling-down on data.
