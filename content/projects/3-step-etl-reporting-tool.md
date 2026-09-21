---
title: 3-Step ETL Reporting Tool
subheading: Integrate 5 Brand Data Sources programmatically to populate reporting records.
weight: 50
tags:
  - reporting
  - etl
  - python
  - apis
draft: false
---
Management makes use of spend, revenue, and eCommerce metrics to make marketing decisions. The stakeholders had a preference for tables that could be explored by date range and broken down by date. Another consideration for this project is the fact that the tool will be used by non-technical stakeholders.

The way to accomplish the data needs was by integrating brand assets: Facebook, Instagram, Shopify, Google Ads, and Google Analytics into an interactive reporting tool. With non-technical stakeholders in mind, I have set my own limitations on top of meeting the data needs:

- I should not require them to code or install Python for them to use the tool.
- The data should be formatted in a way they recognise: tabular like Excel.
- Ensure fool-proof API access and security.

Google assets had native integrations to the selected reporting tool. For the other channels, I used their respective APIs to extract, transform, and store information on a cloud asset. I then integrated all the data on the reporting tool, created calculated fields that combined metrics from different sources, and created the necessary visualisations.

### Main takeaways

- One of the challenges at the time was dealing with APIs, which required a lot of reading up on documentation. One of my main takeaways was to make my code style match the demands of each API's documentation. This allowed me to follow the logic and ensure convenient points to edit my code should there be any changes in the documentation.
- Dealing with JSON objects meant I had to transform column values that were non-singular, such as lists and dictionaries. Working on this project allowed me to develop ways to convert non-relational formats to relational in a vectorised fashion. Using vectorised functions allowed for more efficiency as opposed to methods that iterate over observations.
- Having the end-user in mind allows for a clear picture of limitations in terms of user exprience and infrastructure.
