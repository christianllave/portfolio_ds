---
title: Youtube API Data Extraction
subheading: Extract video and channel data from search queries, and store them
  to a database.
weight: 70
tags:
  - data-engineering
  - etl
  - python
  - pandas
  - sqlite
  - api
external_url: https://github.com/christianllave/api-yt
external_label: View on GitHub
draft: false
---
For my first Python project, I wanted to understand popular video topics in a certain niche, and explore Youtube as a data source. One of the largest limitations to the project is the rate limit, which compelled me to consider an ETL process to avoid redundant extraction.

I used Python to extract data, Pandas to perform transformations, and SQLITE to store the results. In its first iteration, the project allowed for visualisations and inspections of overarching trends of videos and the connectedness of channels. In future iterations, I aim to apply machine learning techniques as a form of feature engineering or predictive analytics.

I intend to use this project as a foundational data source for me to explore other data projects and techniques.

### Main takeaways

- Refactoring is useful for repeatable functions.
- When the cost of API calls is high, store API response results allows for the experimentation of the contents.
- There is value in making connections between the available data between API endpoints.
- When result relevance is important, it's good to know if the API responses are already arranged by relevance to reduce the amount of data to store.
- De-duplicating and batching allow for more efficient API querying by reducing the number of times calls are made.
