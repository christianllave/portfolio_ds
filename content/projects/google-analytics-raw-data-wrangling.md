---
title: 'Google Analytics Raw Data: Wrangling'
subheading: 'Zero to Hero: From raw data to business-ready assets.'
weight: 30
tags:
# skill
- data-modelling
- analytics-engineering
- etl
# tools
- snowflake
- sql
---

At the nascency of this digital radio product, I focused on establishing big data pipelines to meet business needs for reporting, visualisation, and machine learning applications. Google Analytics as a platform displays aggregated data; however, the business required analyses involving event-level data. This prompted the need for the Raw Data, which scales up in volume relative to the brand's user activity. With the requirements in mind, I ensured all my contributions were compute-optimised for the large-scale nature of the data. This foundational initiative resulted in high quality useable data for querying, integrating, and modelling.

#### A number of core challenges arose from the nature of the data and the needs of the business

> [!details]- The table requires flattening to get relevant fields.
>
> Using dataclasses and vectrorised operations in Snowflake Python (SnowPark, similar to PySpark):
>
> 1. Determine all the fields that will be extracted.
> 2. Apply the extraction function to the nested field to extract the fields as columns.
> 3. Partition the data by event types (audio engagement, general engagement, etc) and apply the function by partition.
>
> This parallelised procedures and tasks, which enables smaller partitions (ex. onboarding events) to be processed independently from larger partitions (ex. audio engagement).

> [!details]- Missing session IDs create unattributed engagement.
>
> The solution needed to be closer to deterministic considering errors would flow downstream, so I went with a nearest neighbour model:
>
> 1. Create an initial lookup table with the sessions, start and end times.
> 2. Create pseudo-identifiers for each event.
> 3. Map the events to the lookup table, and calculate each event's timestamp difference to the joined session's start and end timestamps.
> 4. Take the closest session within an acceptable time difference.
> 5. Iterate until no more mappable events are available.
>
> Reducing the null values in sessions allowed for a more complete means of calculating session counts per user and the session duration. Both are used mostly for reporting requirements.

> [!details]- Missing user IDs for critical events like audio events meant unattributed listening metrics.
>
> Solution:
>
> 1. Determine mappable and unmappable user IDs.
> 2. Map 1:1 pseudo IDs with user IDs using a mapping table.
> 3. Map 1:many pseudo IDs with user IDs using window functions on Snowflake SQL. This attributes the most recent user ID to the event with a missing user ID.
>
> This imputed over 90% of null values for audio events, which attributes listening metrics to each user more accurately. This is primarily in preparation for machine learning applications.

> [!details]- Backend data sending incorrect data
>
> Some show_name values were assigned to the wrong brand and a constant 'ERROR' string occupied the user ID field. Solution:
>
> 1. Communicate with the errors with the product team to send correct data for the succeeding records.
> 2. Once the correctly associated fields start coming in, create a mapping table using the latest assignments, and apply the mapping.
> 3. For constant values, apply the same 1:1 or 1:many mapping strategy.
>
> For reporting, this solution attributes the correct performance metrics to the brands. For feature engineering, this assigns the correct degree and preference of listenership to the users.

#### Main Takeaways

- Flattening, transforming, and imputing large-scale tables without default join keys.
- Partitioning, parallel processing, vectorisation, and caching helped with computational efficiency.
- Mapping functions support categorical imputation; however, it is best to communicate with developers to fix data issues on the backend.
