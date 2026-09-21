---
title: User churn model
subheading: Predicting inactivity using activity, and the lack thereof
weight: 20
tags:
  - machine-learning
  - python
  - lightgbm
  - optuna
draft: false
---
Digital media rely on user activity to determine marketable audiences. Being able to predict churning users allow for retention efforts to be put in place. In this project, I defined churn as a 30-day streak of a logged-out state, or 30-day streak of non-listenership.

My main considerations were:

- **Data leakage:** Temporal splitting into 30, 60, 90 day windows of activity prevented this.
- **Representing user activity:** To level the amount of potential activity for each user, I used a moving window of time spent listening.
- **Including or excluding churned users in training:** Reactivation was uncommon, but the possibility of happening warranted its inclusion. Training a model including churned users improved model performance.
- **Correct training metric:** As it was important to determine actual churned users, I used the Precision-Recall AUC metric for tuning with Optuna.
- **Feature selection:** Absolute listenership is tied to activity, so I included changes in listenership, and directions of change as features to better inform the model.

The resulting model predicted around 70% of users correctly, with less than 10% being missed churners. Users are then assigned churn predictions based on the model, which informs the retention team for activation.

### Main Takeaways

- Temporal considerations: splitting the dataset by time windows, engineering features based on time windows, representing inactivity (gaps vs numbers), representing change over time
- Data science: decision-making and experimenting on different models, representations, and modelling components.
- Data analysis: presentation of the model's behaviour to identify trends in user behaviour relevant to churn, broken down by brand and changes in user activity.
- Data engineering: Identifying feature importance in the working model justifies more effort into data quality for relevant fields in the warehouse.
