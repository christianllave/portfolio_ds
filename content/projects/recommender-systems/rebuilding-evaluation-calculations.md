---
title: Rebuilding evaluation calculations for recommendation systems
subheading: A lot of models work. Evaluation metrics tell us which ones work well.
weight: 10
tags:
  - recommender-systems
  - machine-learning
  - linear-algebra
  - python
  - pandas
  - numpy
  - scipy
external_url: https://github.com/christianllave/mind-reco-public/blob/main/html/evaluate-annotated-v2.md
external_label: Read the annotated code
draft: false
---
Evaluation metrics describe how well a model predicts on unseen data, and are used in fine-tuning. In the Python package "implicit", the evaluation module returned errors for common metrics, such as Normalised Discounted Cumulative Gain (NDCG). After some research, the [accepted solution](https://github.com/benfred/implicit/issues/726#issuecomment-2632016615) was to perform the evaluation manually.

### Goal

Create my own version of the evaluation module.

### Considerations for the approach

> [!details]- Starting Metric
>
> I'm particularly interested in the NDCG metric, because it accounts for the position and relevance of a recommended item. Starting with NDCG also allows for the easier addition of other metrics (eg. Precision, Recall, and AUC) because they make use of the components of NDCG.
>
> **Result:** I chose a NDCG metric as a starting point.
>
> For reference, I'm following this [paper](https://dl.acm.org/doi/pdf/10.1145/3394486.3403226), which describes each metric.

> [!details]- Iteration Approach
>
> Loops have their place in evaluating recommendation outputs, as the metrics are often expressed in iterations and positions that determine the rank of an item. Cython is said to be more optimised at loops compared to regular Python, which may be why it was used in the [original evaluation module](https://github.com/benfred/implicit/blob/main/implicit/evaluation.pyx#L366). However, I found setting up C to be an additional overhead at the prototyping and tuning stages.
>
> **Result:** I chose a vectorised approach using Numpy arrays, range logic, and SciPy matrices as opposed to loops as an alternative method.

> [!details]- Validating Predictions
>
> The recommended item needs to be associated with at least two properties (relevance, position in the recommendation). Three methods came to mind:
>
> - **Batched loops** on either the sparse matrix or long-form user interaction dataframe
>   - This is straightforward and made efficient with Cython. However, without Cython, it may run into performance issues.
>   - This method is outside the scope of this project.
> - **Use the explode method on Pandas** on a long-form user interaction dataframe
>   - Maintains the order and position of recommendations.
>   - Can work with a small number (K) of recommendations for each user, as using this method increases the size of the data K times.
>   - Can cause troubles for those working on limited memory.
> - **Compare sparse matrices directly**
>   - The sparse property of the matrix representation allows for memory efficiency.
>   - Using the position as data values preserves the order of recommendation.
>
> **Result:** Produced a new dataframe that indicated the recommendations' relevance as indicated by a positive position value.

> [!details]- Array Operations on Pandas
>
> Array operations within Pandas was not as straightforward as doing math on int or float values. To regain access to element-wise array operations:
>
> 1. Use the Pandas Series as a Numpy array by using the pandas.Series.values method.
> 2. Use numpy.stack to convert the 1D array of arrays into a 2D array.
> 3. Apply the mathematical operations.
> 4. Re-assign to the Pandas Dataframe column using the list function.
>
> **Result:** We can then calculate for the NDCG components: Delta function, Discounted Gain, Discounted Cumulative Gain
>
> For the annotated code: [Read more](https://github.com/christianllave/mind-reco-public/blob/main/html/evaluate-annotated-v2.md)

> [!details]- Handling jagged arrays
>
> Instead of a fixed K-sized list, IDCG is based on the number of relevant items for each user. This means the the list of relevant items for each user has varying sizes. The size is at most K, but could be less if they have fewer relevant items. Representing the number of relevant items as a list intuitively would call for a jagged array (array values with different sizes); however this is not supported in Numpy as of writing. This leads us to padding the rest of the array as 0 to keep a uniform list size. Zero is used as it doesn't interfere with the necessary calculation.
>
> **Result:** Padded arrays create the range where we can reuse the Discounted Gain function to calculate for IDCG.

### Outcome

This project resulted in modules that calculated the NDCG from recommendation systems that:

- is more easily generalisable by avoiding the overhead of a C compiler.
- is particularly useful for Python coders who are not familiar with C.
- can be more computationally efficient by avoiding Python loops.

### Skills applied

- **Translating Linear Algebra to code**
- **Using Sparse Matrices:**
  - Enabled a more memory-efficient execution while using less computational resources.
  - Matrix operations (ex. element-wise math) allowed me to compare predictions with the validation set without loops.
- **Array operations on DataFrames:**
  - Provided an alternative to nested loops for tabular data.
  - Prevented the use of the .apply method.
- **Using Padded Arrays:**
  - Acted as a workaround the lack of support for jagged arrays.
  - May be limited to use cases where the padding value doesn't interfere with calculations.
