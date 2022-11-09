package io.openenterprise.incite.ml.ws.rs

import io.openenterprise.incite.data.domain.Regression

interface RegressionResource: MachineLearningResource<Regression, Regression.Model>