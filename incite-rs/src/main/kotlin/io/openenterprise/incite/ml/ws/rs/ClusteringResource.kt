package io.openenterprise.incite.ml.ws.rs

import io.openenterprise.incite.data.domain.Clustering
import org.springframework.stereotype.Component
import javax.ws.rs.Path

@Path("/cluster-analyses")
@Component
interface ClusteringResource: MachineLearningResource<Clustering, Clustering.Model>