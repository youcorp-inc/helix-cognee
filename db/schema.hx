N::CogneeNode {
	INDEX node_id: String,
	name: String,
	node_type: String,
	properties: String, // (JSON string containing additional DataPoint fields like ontology_valid, version, topological_rank, metadata)
	created_at: Date DEFAULT NOW,
	updated_at: Date DEFAULT NOW,
}

E::CogneeEdge {
	From: CogneeNode,
	To: CogneeNode,
	Properties: {
		relationship_name: String,
		properties: String, // (JSON string containing additional DataPoint fields like ontology_valid, version, topological_rank, metadata)
	}
}
