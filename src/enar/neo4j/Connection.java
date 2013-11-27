package enar.neo4j;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Connection {

	private final GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase("mydb");
	private final ExecutionEngine engine = new ExecutionEngine(db);
	
	public Transaction startTransation() {
		return db.beginTx();
	}
	
	public Node createNode() {
		return db.createNode();
	}
	
	public ExecutionResult query(String query) {
		return engine.execute(query);
	}
	
	public void finalise() {
		Transaction transaction = startTransation();
	
		try {
			for (Node n : db.getAllNodes()) {
				for (Relationship r : n.getRelationships()) {
					r.delete();
				}
				n.delete();
			}
			transaction.success();
			
		} finally {
			transaction.finish();
		}
        
        db.shutdown();
	}
}
