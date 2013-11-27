package enar.neo4j;

import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

/*
 * This class demonstrates a very simple usage of the 
 * Neo4J document store. Note that this code uses a 
 * single database node. (How might this differ in a 
 * production environment?) 
 * 
 * Run this code using the Neo4J.launch configuration
 * in Eclipse. (Right-click the .launch file and then select 
 * Run as > Neo4J). 
 * 
 * The Neo4J manual is very good:
 * http://docs.neo4j.org/chunked/stable/index.html
 * 
 * The section on Cypher (the Neo4J query language) will be
 * helpful when working with this code:
 * http://docs.neo4j.org/chunked/stable/cypher-query-lang.html
 */
@SuppressWarnings("unused")
public class Neo4J {

	public static void main(String[] args) {
		new Neo4J().run();
	}

	// Connect to the Neo4J database
	private Connection connection = new Connection();

	public void run() {
	try {
		// Initialise the database with some data
		// Take a look at this code, and draw a picture of the graph that is
		// being constructed.
		createGraph();

		// Next, read this code, run it, change and rerun it
		simpleQuery();
		
		// Once you're comfortable with the simpleQuery method, comment in this
		// method, read the source, run it, change and rerun it, etc.
//		matchQuery();
		
		// Now think about and research:
		// -- how you would specify cypher queries that update (write) to the store?
		// -- how you would use the traversal framework to compute the future team mates
		//    of a footballer who is wanted by another football team. For example, the
		//    future team mates of Tom Cleverley would be Andros Townsend and Robert
		//    Soldado.

		} finally {
			// This code ensures that we dispose of the connection to the database
			// even if an exception is raised during the execution of our program
			connection.finalise();
		}
	}

	private void createGraph() {
		Transaction tx = connection.startTransation();
		try {

			// Create several football players
			Node soldado = createFootballer("Robert Soldado");
			Node townsend = createFootballer("Andros Townsend");
			Node rooney = createFootballer("Wayne Rooney");
			Node evra = createFootballer("Patrice Evra");
			Node cleverley = createFootballer("Tom Cleverley");

			// Create a couple of football teams
			Node spurs = createFootballTeam(
				"Tottenham Hotspur",
				"White Hart Lane",
				soldado,
				townsend
			);

			Node manUtd = createFootballTeam(
				"Manchester United",
				"Old Trafford",
				rooney,
				evra,
				cleverley
			);

			// Add additional relationships between teams and players
			// that they would like to buy
			createTransferTarget(manUtd, townsend);
			createTransferTarget(spurs, cleverley);

			tx.success();
		} finally {
			tx.finish();
		}
	}
	
	private void simpleQuery() {
		// Use the Cypher query language to find out which teams have which players
		// Note the the HAS relationship is defined below (in the FootballRelationships
		// enumeration) and that the createFootballTeam method establishes these
		// relationships
		ExecutionResult result = connection
				.query("start p=node(*) match (t)-[:HAS]->(p) return t.name, p.name");
		
		// The result of the query is a set of rows, where each row is a map
		// (called a dictionary or associative array in other programming languages).
		// The map is keyed on the t.name and p.name as these are the return values
		// specified in our query, above.
		for (Map<String, Object> row : result) {
			Object playerName = row.get("p.name");
			Object currentTeamName = row.get("t.name");
			
			String message = String.format("%s plays for %s.", playerName, currentTeamName);
			System.out.println(message);
		}
	}
	
	private void matchQuery() {
		// Use the Cypher query language to find out which teams want to buy which players.
		// Note the the WANTS relationship is defined below (in the FootballRelationships
		// enumeration) and that the createTransferTarget method establishes these
		// relationships
		ExecutionResult result = connection
				.query("start p=node(*) match (t)-[:HAS]->(p)<-[:WANTS]-(nt) where t <> nt return t.name, p.name, nt");

		// Print out the transfer rumours according to the WANTS relationship
		for (Map<String, Object> row : result) {
			Object playerName = row.get("p.name");
			Object currentTeamName = row.get("t.name");
			
			Node newTeam = (Node) row.get("nt");
			Object targetGround = newTeam.getProperty("ground");
			Object targetName = newTeam.getProperty("name");
			
			String message = String.format("%s could be on his way to %s as %s prepare a bid for the %s player.", playerName, targetGround, targetName, currentTeamName );
			System.out.println(message);
		}
	}

	private Node createFootballTeam(String name, String ground, Node... players) {
		Node n = connection.createNode();
		n.setProperty("name", name);
		n.setProperty("ground", ground);

		for (Node player : players) {
			n.createRelationshipTo(player, FootballRelationships.HAS);
		}

		return n;
	}

	private Node createFootballer(String name) {
		Node n = connection.createNode();
		n.setProperty("name", name);
		return n;
	}

	private void createTransferTarget(Node team, Node player) {
		team.createRelationshipTo(player, FootballRelationships.WANTS);
	}

	private static enum FootballRelationships implements RelationshipType {
		HAS, WANTS;
	}
}
