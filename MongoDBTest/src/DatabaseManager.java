import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import models.Actor;
import models.Movie;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class DatabaseManager {
	
	private DB db;
	private DBCollection movies;
	private DBCollection actors;
	private MongoClient mongoClient;

	public DatabaseManager(String nameDB) throws UnknownHostException {

		connectToDB(nameDB);
		movies = db.getCollection("movies");
		actors = db.getCollection("actors");
	}

	@SuppressWarnings("deprecation")
	public void connectToDB(String nameDB) throws UnknownHostException {
		
		mongoClient = new MongoClient("localhost", 27017);
		db = mongoClient.getDB(nameDB);
		
	}
	
	public void closeConnection () {
		mongoClient.close();
	}
	
	public DBCollection getMovies() {
		return movies;
	}

	public void setMovies(DBCollection movies) {
		this.movies = movies;
	}

	public DBCollection getActors() {
		return actors;
	}

	public void setActors(DBCollection actors) {
		this.actors = actors;
	}

	public void insertMovie(Movie movie) {

		BasicDBObject newObject = new BasicDBObject();
		newObject.append("_id", movie.getId())
				 .append("name", movie.getName())
				 .append("year", movie.getYear())
				 .append("actors", movie.getActors());

		movies.insert(newObject);

	}

	public void insertActor(Actor actor) {
		BasicDBObject newObject = new BasicDBObject();
		newObject.put("_id", actor.getId());
		newObject.append("name", actor.getName())
				 .append("description", actor.getDescription())
			     .append("dateBirth", actor.getDateBirth());

		actors.insert(newObject);
	}

	public void showCollection(DBCollection collectionName) {
		DBCursor cursor = collectionName.find();
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}

	public void indexCollectionField(DBCollection collectionName, String field) {
		BasicDBObject indexObject = new BasicDBObject(field, 1);
	    collectionName.createIndex(indexObject);
	}

	// sortingField = year
	public DBCursor sortCollectionBy(DBCollection collectionName,String sortingField, int limitNumber) {
		
		return collectionName.find().sort(new BasicDBObject(sortingField, 1)).limit(limitNumber);
	}

	private DBCursor getLimitActors(int limit) {
		return actors.find().limit(limit);
	}
	
	private DBCursor getLimitMovies(int limit) {
		return movies.find().limit(limit);
	}
	
	private BasicDBObject fieldsToshow(String field1,String field2,int showId) {
		BasicDBObject fields = new BasicDBObject(field1, 1).append(field2, 1).append("_id", showId);
		return fields;
	}
	

	public void printActorsMovies(DBCursor actorsCursor) {
		
		//actorsCursor = getLimitActors(2);
		BasicDBObject fields = fieldsToshow("name", "year", 0);
		while (actorsCursor.hasNext()) {
			
			Object actorId = actorsCursor.next().get("_id");
			DBCursor foundedMovies = movies.find(new BasicDBObject("actors",actorId),fields);
			
			System.out.println(foundedMovies.toArray());

		}
	}

	public DBCursor sortActorsFromMoviesBy(String sortingField,int limit) {
		Set<Object> set = new HashSet<Object>();
		BasicDBObject fields = new BasicDBObject("_id",0).append("actors", 1);
		
		DBCursor searchedMovies = movies.find(new BasicDBObject(),fields).limit(limit);
		
		while (searchedMovies.hasNext()) {
			List<Object> currMovieActors = (List<Object>) searchedMovies.next().get("actors");
			set.addAll(currMovieActors);
		}
		BasicDBObject query =new BasicDBObject("_id",new BasicDBObject("$in",set));
	    DBCursor sortedActors = actors.find(query).sort(new BasicDBObject(sortingField,1));
	    
	    return sortedActors;
		
	}
	
	public void deleteActors(DBCursor actorsCursor) {
		//actorsCursor = getLimitActors(2);
		while (actorsCursor.hasNext()) {
			BasicDBObject docToDelete = new BasicDBObject("_id",actorsCursor.next().get("_id"));
			
			Object toBeDeleted = actorsCursor.next().get("_id");
			BasicDBObject query = new BasicDBObject("actors",toBeDeleted);
			movies.updateMulti(query,new BasicDBObject("$pull", query));
			actors.remove(docToDelete);
			
		}
	}

	
	public void changeActorID(DBObject actor,String id) {
		//actor = actors.findOne();
		
			BasicDBObject chengedActor = new BasicDBObject();
			chengedActor.append("_id", id)
				    	.append("name", actor.get("name"))
				    	.append("description", actor.get("description"))
				    	.append("dateBirth", actor.get("dateBirth"));
		
			actors.remove(actor);
			actors.insert(chengedActor);

			Object searchedActor = actor.get("_id");
			BasicDBObject searchQuery = new BasicDBObject("actors",searchedActor);
			BasicDBObject elemMatch = new BasicDBObject("actors.$",id);
			BasicDBObject updateQuery =  new BasicDBObject("$set",elemMatch);
		
			movies.updateMulti(searchQuery, updateQuery);

	}
	
	public List<String> getMoviesIds(DBCursor movies) {
		List<String> listOfIds = new ArrayList<String>();
		while (movies.hasNext()) {
			String currId = (String) movies.next().get("_id");
			listOfIds.add(currId);
			System.out.println(currId);
		}
		System.out.println(listOfIds);
		return listOfIds;
	}
	
	
	public void insertActorToMovies (DBCursor moviesCursor,Actor newActor) {
		String actorId = newActor.getId();
		List<String> searchedMovies = getMoviesIds(moviesCursor);
		
		BasicDBObject pushQuery = new BasicDBObject("$push", new BasicDBObject("actors",actorId));
		BasicDBObject searchQuery = new BasicDBObject("_id",new BasicDBObject("$in",searchedMovies));
		System.out.println(searchQuery);
		System.out.println(pushQuery);
		movies.updateMulti(searchQuery,pushQuery);
		
	}
	

}
