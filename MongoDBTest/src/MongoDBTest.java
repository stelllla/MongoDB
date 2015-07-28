import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import models.Actor;
import models.Movie;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

	public class MongoDBTest {
		
		public static void insertData(DatabaseManager manager) {
			
			Calendar cal = Calendar.getInstance();
			cal.set(1990, 5, 24);
			Date date =cal.getTime();
			Actor newActor = new Actor("1","Jonnie Depth", "gotin si e", date);
			manager.insertActor(newActor);
			cal.set(1988, 9, 4);
			date =cal.getTime();
			newActor.changeActor("2","Brad Pitt", "i toi stava", date);
			manager.insertActor(newActor);
			cal.set(1976, 10, 31);
			date =cal.getTime();
			newActor.changeActor("3","Hilary Duff", "sladuranka e", date);
			manager.insertActor(newActor);
			cal.set(1968, 3, 14);
			date =cal.getTime();
			newActor.changeActor("4","Asen Blatechki", "pich e asencho", date);
			manager.insertActor(newActor);
			cal.set(1991, 12, 13);
			date =cal.getTime();
			newActor.changeActor("5","Dilqna Popova", "dosta e dobre daje", date);
			manager.insertActor(newActor);
			cal.set(1999, 6, 9);
			date =cal.getTime();
			newActor.changeActor("6","Angelina Jolie", "tatosi matosi", date);
			manager.insertActor(newActor);
			cal.set(1986, 7, 8);
			date =cal.getTime();
			newActor.changeActor("7","Dwayne Johnson", "Skalata", date);
			manager.insertActor(newActor);
			

			List<String> actorsIds = new ArrayList<String>();
			actorsIds.add("1");
			actorsIds.add("2");
			Movie newMovie = new Movie ("1","Hakuna Matata", 2000, actorsIds);
			manager.insertMovie(newMovie);
			newMovie.changeMovie("2","Tinkerbell", 2015, actorsIds);
			manager.insertMovie(newMovie);
			actorsIds.add("4");
			newMovie.changeMovie("3","Monions", 2010, actorsIds);
			manager.insertMovie(newMovie);
			
			actorsIds.remove(1);
			actorsIds.add("6");
			newMovie.changeMovie("4","Nepobedimite", 1999, actorsIds);
			manager.insertMovie(newMovie);
			actorsIds.add("3");
			newMovie.changeMovie("5","Mean Girls", 1960, actorsIds);
			manager.insertMovie(newMovie);
			actorsIds.remove(2);
			actorsIds.add("7");
			newMovie.changeMovie("6","Batman bez Robin", 2016, actorsIds);
			manager.insertMovie(newMovie);
			
			actorsIds.add("5");
			newMovie.changeMovie("7","Umirai trudno 1", 2008, actorsIds);
			manager.insertMovie(newMovie);
			actorsIds.remove(3);
			newMovie.changeMovie("8","TED", 1890, actorsIds);
			manager.insertMovie(newMovie);
			actorsIds.remove(1);
			newMovie.changeMovie("9","The Hunger Games", 2014, actorsIds);
			manager.insertMovie(newMovie);
			
			actorsIds.remove(1);
			newMovie.changeMovie("10","50 shades of gray", 2017, actorsIds);
			manager.insertMovie(newMovie);

		}

		public static void main(String[] args) throws UnknownHostException {

			DatabaseManager manager = new DatabaseManager("test");
			DBCollection movies = manager.getMovies();
			DBCollection actors = manager.getActors();
			
			//1,2,3
			//insertData(manager);

			//4
			manager.indexCollectionField(movies, "year");
			manager.indexCollectionField(actors, "dateBirth");
			
			//5
			DBCursor result = manager.sortCollectionBy(movies, "year", 4);
//			while (result.hasNext()) {
//			System.out.println(result.next());
//		    }	
			
			//6
			result = manager.sortActorsFromMoviesBy("dateBirth", 3);
//			while (result.hasNext()) {
//				System.out.println(result.next());
//			}
			
			//7
			BasicDBList or =  new BasicDBList();
			or.add(new BasicDBObject("name","Brad Pitt"));
			or.add(new BasicDBObject("name","Hilary Duff"));
			DBObject orQuery = new BasicDBObject("$or",or);
			DBCursor actorsCursor = actors.find(orQuery).limit(2);
			//manager.printActorsMovies(actorsCursor);
			
			
			//8
			manager.deleteActors(actorsCursor);
			
			//9
			DBObject actor = actors.findOne(new BasicDBObject("name","Jonnie Depth"));
			manager.changeActorID(actor,"123456789101");
			
			
			//10
			BasicDBList orList =  new BasicDBList();
			orList.add(new BasicDBObject("name","TED"));
			orList.add(new BasicDBObject("name","The Hunger Games"));
			DBObject orQueryList = new BasicDBObject("$or",orList);
			DBCursor moviesCursor = movies.find(orQueryList); 
			
			Calendar cal = Calendar.getInstance();
			cal.set(1900,1,1);
			Date date = cal.getTime();
			Actor newActor = new Actor("1111","Noviq","emi nov si e",date);
			manager.insertActorToMovies(moviesCursor, newActor);
			
			
			//movies.drop();
			//actors.drop();
			manager.closeConnection();
			
			}
	}


