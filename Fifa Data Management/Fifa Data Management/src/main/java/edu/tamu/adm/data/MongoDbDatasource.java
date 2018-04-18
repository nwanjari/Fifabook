package edu.tamu.adm.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;

/**
 * DataSource class for creating connection to MongoDB instance.
 * For creating collections, inserting, deleting, fetching documents.
 *
 */
public class MongoDbDatasource {

	private static MongoClient mongo;
	private static DB db;
	private static DBCollection collection;

	private MongoDbDatasource() {

	}

	/*
	 * method to create a connection to MongoDb local host on default port
	 * 
	 */
	public static MongoClient createMongoDbInstance() {
		System.out.println("***************Start of method getMongoDbInstance*******************");
		if (null == mongo)
			mongo = new MongoClient("localhost", 27017);
		System.out.println("***************End of method getMongoDbInstance*******************");

		return mongo;

	}

	/*
	 * @input: database name and collection name to be created method to create
	 * database and collection in it
	 */
	public static DBCollection createDatabaseCollection(String databaseName, String collectionName) {
		System.out.println("***************Start of method createDatabaseCollection*******************");

		db = mongo.getDB(databaseName);
		collection = db.getCollection(collectionName);
		System.out.println("***************End of method init*******************");
		return collection;
	}

	/*
	 * method to delete all records from a collection
	 */
	public static void deleteDatabaseCollection() {
		System.out.println("***************Start of method deleteData*******************");

		WriteResult result = collection.remove(new BasicDBObject());
		System.out.println(result.toString());
		System.out.println("***************End of method deleteData*******************");

	}

	/*
	 * @input: data file method to insert records in the collection from a data file
	 */
	public static void insertData(File file) throws FileNotFoundException, IOException, ParseException {
		System.out.println("***************Start of method insertData*******************");

		JSONParser parser = new JSONParser();
		Object obj = parser.parse(new FileReader(file));

		JSONArray jsonObject = (JSONArray) obj;
		BasicDBList data = (BasicDBList) JSON.parse(jsonObject.toString());
		for (int i = 0; i < data.size(); i++) {
			collection.insert((DBObject) data.get(i));
		}
		System.out.println("***************End of method insertData*******************");

	}

	/*
	 * method to fetch all the documents from the collection
	 */
	public static void fetchAllData() {
		System.out.println("***************Start of method fetchData*******************");

		DBCursor cursor = collection.find();

		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}

		System.out.println("***************End of method fetchData*******************");

	}

	
	

	/*
	 * @input: team name method to display team wise aggregate: Defence, MidField,
	 * Attack, GoalKeeping
	 */
	public static void fetchTeamAggregates(String teamName) {
		ArrayList<String> playerNames = null;
		BasicDBObject gtQuery = new BasicDBObject();
		gtQuery.put("Club", new BasicDBObject("$eq", teamName));
		DBCursor cursor = collection.find(gtQuery);
		cursor.sort(new BasicDBObject("Rating", -1));

		int sumDefence = 0;
		int sumMidField = 0;
		int sumAttack = 0;
		int sumGoalKeeping = 0;
		int counterDefence = 0;
		int counterMidField = 0;
		int counterAttack = 0;
		int counterGoalKeeping = 0;
		List<String> DefenceSkillsList = Arrays.asList("Ball_Control", "Dribbling", "Marking", "Sliding_Tackle",
				"Standing_Tackle", "Aggression", "Reactions", "Interceptions", "Vision", "Composure", "Short_Pass",
				"Long_Pass", "Acceleration", "Speed", "Stamina", "Strength", "Balance", "Agility", "Jumping", "Heading",
				"Shot_Power", "Long_Shots", "Volleys");
		List<String> MidFieldSkillsList = Arrays.asList("Ball_Control", "Dribbling", "Marking", "Sliding_Tackle",
				"Standing_Tackle", "Aggression", "Reactions", "Attacking_Position", "Interceptions", "Vision",
				"Composure", "Crossing", "Short_Pass", "Long_Pass", "Acceleration", "Speed", "Stamina", "Strength",
				"Balance", "Agility", "Jumping", "Heading", "Long_Shots", "Curve", "Freekick_Accuracy", "Penalties",
				"Volleys");
		List<String> AttackSkillsList = Arrays.asList("Ball_Control", "Dribbling", "Standing_Tackle", "Reactions",
				"Attacking_Position", "Interceptions", "Vision", "Composure", "Crossing", "Short_Pass", "Acceleration",
				"Speed", "Stamina", "Aggression", "Strength", "Balance", "Agility", "Jumping", "Heading", "Shot_Power",
				"Finishing", "Long_Shots", "Curve", "Freekick_Accuracy", "Penalties", "Volleys");
		List<String> GoalKeeperSkillsList = Arrays.asList("Reactions", "Composure", "Jumping", "Shot_Power",
				"Long_Shots", "GK_Reflexes", "GK_Positioning", "GK_Kicking", "GK_Handling", "GK_Diving");

		if (cursor.count() == 0) {
			System.out.println(" NO RESULTS FOUND !! ");
			return;
		}
		HashMap<String, List<String>> mapPlayerCategory= new HashMap<String, List<String>>();
		
		while (cursor.hasNext()) {

			DBObject dbObj = cursor.next();
			String prefPosition = (String) dbObj.get("Preffered_Position");
			String lastLetter = prefPosition.substring(prefPosition.length() - 1);

			if ("B".equalsIgnoreCase(lastLetter)) {
				Integer defence = 0;
				for (String temp : DefenceSkillsList) {
					defence += (Integer) dbObj.get(temp);
				}
				sumDefence += defence;
				counterDefence++;
				
				if(mapPlayerCategory.containsKey("Defence"))
				{
					List<String> list=mapPlayerCategory.get("Defence");
					list.add((String) dbObj.get("Name"));
				}
				else
				{
					playerNames = new ArrayList<String>();
					playerNames.add((String) dbObj.get("Name"));
					mapPlayerCategory.put("Defence", playerNames);

				}
				
			}

			if ("M".equalsIgnoreCase(lastLetter)) {
				Integer midField = 0;
				for (String temp : MidFieldSkillsList) {
					midField += (Integer) dbObj.get(temp);
				}
				sumMidField += midField;
				counterMidField++;
				
				if(mapPlayerCategory.containsKey("MidField"))
				{
					List<String> list=mapPlayerCategory.get("MidField");
					list.add((String) dbObj.get("Name"));
				}
				else
				{
					playerNames = new ArrayList<String>();
					playerNames.add((String) dbObj.get("Name"));
					mapPlayerCategory.put("MidField", playerNames);

				}
			}

			if ("F".equalsIgnoreCase(lastLetter) || "S".equalsIgnoreCase(lastLetter) || "T".equalsIgnoreCase(lastLetter)
					|| "W".equalsIgnoreCase(lastLetter)) {
				Integer attack = 0;
				for (String temp : AttackSkillsList) {
					attack += (Integer) dbObj.get(temp);
				}
				sumAttack += attack;
				counterAttack++;
				Integer I = AttackSkillsList.size();
				
				if(mapPlayerCategory.containsKey("Attack"))
				{
					List<String> list=mapPlayerCategory.get("Attack");
					list.add((String) dbObj.get("Name"));
				}
				else
				{
					playerNames = new ArrayList<String>();
					playerNames.add((String) dbObj.get("Name"));
					mapPlayerCategory.put("Attack", playerNames);

				}
			}

			if ("K".equalsIgnoreCase(lastLetter)) {
				Integer goalKeeping = 0;
				for (String temp : GoalKeeperSkillsList) {
					goalKeeping += (Integer) dbObj.get(temp);
				}
				sumGoalKeeping += goalKeeping;
				counterGoalKeeping++;
				
				if(mapPlayerCategory.containsKey("GoalKeeping"))
				{
					List<String> list=mapPlayerCategory.get("GoalKeeping");
					list.add((String) dbObj.get("Name"));
				}
				else
				{
					playerNames = new ArrayList<String>();
					playerNames.add((String) dbObj.get("Name"));
					mapPlayerCategory.put("GoalKeeping", playerNames);

				}
			}
		}
		
		float avgDefence=0F;
		float avgMidField=0F;
		float avgAttack=0F;
		float avgGoalKeeping=0F;
		if( DefenceSkillsList.size()!=0 && counterDefence!=0)
		{
		 avgDefence = sumDefence / (counterDefence * DefenceSkillsList.size());
		}
		
		if( MidFieldSkillsList.size()!=0 && counterMidField!=0)
		{
		avgMidField = sumMidField / (counterMidField * MidFieldSkillsList.size());
		}
		
		if( AttackSkillsList.size()!=0 && counterAttack!=0)
		{
		 avgAttack = sumAttack / (counterAttack * AttackSkillsList.size());
		}
		
		
		if( GoalKeeperSkillsList.size()!=0 && counterGoalKeeping!=0)
		{
		 avgGoalKeeping = sumGoalKeeping / (counterGoalKeeping * GoalKeeperSkillsList.size());
		}
		
		System.out.println();
		System.out.println("*******************************Team Aggregate*****************************");
		System.out.printf("Average Defence of Team  = %20.4s\n", avgDefence);
		System.out.printf("Average Mid Field of Team = %20.4s\n", avgMidField);
		System.out.printf("Average Attack of Team = %20.4s\n", avgAttack);
		System.out.printf("Average Goal Keeping of Team = %20.4s\n", avgGoalKeeping);
		System.out.println();
		cursor.close();
		
		
		proceed(teamName,mapPlayerCategory);
	
	}

	/* @input team name, player list along with category
	 * method to display menu of choices to the user
	 */
	private static void proceed(String teamName, HashMap<String, List<String>> mapPlayerCategory) 
	{
		System.out.println();
		System.out.println(" *** Enter 1 for Nationality Wise Representation Via Map Reduce ***");
		System.out.println(" *** Enter 2 for Age Wise Representation Via Map Reduce ***");
		System.out.println(" *** Enter 3 for Top Rated Players ***");
		System.out.println(" *** Enter 4 for Full Player List ***");
		System.out.println(" *** Enter 5 for Contract Expiry Details ***");

		System.out.println(" *** Enter 6 for Exit ***");

		Scanner scanner = new Scanner(System.in);
		String input=scanner.nextLine();

		if("1".equals(input))
		printNationalityRepresentation(teamName, mapPlayerCategory);
		
		else if("2".equals(input))
		displayAgeWiseCountOfPlayers(teamName, mapPlayerCategory);
		
		else if("3".equals(input))
		{
			System.out.println(" Enter number of top players you want to see::");
			String num=scanner.nextLine();
		    printTopPlayers(teamName,mapPlayerCategory, num);
		}
		else if("4".equals(input))
		{
			displayPlayerList(teamName, mapPlayerCategory);

		}
		else if("5".equals(input))
		{
		    printFreshFaces(teamName,mapPlayerCategory);
		}
		else if("6".equals(input))
		{
			System.out.println("Exiting!!");
			return;

		}
		else 
		System.out.println("Found unexpected input!!");
		System.out.println();
		
		
	}

	private static void printFreshFaces(String teamName, HashMap<String, List<String>> mapPlayerCategory) {
		BasicDBObject gtQuery = new BasicDBObject();
		gtQuery.put("Club", new BasicDBObject("$eq", teamName));
		DBCursor cursor = collection.find(gtQuery);
		cursor.sort(new BasicDBObject("Contract_Expiry", -1));


		System.out.println();
		System.out.println("**********************************Contract Expiry Details*************************************");
		System.out.println();

		System.out.println("Player Name			Contract Expiry Date 		Rating");

		while (cursor.hasNext()) 
		{
			DBObject dbObj = cursor.next();
			Integer contractExpiry=(Integer)dbObj.get("Contract_Expiry");
			String name = (String) dbObj.get("Name");
			Integer rating = (Integer) dbObj.get("Rating");
			System.out.println(name.trim() + "		 	" + contractExpiry+"					"+rating);
		}

		System.out.println();
		System.out.println();
		cursor.close();
		proceed(teamName,mapPlayerCategory);
		
	}

	/*
	 * @input team name
	 * method to find count of players across different age groups
	 * using Map Reduce
	 */
	private static void displayAgeWiseCountOfPlayers(String teamName, HashMap<String, List<String>> mapPlayerCategory) 
	{
		BasicDBObject gtQuery = new BasicDBObject();
		gtQuery.put("Club", new BasicDBObject("$eq", teamName));
		DBCursor cursor = collection.find(gtQuery);
		cursor.sort(new BasicDBObject("Rating", -1));

		String map = "function() {emit(this.Age, 1);}";
		String reduce = "function(key,values) {return Array.sum(values)}";

		MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce, null, MapReduceCommand.OutputType.INLINE,
				gtQuery);

		MapReduceOutput out = collection.mapReduce(cmd);
		System.out.println();
		System.out.println("*****************************Age Represented*********************************");
		System.out.println();
		HashMap<String, Double> ageMap= new HashMap<String, Double>();
		for (DBObject o : out.results()) 
		{
			Double age=(Double) o.get("_id");
			Double count= (Double) o.get("value");
			if(age< 20)
			{
				if (ageMap.containsKey("<20"))
				{
					double ct=ageMap.get("<20");
					ageMap.put("<20", ct+count);
				}
				else
				{
					ageMap.put("<20", count);
				}
				
			}
			else if(age <=25)
			{
			
				if (ageMap.containsKey("21-25"))
				{
					double ct=ageMap.get("21-25");
					ageMap.put("21-25", ct+count);
				}
				else
				{
					ageMap.put("21-25", count);
				}
			}
			else if(age<=30)
			{
				if (ageMap.containsKey("26-30"))
				{
					double ct=ageMap.get("26-30");
					ageMap.put("26-30", ct+count);
				}
				else
				{
					ageMap.put("26-30", count);
				}
			}
			else if(age<=35)
			{
				if (ageMap.containsKey("31-35"))
				{
					double ct=ageMap.get("31-35");
					ageMap.put("31-35", ct+count);
				}
				else
				{
					ageMap.put("31-35", count);
				}
			}
			else
			{
				if (ageMap.containsKey(">36"))
				{
					double ct=ageMap.get(">36");
					ageMap.put(">36", ct+count);
				}
				else
				{
					ageMap.put(">36", count);
				}
				
			}
							
		}
		cursor.close();
		
		
		for(String key: ageMap.keySet())
		{
			System.out.println("Age Group ( "+key+" ) Number of Players ::"+ageMap.get(key));
		}
		proceed(teamName,mapPlayerCategory);
	}

	/*
	 * @input: team name 
	 * method to print top players in a team by rating
	 */
	private static void printTopPlayers(String teamName, HashMap<String, List<String>> mapPlayerCategory, String number) {
		BasicDBObject gtQuery = new BasicDBObject();
		gtQuery.put("Club", new BasicDBObject("$eq", teamName));
		DBCursor cursor = collection.find(gtQuery);
		cursor.sort(new BasicDBObject("Rating", -1));

		cursor.limit(Integer.parseInt(number));

		System.out.println();
		System.out.println("**********************************Top Rated Players*************************************");
		System.out.println();

		while (cursor.hasNext()) {
			DBObject dbObj = cursor.next();
			String name = (String) dbObj.get("Name");
			Integer rating = (Integer) dbObj.get("Rating");
			System.out.println(" Player:: "+name.trim() + "  Rating::  " + rating.toString().trim());
		}

		System.out.println();
		System.out.println();
		cursor.close();
		proceed(teamName,mapPlayerCategory);
	}

	/*
	 * @input: team name 
	 * method to display count of players of different nationalities in a team
	 * using Map Reduce
	 */
	private static void printNationalityRepresentation(String teamName, HashMap<String, List<String>> mapPlayerCategory) {
		BasicDBObject gtQuery = new BasicDBObject();
		gtQuery.put("Club", new BasicDBObject("$eq", teamName));
		DBCursor cursor = collection.find(gtQuery);
		cursor.sort(new BasicDBObject("Rating", -1));

		String map = "function() {emit(this.Nationality, 1);}";
		String reduce = "function(key,values) {return Array.sum(values)}";

		MapReduceCommand cmd = new MapReduceCommand(collection, map, reduce, null, MapReduceCommand.OutputType.INLINE,
				gtQuery);

		MapReduceOutput out = collection.mapReduce(cmd);
		System.out.println("*******************************Nationalities represented********************************");
		for (DBObject o : out.results()) {
			System.out.println(o.get("_id") + " :: " + o.get("value"));
		}
		cursor.close();
		
		proceed(teamName,mapPlayerCategory);

	}

	/*
	 * @input: club name and player list, method to display all players of a
	 * particular team
	 */

	private static void displayPlayerList(String club, HashMap<String, List<String>> playerNames)
	{

		System.out.println("********************All players in this team******************************");
		System.out.println();

		for (String key : playerNames.keySet()) 
		{
			
			System.out.println(" Category **"+key+"**");
			System.out.println();
			List<String> playerList= playerNames.get(key);
			for(String player:playerList)
			{
				System.out.println(player);
			}
			System.out.println();
		}

		System.out.println();
		Scanner scanner = new Scanner(System.in);

		System.out.println(" Enter your player name from the list above :: ");
		
     	String player = scanner.nextLine();		
		fetchPlayerData(club, player.trim());
		

	}

	/*
	 * @input: team name and player name, method to fetch players for a team
	 * method to fetch player statistics
	 */
	private static void fetchPlayerData(String team, String player) {
		BasicDBObject gtQuery = new BasicDBObject();
		gtQuery.put("Name", new BasicDBObject("$eq", player));
		gtQuery.put("Club", new BasicDBObject("$eq", team));
		DBCursor cursor = collection.find(gtQuery);

		if (cursor.count() == 0) {
			System.out.println(" NO RESULTS FOUND !! ");
			return;
		}

		while (cursor.hasNext()) {
			DBObject dbObj = cursor.next();
			Integer Stamina = (Integer) dbObj.get("Stamina");
			Integer Strength = (Integer) dbObj.get("Strength");
			Integer Acceleration = (Integer) dbObj.get("Acceleration");
			Integer Speed = (Integer) dbObj.get("Speed");
			Integer club_kit = (Integer) dbObj.get("Club_Kit");
			Integer age = (Integer) dbObj.get("Age");
			String height = (String) dbObj.get("Height");
			String weight = (String) dbObj.get("Weight");
			

			System.out.println("*********************Player Statistics**********************");
			System.out.println();
			System.out.println("Stamina::" + Stamina);
			System.out.println("Strength::" + Strength);
			System.out.println("Acceleration::" + Acceleration);
			System.out.println("Speed::" + Speed);
			System.out.println("Jersey Number::" + club_kit);
			System.out.println("Age::" + age);
			System.out.println("Height::" + height);
			System.out.println("Weight::" + weight);

		}
		
	
		
	}

	/*
	 * @input: field name whose values are to be fetched from the database, method to
	 * fetch distinct club names from the database
	 */
	public static List<String> loadFieldValues(String fieldName) {

		List<String> list = collection.distinct("Club");
		Collections.sort(list);
		return list;

	}
	
	
	/*
	 * method to disconnect from the database
	 */
	public static void disconnect() {
		System.out.println("***************Start of method closeResources*******************");

		mongo.close();
		System.out.println("***************End of method closeResources*******************");
	}
	
	

}
