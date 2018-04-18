package edu.tamu.adm;

import java.io.File;
import java.util.List;
import java.util.Scanner;

import edu.tamu.adm.data.MongoDbDatasource;

public class Main {

	public static void main(String[] args) throws Exception {
		try {

			MongoDbDatasource.createMongoDbInstance();
			MongoDbDatasource.createDatabaseCollection("FIFA", "FIFA_Collection");
			MongoDbDatasource.deleteDatabaseCollection();
			File file = new File("dataFiles/data.json");
			MongoDbDatasource.insertData(file);
			List<String> clubList = MongoDbDatasource.loadFieldValues("Club");
			getInputFromUser(clubList);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			MongoDbDatasource.disconnect();
		}

	}

	

	/*
	 * @input: list of clubs from the database method displays list of clubs user
	 * enters a club name from the list
	 * 
	 * @return : club name
	 */
	private static void getInputFromUser(List<String> clubList) {
		Scanner scanner = new Scanner(System.in);
		System.out.println();
		System.out.println(" Welcome To FIFA-BOOK !! ");
		System.out.println(" Select your favorite club from the following:: ");
		System.out.println( );

		for (String club : clubList) {
			System.out.println(club);
		}
		
		String club = scanner.nextLine();
		MongoDbDatasource.fetchTeamAggregates(club.trim());


	}

}
