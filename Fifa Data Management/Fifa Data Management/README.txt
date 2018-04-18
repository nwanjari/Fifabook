FIFA Book

**************************************************************************
SUMMARY
**************************************************************************
What is life without Goals? Well, we meant soccer goals. With thousands of players from hundreds of clubs around, whom to include in your playing squad can be quite a Messi (messy) affair. With FIFA Book, we aim to develop a tool that lets you move beyond your intuition and presents you with facts, numbers, and statistics to help you finalize your playing eleven. The tool has an inbuilt algorithm which analyzes skills data from over 20K player records and suggests the best team to play against your opponent. If you log in to FIFA Book, you could see a player's profile and career graphs based on historic performances across clubs. If you are a soccer player and want to improve your skills, why not log in to the system and let the tool identify your weak areas? The intended tool would recommend a workout and training routine to strengthen your weak spots and boost your performance. Further implementation would include creating a coaching schedule, maintaining a performance index for a group of new and emerging players and determining popularity of players on social media (popularity index).

**************************************************************************
IMPLEMENTATION
**************************************************************************
A list of all clubs available in the database is fetched and presented to the user. The user can select the club of his choice for which he/she wants to view the statistics. Based on the selected club, one can view the overall skills aggregate of the players according to their positions as Attack, Defense, Midfield and Goal Keeping.
A list of all players for the selected club is displayed. User can select a player of his choice to view the player's record and performance statistics.
Implementation of Map-Reduce is done while showing the nationalities of players from the selected club. Map-Reduce functionality is also used to display age wise distribution information of players in a team.
Player list is categorized as per position and additional player information such as height and weight have been included in the player dashboard to improve club managers' decisions.
The system also displays top 'n' players from a team based on their overall rating, where 'n' is input by the user. 
A club manager can see a report of players with their performance rating and contract expiration date, to support the decision of extending/shortening of legal contract for that player.
The overall application is robust to handle errors and wrong user input gracefully. 

**************************************************************************
SETUP
**************************************************************************
1. Import the gradle project and select run from the gradle tasks
2. Enter club name from the list presented
3. Select the menu option for the functionality which you want to view
4. Enter player name from the list presented to view statistics for the player
