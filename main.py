import psycopg2
import matplotlib.pyplot as plt
import pandas as pd

db_params = {
    'host': 'localhost',
    'database': 'bd',
    'user': 'postgres',
    'password': 'byebye'
}

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

if __name__ == "__main__":
    # Перший запит: виведення глобальних продажів ігор відсортованих за роком виходу
    query1 = """
    CREATE VIEW global_saling AS
    (
        SELECT v.name, Year_of_Release as Release_Year, SUM(Global_Sales_in_Millions) as Total_Global_Sales
        FROM Video_Game as v
        JOIN Global_Sales as g ON v.Indexx = g.Indexx
        GROUP BY Year_of_Release, name
        ORDER BY Release_Year
    );
    """
    cursor.execute('DROP VIEW IF EXISTS global_saling;')
    cursor.execute(query1)
    cursor.execute('SELECT * FROM global_saling LIMIT 20;')
    result1 = cursor.fetchall()

    # Другий запит: підрахунок скільки різних ігор відповідають різним жанрам
    query2 = """
    CREATE VIEW game_genre AS
    (
        SELECT Genre, COUNT(*) as Genre_Count
        FROM Video_Game
        GROUP BY Genre
    );
    """
    cursor.execute('DROP VIEW IF EXISTS game_genre;')
    cursor.execute(query2)
    cursor.execute('SELECT * FROM game_genre;')
    result2 = cursor.fetchall()

    # Третій запит: вивід ігор і їх оцінок від критиків і гравців
    query3 = """
    CREATE VIEW game_score AS
    (
        SELECT Name, Critic_Score, User_Score
        FROM Video_Game
    );
    """

    cursor.execute('DROP VIEW IF EXISTS game_score;')
    cursor.execute(query3)
    cursor.execute('SELECT * FROM game_score LIMIT 20;')
    result3 = cursor.fetchall()

    cursor.close()
    conn.close()

names = [item[0] for item in result1]
years = [item[1] for item in result1]
sales = [float(item[2]) for item in result1]

fig, (f1, f2, f3) = plt.subplots(1, 3, figsize=(20, 10))


f1.bar(names, sales, color='skyblue')
f1.set_xlabel('Game Name')
f1.set_ylabel('Total Global Sales (Millions)')
f1.set_title('Total Global Sales per Game')
f1.set_xticklabels(names, rotation=45, ha='right')

genre_counts = [item[1] for item in result2]
genres = [item[0] for item in result2]


f2.pie(genre_counts, labels=genres, autopct='%1.1f%%')

f2.set_title('Distribution of Games by Genre')



names = [item[0] for item in result3]
critic_scores = [float(item[1]) / 10 for item in result3]  
user_scores = [float(item[2]) for item in result3]


f3.bar(names, critic_scores, color='orange', label='Critic Score')
f3.bar(names, user_scores, color='blue', alpha=0.5, label='User Score')

f3.set_xlabel('Game Name')
f3.set_ylabel('Scores')
f3.set_title('Comparison of Critic and User Scores for Each Game')
f3.set_xticklabels(names, rotation=45, ha='right')
f3.legend()


mng = plt.get_current_fig_manager()
mng.full_screen_toggle()
plt.savefig('new_graphs.png', bbox_inches='tight')

plt.show()
