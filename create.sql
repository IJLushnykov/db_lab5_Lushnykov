CREATE TABLE Publisher (
    Publisher_ID SERIAL PRIMARY KEY,
    Publisher VARCHAR(255)
);

CREATE TABLE Developer (
    Developer_ID SERIAL PRIMARY KEY,
    Developer VARCHAR(255)
);

CREATE TABLE Video_Game (
    Indexx INT PRIMARY KEY,
    Name VARCHAR(255),
    Platform VARCHAR(50),
    Year_of_Release INT,
    Genre VARCHAR(50),
    Publisher_ID INT,
    Developer_ID INT,
    Critic_Score DECIMAL(5, 2),
    Critic_Count INT,
    User_Score DECIMAL(5, 2),
    User_Count INT,
    Rating VARCHAR(5),
    FOREIGN KEY (Publisher_ID) REFERENCES Publisher(Publisher_ID),
    FOREIGN KEY (Developer_ID) REFERENCES Developer(Developer_ID)
);

CREATE TABLE Sales_Info (
    Sale_ID SERIAL PRIMARY KEY,
    Indexx INT,
    Region VARCHAR(10),
    Sales_in_Millions DECIMAL(10, 2),
    FOREIGN KEY (Indexx) REFERENCES Video_Game(Indexx)
);

CREATE TABLE Global_Sales (
    Indexx INT PRIMARY KEY,
    Global_Sales_in_Millions DECIMAL(10, 2),
    FOREIGN KEY (Indexx) REFERENCES Video_Game(Indexx)
);

