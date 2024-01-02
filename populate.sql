-- Заповнення таблиці Publisher
INSERT INTO Publisher (Publisher_ID, Publisher)
VALUES
    (1, 'Nintendo'),
    (2, 'Take-Two Interactive'),
    (3, 'Sony Computer Entertainment'),
    (4, 'Bethesda Softworks'),
    (5, 'Ubisoft'),
    (6, 'Konami Digital Entertainment'),
    (7, 'Microsoft Game Studios');

-- Заповнення таблиці Developer
INSERT INTO Developer (Developer_ID, Developer)
VALUES
    (1, 'Nintendo'),
    (2, 'Rockstar North'),
    (3, 'Polyphony Digital'),
    (4, 'Bethesda Game Studios'),
    (5, 'Ubisoft'),
    (6, 'KCEJ'),
    (7, 'Lionhead Studios');

-- Заповнення таблиці Video_Game
INSERT INTO Video_Game (Indexx, Name, Platform, Year_of_Release, Genre, Publisher_ID, Developer_ID, Critic_Score, Critic_Count, User_Score, User_Count, Rating)
VALUES
    (6, 'New Super Mario Bros.', 'DS', 2006, 'Platform', 1, 1, 89.0, 65, 8.5, 431, 'E'),
    (17, 'Grand Theft Auto: San Andreas', 'PS2', 2004, 'Action', 2, 2, 95.0, 80, 9.0, 1588, 'M'),
    (48, 'Gran Turismo 4', 'PS2', 2004, 'Racing', 3, 3, 89.0, 74, 8.5, 272, 'E'),
    (105, 'Fallout 4', 'PS4', 2015, 'Role-Playing', 4, 4, 87.0, 58, 6.5, 4228, 'M'),
    (116, 'Just Dance 4', 'Wii', 2012, 'Misc', 5, 5, 74.0, 10, 7.3, 28, 'E10+'),
    (144, 'Metal Gear Solid 2: Sons of Liberty', 'PS2', 2001, 'Action', 6, 6, 96.0, 42, 8.7, 1026, 'M'),
    (265, 'Fable II', 'X360', 2008, 'Role-Playing', 7, 7, 89.0, 95, 6.5, 2423, 'M');

-- Заповнення таблиці Sales_Info
INSERT INTO Sales_Info (Sale_ID, Indexx, Region, Sales_in_Millions)
VALUES
    (1, 6, 'NA', 11.28),
    (2, 6, 'EU', 9.14),
    (3, 6, 'JP', 6.5),
    (4, 6, 'Other', 2.88),
    (6, 17, 'NA', 9.43),
    (7, 17, 'EU', 0.4),
    (8, 17, 'JP', 0.41),
    (9, 17, 'Other', 10.57),
    (11, 48, 'NA', 3.01),
    (12, 48, 'EU', 0.01),
    (13, 48, 'JP', 1.1),
    (14, 48, 'Other', 7.53),
    (16, 105, 'NA', 2.53),
    (17, 105, 'EU', 3.27),
    (18, 105, 'JP', 0.24),
    (19, 105, 'Other', 1.13),
    (21, 116, 'NA', 4.05),
    (22, 116, 'EU', 2.16),
    (23, 116, 'JP', 0.0),
    (24, 116, 'Other', 0.55),
    (26, 144, 'NA', 2.45),
    (27, 144, 'EU', 2.01),
    (28, 144, 'JP', 0.87),
    (29, 144, 'Other', 0.72),
    (31, 265, 'NA', 2.51),
    (32, 265, 'EU', 1.24),
    (33, 265, 'JP', 0.11),
    (34, 265, 'Other', 0.41);


-- Заповнення таблиці Global_Sales
INSERT INTO Global_Sales (Indexx, Global_Sales_in_Millions)
VALUES
    (6, 29.8),
    (17, 20.81),
    (48, 11.66),
    (105, 7.16),
    (116, 6.76),
    (144, 6.05),
    (265, 4.27);
