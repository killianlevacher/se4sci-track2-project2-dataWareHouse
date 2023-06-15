# se4sci-dataWareHouse

* remove A/A/A


### Useful SQL Queries
# Dealing with duplicates
# SELECT user_id, COUNT(*) as count FROM user_table GROUP BY user_id ORDER BY countcl DESC;
# SELECT user_id, first_name, last_name FROM user_table GROUP BY user_id, first_name, last_name ORDER BY user_id DESC;
# SELECT artist_id, artist_name, location FROM artist_table GROUP BY artist_id, artist_name, location ORDER BY artist_id DESC;