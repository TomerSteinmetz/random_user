import json
import requests
import mysql.connector
import json_parser
import connection_detail

# creates Users dataset from the fields of the json
def init_table(response, mycursor):
    keys = json_parser.clean_keys(response)
    mycursor.execute("CREATE TABLE %s (%s);" % ('Users', keys))

# this function and the json_parser functions implemention tries to keep the code generic
# and to reduce as much as i can the hard code lines 
def insert_users(count_of_users, mycursor,db):
    base_url = f"https://randomuser.me/api/?results={count_of_users}"
    response = requests.get(base_url)
    for result in response.json()["results"]:
        query = "INSERT INTO %s VALUES (%s);" % ('Users', json_parser.clean_values(result))
        mycursor.execute(query)
    db.commit()
    
def dict_to_json(features,keys):
    """
    return each random user in dataset as a dict
    """
    json = dict()
    ziped = zip(keys,features)
    for field in ziped:    
        json.update({field[0]:field[1]})
    return json

def dataset_to_json(mycursor,keys,filename):
    """
    transform a dataset to json file, making a special id for each user in the dataset (which
    helps to test some of the requirements), stores a dict of all of his values, and storing it localy
    """
    ans = dict()
    id = 1
    for random_person in mycursor.fetchall():
        ans.update({id: dict_to_json(random_person, keys)})
        id += 1
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(ans, f, ensure_ascii=False)

def main():
# creates a connection to the database
 
    db = mysql.connector.connect(
    host=connection_detail.Host,
    user=connection_detail.Database,
    password=connection_detail.Password,
    database=connection_detail.Database
)

    # creates an integration with the randomgenerator api
    base_url = "https://randomuser.me/api/"
    response = requests.get(base_url)
    # curse threw the db and gets the information we want   
    mycursor = db.cursor()
    # Requirement_1
    # helper method, put the json fields as the database fields
    init_table(response, mycursor)
    # helper method, by using randomuser api, generates 4500 users and indert them to the dataset at once
    insert_users(4500,mycursor,db)
    # Requirement_2
    # splitting the data to 2 gender datasets by creating two tables 
    mycursor.execute("CREATE TABLE TomerSteinmetz_test_Female AS SELECT *  FROM Users WHERE gender= 'female';")
    db.commit()
    mycursor.execute("CREATE TABLE TomerSteinmetz_test_male AS SELECT *  FROM Users WHERE gender= 'male';")
    db.commit()
    # Requirement_3 && Requirement 4
    # convert 'dob.age' datatype to Integer in order to get comparison function
    mycursor.execute("ALTER TABLE Users Modify column `dob.age` INTEGER;")
    for age in range(10,110,10):
        mycursor.execute(f"CREATE TABLE TomerSteinmetz_test_{age // 10} AS SELECT * FROM Users WHERE `dob.age` >= {age} AND `dob.age` <{age + 10};")
    db.commit()
    # Requirement_5
    # creat from the 20 last registered females the dataset
    # insert the last 20 registered males to the dataset 
    mycursor.execute("CREATE TABLE TomerSteinmetz_test_20 AS SELECT * FROM TomerSteinmetz_test_Female ORDER BY (`registered.date`) DESC LIMIT 20")
    mycursor.execute("INSERT INTO TomerSteinmetz_test_20 SELECT * FROM TomerSteinmetz_test_Male ORDER BY (`registered.date`) DESC LIMIT 20")
    db.commit()
    
    # Requirement_6
    # combining using the UNION statement in order to get dataset without duplicate rows
    mycursor.execute("SELECT * FROM TomerSteinmetz_test_20 UNION SELECT * FROM TomerSteinmetz_test_5")
    # getting the keys for the json file 
    keys = [*json_parser.get_keys(response.json()["results"][0])]
    # helper method, write the dataset into json and stores it locally
    dataset_to_json(mycursor,keys,'first.json')
    # Requirement_7
    # combining using the UNION ALL statement in order to get every row from datasets.
    mycursor.execute("SELECT * FROM TomerSteinmetz_test_20 UNION ALL SELECT * FROM TomerSteinmetz_test_2")
    dataset_to_json(mycursor,keys,'second.json')
        
if __name__ == '__main__':
    main()

