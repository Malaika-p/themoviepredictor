# module utile pour le script à mettre au début
import mysql.connector
import sys
import argparse
import csv  


# for arg in sys.argv:
#    print(arg)


def connectToDatabase():
    return mysql.connector.connect(user='predictor', password='predictor',
                              host='127.0.0.1',
                              database='predictor')
    
# cnx.cursor(named_tuple=True) named_turple permet de donnée un nom à la liste named_turple.clef
def disconnectDatabase(cnx):
     cnx.close()



def createCursor(cnx):
    return cnx.cursor(dictionary=True) # dictionary pour appeler les valeurs name_dictionary['clef']

def closeCursor(cursor):
    cursor.close()


def findQuery(table, id):
    return ("SELECT * FROM {} WHERE id = {}".format(table, id)) # retourne un tuple(n-upplet)pas seulement une chaine de caractère


def find(table, id): 
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    cursor.execute(findQuery(table, id))
    results = cursor.fetchall()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results


def findAllQuery(table):
    return ("SELECT * FROM {}".format(table))

def findAll(table): 
    cnx = connectToDatabase()
    cursor = createCursor(cnx) # crée un curseur en fonction du connecteur ( que l'on passe en argument)
    cursor.execute(findAllQuery(table))
    results = cursor.fetchall() # récupère toutes les lignes et les envoie dans une liste de tuples
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results


def findRoleQuery(table):
    return ("SELECT * FROM {} JOIN movies_people_roles ON people.id  = movies_people_roles.people_id JOIN roles ON roles.id = movies_people_roles.role_id JOIN movies ON movies.id  = movies_people_roles.movie_id".format(table))

def findRole(table): 
    cnx = connectToDatabase()
    cursor = createCursor(cnx) # crée un curseur en fonction du connecteur ( que l'on passe en argument)
    cursor.execute(findRoleQuery(table))
    results = cursor.fetchall() # récupère toutes les lignes et les envoie dans une liste de tuples
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results

def findRoleIdQuery(table, id):
    return ("SELECT * FROM {} JOIN movies_people_roles ON people.id  = movies_people_roles.people_id JOIN roles ON roles.id = movies_people_roles.role_id JOIN movies ON movies.id  = movies_people_roles.movie_id WHERE people.id = {}".format(table, id))

def findRoleId(table, id): 
    cnx = connectToDatabase()
    cursor = createCursor(cnx) # crée un curseur en fonction du connecteur ( que l'on passe en argument)
    cursor.execute(findRoleIdQuery(table, id))
    results = cursor.fetchall() # récupère toutes les lignes et les envoie dans une liste de tuples
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results

parser = argparse.ArgumentParser(description='Process MoviePredictor data')
parser.add_argument('context', choices=['people', 'movies'], help="Le contexte dans lequel nous allons tavailler", type=str)
subparsers = parser.add_subparsers(dest='action', required=True,  help="L'action à effectuer") # arg obligatoire 
parser_list = subparsers.add_parser('list')
parser_role = subparsers.add_parser('role')
parser_find = subparsers.add_parser('find')
parser_role.add_argument('--id', metavar='id', help="L'identifiant à rechercher", type=int, required=False)
parser_find.add_argument('id', metavar='id', help="L'identifiant à rechercher", type=int)

parser.add_argument('--export', help="Chemin du fichier exporté", metavar='file.csv',  type=str) # --arg argument optionnel

args = parser.parse_args() # parse = analyser  analyse les argument et les stocke dans args



if args.context == "people":
    print("Mode People")
    if args.action == "find":
        print("Mode Find")
        peopleId = args.id
        print(f"Will try to find person #{peopleId}") 
        people = find("people", peopleId)
        if args.export: # Si l'argument export est != None (si il existe)
            print('exportation')
            with open(args.export, mode='w') as csvfile:
                fieldnames = ['id', 'firstname', 'lastname']
                writer =csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for person in people:
                    writer.writerow(person)
                    
        else:
            for person in people: # pour chaque valeur dans people 
                print("#{}: \n     {} {}".format(person['id'], person['firstname'], person['lastname']))

    if args.action == "list":
        print("Mode list")
        print("Will try to list people")
        people = findAll('people')
        if args.export:
            print('exportation')
            with open(args.export, mode='w') as csvfile:
                fieldnames = ['id', 'firstname', 'lastname']
                writer =csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for person in people:
                    writer.writerow(person)
                    
        else:
            for person in people:
                print("#{}: \n     {} {}".format(person['id'], person['firstname'], person['lastname']))

    # sorTir people + role 
    if args.action == "role":
        print('Mode Role')
        
        print(args)
        

        # if we had the id looking for one particular person
        if args.id:
            peopleId = args.id
            print(f"will try to find person #{peopleId} and their role")
            people = findRoleId('people', peopleId)
            print(people)
            
        else:
            #people = findRole('people')
            print("will try to find people and their role")
            if args.export:
                print('exportation')
                with open(args.export, mode='w') as csvfile:
                    fieldnames = ['id', 'firstname', 'lastname', 'role', 'movie_title']
                    writer =csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                
                    for person in people:
                        writer.writerow({'id': person['id'], 'firstname': person['firstname'], 'lastname': person['lastname'], 'role': person['role'], 'movie_title': person['title']})
        
            else:
                for person in people:
                    print("#{}: \n     {} {} {} in {}".format(person['id'], person['firstname'], person['lastname'], person['role'], person['title']))


if args.context == "movies":
    print("Mode Movies")
    if args.action == "find":
        print("Mode Find")
        movieId = args.id
        print(f"Will try to find movie #{movieId}")

        
        movies = find("movies", movieId)
        if args.export:
            print('exportation')
            with open(args.export, mode='w') as csvfile:
                fieldnames = ['id', 'original_title', 'title', 'rating', 'production_budget', 'marketing_budget',  'duration', 'release_date',  'in3d', 'synopsis']
                writer =csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for movie in movies:
                    writer.writerow(movie)
        else:
            for movie in movies:
                print("#{} - Original Title : {}\n     Title : {}\n     Rating : {}\n     Duration : {} minutes \n     Release on : {}\n     Synopsis : {}".format(movie['id'], movie['original_title'], movie['title'], movie['rating'], movie['duration'], movie['release_date'], movie['synopsis']))
    
    if args.action == "list":
        print("Mode list")
        print("Will try to list movies")
        movies = findAll('movies')

        if args.export:
            print('exportation')
            with open(args.export, mode='w') as csvfile:
                fieldnames = ['id', 'original_title', 'title', 'rating', 'production_budget', 'marketing_budget',  'duration', 'release_date',  'in3d', 'synopsis']
                writer =csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for movie in movies:
                    writer.writerow(movie)
        else:
            for movie in movies:
                print("#{} - Original Title : {}\n     Title : {}\n     Rating : {}\n     Duration : {} minutes \n     Release on : {}\n     Synopsis : {}".format(movie['id'], movie['original_title'], movie['title'], movie['rating'], movie['duration'], movie['release_date'], movie['synopsis']))
    



exit()

with open('emaviepredictorexport.csv', mode='w') as csv_file:
    fieldnames = ['id', 'firstname', 'lastname']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerow({'id': person['id'], 'firstname': person['firstname'], 'lastname': person['lastname']})


if args.context == "people" and args.action == 'find':
    writer.writerow({'id': person['id'], 'firstname': person['firstname'], 'lastname': person['lastname']})

if args.context == "people" and args.action == 'list':
    exit()

if args.context == "movies" and args.action == 'find':
    exit()

    



""" print(mysql.connector.version)
cnx = mysql.connector.connect(user='predictor', password='predictor',
                              host='127.0.0.1',
                              database='predictor')
cursor = cnx.cursor()

query = ("INSERT INTO people (firstname, lastname) VALUES ('Alain', 'Deloin'), ('John', 'Travolta') ")

cursor.execute(query)

# Make sure data is committed to the database
cnx.commit()
# cnx.rollback()
print("Last insererted id: {}".format(cursor.lastrowid))



cursor.close()
cnx.close()"""


print('hello world')
