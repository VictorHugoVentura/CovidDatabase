from configparser import ConfigParser

def config(file="database.ini", section="postgresql"):
    parser = ConfigParser()
    parser.read(file)
    
    db = {}
    params = parser.items(section)

    for param in params:
        db[param[0]] = param[1]
    
    return db
