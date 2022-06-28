'''
Script for writing the cypher queries to the "queries.cyp" file
'''

#todo Dateien durchforsten, fuer jede Datei CREATEs machen und dann Daten mit LOAD CSV importieren?

'''
USING PERIODIC COMMIT LOAD CSV FROM 'file:///artists.csv' AS line
CREATE (:Artist {name: line[1], year: toInteger(line[2])})
'''